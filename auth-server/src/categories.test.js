import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import sqlite3 from "sqlite3";
import {
  ensureCategoriesSchema,
  isSystemTournamentCategory,
  serializeCategory,
  SYSTEM_TOURNAMENT_CATEGORIES,
} from "./categories.js";

const server = readFileSync(new URL("./server.js", import.meta.url), "utf8");

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) reject(error);
      else resolve(this);
    });
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows));
  });
}

async function database(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve, reject) => db.close((error) => error ? reject(error) : resolve())));
  for (const table of ["icons", "news", "tournaments"]) {
    await run(db, `CREATE TABLE ${table} (
      id TEXT PRIMARY KEY, category TEXT, payload TEXT,
      created_at TEXT DEFAULT 'original-created', updated_at TEXT DEFAULT 'original-updated', deleted_at TEXT
    )`);
  }
  return db;
}

async function snapshot(db) {
  const result = {};
  for (const table of ["categories", "icons", "news", "tournaments"]) {
    result[table] = await all(db, `SELECT * FROM ${table} ORDER BY id`);
  }
  return result;
}

test("creates all eleven protected tournament categories and retains shared content categories", async (t) => {
  const db = await database(t);
  await ensureCategoriesSchema(db);
  const rows = await all(db, "SELECT * FROM categories ORDER BY name");
  assert.deepEqual(rows.filter((row) => isSystemTournamentCategory(row.name)).map((row) => row.name), [
    "Asian Cup", "CCL", "Clubs", "Copa America", "ETCOC", "Friendly",
    "King of Carcassonne", "MSO", "National", "Rivals", "WTCOC",
  ]);
  for (const row of rows) {
    assert.equal(row.is_tournament, isSystemTournamentCategory(row.name) ? 1 : 0);
    assert.equal(serializeCategory(row).is_read_only, isSystemTournamentCategory(row.name));
    assert.equal(serializeCategory(row).tournament_type,
      SYSTEM_TOURNAMENT_CATEGORIES.find((category) => category.name === row.name)?.type || null);
  }
  assert.equal(rows.find((row) => row.name === "Local").is_tournament, 0);
  assert.equal(isSystemTournamentCategory(" kOc "), true);
  assert.equal(isSystemTournamentCategory(" king  of CARCASSONNE "), true);
  assert.equal(isSystemTournamentCategory("Custom Cup"), false);
  assert.equal(serializeCategory(null), null);
});

test("renames KoC in place and migrates all references without changing other data; reruns are unchanged", async (t) => {
  const db = await database(t);
  await ensureCategoriesSchema(db);
  await run(db, `UPDATE categories SET name = 'KoC', sort_order = 777, is_tournament = 0,
    created_at = 'original-created' WHERE name = 'King of Carcassonne'`);
  await run(db, "UPDATE categories SET name = ' ccl ', is_tournament = 0, sort_order = 778 WHERE name = 'CCL'");
  await run(db, "INSERT INTO categories (name, sort_order, is_tournament) VALUES ('Custom Cup', 123, 1)");
  for (const table of ["icons", "news", "tournaments"]) {
    await run(db, `INSERT INTO ${table} (id, category, payload) VALUES ('king', ' kOc ', 'keep')`);
    await run(db, `INSERT INTO ${table} (id, category, payload, deleted_at) VALUES ('deleted', 'KoC', 'keep-deleted', 'deleted-date')`);
    await run(db, `INSERT INTO ${table} (id, category, payload) VALUES ('other', 'Local', 'untouched')`);
  }
  const before = await snapshot(db);
  await ensureCategoriesSchema(db);
  const after = await snapshot(db);
  const oldKing = before.categories.find((row) => row.name === "KoC");
  const king = after.categories.find((row) => row.name === "King of Carcassonne");
  assert.equal(king.id, oldKing.id);
  assert.equal(king.sort_order, 777);
  assert.equal(king.created_at, "original-created");
  assert.equal(king.is_tournament, 1);
  assert.equal(after.categories.find((row) => row.name === "CCL").sort_order, 778);
  assert.deepEqual(after.categories.filter((row) => ["Local", "Custom Cup"].includes(row.name)),
    before.categories.filter((row) => ["Local", "Custom Cup"].includes(row.name)));
  for (const table of ["icons", "news", "tournaments"]) {
    for (let i = 0; i < before[table].length; i++) {
      const original = before[table][i];
      assert.deepEqual(after[table][i], original.id === "other" ? original : {
        ...original, category: "King of Carcassonne", updated_at: after[table][i].updated_at,
      });
    }
  }
  assert.equal(after.categories.some((row) => row.name.toLowerCase() === "koc"), false);
  await ensureCategoriesSchema(db);
  assert.deepEqual(await snapshot(db), after);
});

test("upgrades a populated legacy catalog and imports shared categories without recreating KoC", async (t) => {
  const db = await database(t);
  await run(db, "CREATE TABLE categories (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)");
  await run(db, "INSERT INTO categories (id, name) VALUES (42, 'KoC')");
  await run(db, "INSERT INTO news (id, category) VALUES ('legacy', 'KoC'), ('custom', 'Custom News')");
  await ensureCategoriesSchema(db);
  const rows = await all(db, "SELECT * FROM categories");
  assert.equal(rows.find((row) => row.name === "King of Carcassonne").id, 42);
  assert.equal(rows.find((row) => row.name === "Custom News").is_tournament, 0);
  assert.equal(rows.some((row) => row.name === "KoC"), false);
  assert.ok(rows.every((row) => row.created_at && row.updated_at));
});

test("rebuilds missing catalog entries from references with canonical defaults and no duplicate KoC", async (t) => {
  const db = await database(t);
  await run(db, "INSERT INTO news (id, category) VALUES ('king', 'KoC'), ('ccl', 'CCL')");
  await ensureCategoriesSchema(db);
  const rows = await all(db, "SELECT * FROM categories");
  assert.equal(rows.find((row) => row.name === "King of Carcassonne").sort_order, 100);
  assert.equal(rows.find((row) => row.name === "CCL").sort_order, 80);
  assert.equal(rows.some((row) => row.name === "KoC"), false);
  assert.equal((await all(db, "SELECT category FROM news WHERE id = 'king'"))[0].category, "King of Carcassonne");
});

test("fails without losing either category ID if both KoC names already exist", async (t) => {
  const db = await database(t);
  await ensureCategoriesSchema(db);
  await run(db, "INSERT INTO categories (name, sort_order) VALUES ('KoC', 200)");
  const before = await snapshot(db);
  await assert.rejects(ensureCategoriesSchema(db), /Both KoC and King of Carcassonne categories exist/);
  assert.deepEqual(await snapshot(db), before);
});

test("a reference update failure rolls back the category rename and earlier reference updates", async (t) => {
  const db = await database(t);
  await ensureCategoriesSchema(db);
  await run(db, "UPDATE categories SET name = 'KoC' WHERE name = 'King of Carcassonne'");
  for (const table of ["icons", "news", "tournaments"]) {
    await run(db, `INSERT INTO ${table} (id, category) VALUES ('king', 'KoC')`);
  }
  await run(db, `CREATE TRIGGER fail_news_category BEFORE UPDATE OF category ON news
    BEGIN SELECT RAISE(ABORT, 'reference update failed'); END`);
  const before = await snapshot(db);
  await assert.rejects(ensureCategoriesSchema(db), /reference update failed/);
  assert.deepEqual(await snapshot(db), before);
});

function serverFunction(name) {
  const start = server.search(new RegExp(`(?:async )?function ${name}\\(`));
  assert.ok(start >= 0, `${name} must exist`);
  return server.slice(start, server.indexOf("\n}", start) + 2);
}

async function api(t) {
  const db = await database(t);
  await ensureCategoriesSchema(db);
  const routes = new Map();
  const app = Object.fromEntries(["get", "post", "patch", "delete"].map((method) => [method,
    (path, ...handlers) => routes.set(`${method} ${path}`, handlers)]));
  const helpers = ["normalizeCategoryName", "normalizePositiveInteger", "normalizeIntegerOrNull", "requireAdmin",
    "loadCategoryById", "countCategoryReferences"].map(serverFunction).join("\n");
  const routeStart = server.indexOf('app.get("/categories",');
  const routeEnd = server.indexOf('app.get("/local-websites",', routeStart);
  assert.ok(routeStart >= 0 && routeEnd > routeStart);
  new Function("app", "db", "dbRunAsync", "dbGetAsync", "isSystemTournamentCategory", "serializeCategory",
    `${helpers}\n${server.slice(routeStart, routeEnd)}`)(
    app, db, (sql, params) => run(db, sql, params), async (sql, params) => (await all(db, sql, params))[0] || null,
    isSystemTournamentCategory, serializeCategory);

  const request = (method, { id, body = {}, user = { admin: 1 } } = {}) => new Promise((resolve, reject) => {
    const handlers = routes.get(`${method} /categories${id === undefined ? "" : "/:id"}`);
    let status = 200;
    let index = 0;
    const req = { params: { id }, body, user };
    const res = { status(code) { status = code; return this; }, json(data) { resolve({ status, data }); } };
    const next = (error) => {
      if (error) return reject(error);
      try {
        const result = handlers[index++](req, res, next);
        if (result?.catch) result.catch(reject);
      } catch (error) { reject(error); }
    };
    next();
  });
  return { db, request };
}

test("API exposes read-only metadata and prevents global admins from changing or deleting every system category", async (t) => {
  const { db, request } = await api(t);
  const response = await request("get", { user: null });
  assert.equal(response.status, 200);
  const system = response.data.categories.filter((row) => row.is_read_only);
  assert.equal(system.length, SYSTEM_TOURNAMENT_CATEGORIES.length);
  const before = await snapshot(db);
  for (const category of system) {
    for (const changes of [{ name: "Renamed" }, { sort_order: 999 }, { is_tournament: 0 }, { is_read_only: false }]) {
      const result = await request("patch", { id: category.id, body: { ...category, ...changes } });
      assert.equal(result.status, 403);
      assert.equal(result.data.code, "SYSTEM_CATEGORY_READ_ONLY");
    }
    assert.equal((await request("delete", { id: category.id })).status, 403);
  }
  assert.deepEqual(await snapshot(db), before);
});

test("API reserves system names and KoC, including when renaming an ordinary category", async (t) => {
  const { request } = await api(t);
  const custom = (await request("post", { body: { name: "Custom Cup" } })).data.category;
  for (const name of [...SYSTEM_TOURNAMENT_CATEGORIES.map((row) => ` ${row.name.toUpperCase()} `), " KoC ", "King  of Carcassonne"]) {
    assert.equal((await request("post", { body: { name } })).status, 409);
    assert.equal((await request("patch", { id: custom.id, body: { name } })).status, 409);
  }
});

test("ordinary categories retain CRUD and reference updates, with existing admin authorization", async (t) => {
  const { db, request } = await api(t);
  for (const [user, status] of [[null, 401], [{ admin: 0 }, 403]]) {
    assert.equal((await request("post", { user, body: { name: "Forbidden" } })).status, status);
  }
  const created = await request("post", { body: { name: "Custom Cup", sort_order: 222, is_tournament: 1 } });
  assert.equal(created.status, 201);
  const category = created.data.category;
  assert.equal(category.is_read_only, false);
  for (const method of ["patch", "delete"]) {
    for (const [user, status] of [[null, 401], [{ admin: 0 }, 403]]) {
      assert.equal((await request(method, { id: category.id, user, body: { name: "Forbidden" } })).status, status);
    }
  }
  for (const table of ["icons", "news", "tournaments"]) {
    await run(db, `INSERT INTO ${table} (id, category) VALUES ('custom', 'Custom Cup')`);
  }
  const changed = await request("patch", { id: category.id, body: { name: "Renamed Cup", sort_order: 333, is_tournament: 0 } });
  assert.equal(changed.status, 200);
  assert.equal(changed.data.category.is_read_only, false);
  assert.equal(changed.data.category.sort_order, 333);
  assert.equal(changed.data.category.is_tournament, 0);
  for (const table of ["icons", "news", "tournaments"]) {
    assert.equal((await all(db, `SELECT category FROM ${table} WHERE id = 'custom'`))[0].category, "Renamed Cup");
  }
  assert.equal((await request("delete", { id: category.id })).status, 409);
  for (const table of ["icons", "news", "tournaments"]) await run(db, `DELETE FROM ${table}`);
  assert.equal((await request("delete", { id: category.id })).status, 200);
});
