import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import {
  DUEL_BRACKET_COLUMNS,
  ensureDuelBracketColumns,
  ensureTournamentStructureSchema,
} from "./tournament-structure-schema.js";

const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (error) => (error ? reject(error) : resolve())));
const run = (db, sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, (error) => (error ? reject(error) : resolve())));
const all = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || []))));

async function openDb(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    PRAGMA foreign_keys = ON;
    CREATE TABLE tournaments (id TEXT PRIMARY KEY COLLATE NOCASE);
    INSERT INTO tournaments (id) VALUES ('CCL-2027');
  `);
  return db;
}

const addStage = (db, id, values = {}) => run(
  db,
  `INSERT INTO tournament_stages (id, tournament_id, parent_id, stage_key, name, format, sort_order)
   VALUES (?, ?, ?, ?, ?, ?, ?)`,
  [id, values.tournament_id ?? "CCL-2027", values.parent_id ?? null, values.stage_key ?? id,
    values.name ?? id, values.format ?? null, values.sort_order ?? 0],
);

test("stages form a tree under the tournament; groups belong to a stage", async (t) => {
  const db = await openDb(t);
  await ensureTournamentStructureSchema(db);
  await ensureTournamentStructureSchema(db);

  await addStage(db, "s-qual", { stage_key: "qualifiers", name: "Qualifiers" });
  await addStage(db, "s-q1", { parent_id: "s-qual", stage_key: "Q1", format: "double_elimination" });
  await run(db, "INSERT INTO tournament_groups (id, stage_id, group_key, name) VALUES ('g-a', 's-q1', 'A', 'Group A')");
  const stage = (await all(db, "SELECT * FROM tournament_stages WHERE id = 's-q1'"))[0];
  assert.equal(stage.status, "structure");
  assert.equal(stage.parent_id, "s-qual");

  // stage_key and group_key are unique (case-insensitive) within their parent.
  await assert.rejects(addStage(db, "s-dup", { stage_key: "q1" }), /UNIQUE/);
  await assert.rejects(run(db, "INSERT INTO tournament_groups (id, stage_id, group_key) VALUES ('g-dup', 's-q1', 'a')"), /UNIQUE/);
  // Unknown format, status or tournament are refused.
  await assert.rejects(addStage(db, "s-bad", { stage_key: "bad", format: "knockout" }), /CHECK/);
  await assert.rejects(run(db, "UPDATE tournament_stages SET status = 'open' WHERE id = 's-q1'"), /CHECK/);
  await assert.rejects(addStage(db, "s-orphan", { tournament_id: "NOPE" }), /FOREIGN KEY/);

  // Deleting the top stage removes its paths and their groups.
  await run(db, "DELETE FROM tournament_stages WHERE id = 's-qual'");
  assert.deepEqual(await all(db, "SELECT id FROM tournament_stages"), []);
  assert.deepEqual(await all(db, "SELECT id FROM tournament_groups"), []);

  // Deleting the tournament removes the whole structure.
  await addStage(db, "s-league", { stage_key: "league", format: "league" });
  await run(db, "DELETE FROM tournaments WHERE id = 'CCL-2027'");
  assert.deepEqual(await all(db, "SELECT id FROM tournament_stages"), []);
});

test("duels get bracket columns; one live duel per group slot", async (t) => {
  const db = await openDb(t);
  // Without a duels table the step does nothing.
  await ensureDuelBracketColumns(db);
  await exec(db, "CREATE TABLE duels (id TEXT PRIMARY KEY, tournament_id TEXT, deleted_at TEXT)");
  await ensureDuelBracketColumns(db);
  await ensureDuelBracketColumns(db);
  const columns = (await all(db, "PRAGMA table_info(duels)")).map((column) => column.name);
  for (const [name] of DUEL_BRACKET_COLUMNS) assert.ok(columns.includes(name), name);

  const insert = (id, values = {}) => run(
    db,
    "INSERT INTO duels (id, group_id, bracket_no, bracket_side, p1_source, deleted_at) VALUES (?, ?, ?, ?, ?, ?)",
    [id, values.group_id ?? "g-a", values.bracket_no ?? 1, values.bracket_side ?? "upper",
      values.p1_source ?? "draw:1", values.deleted_at ?? null],
  );
  await insert("d1");
  assert.equal((await all(db, "SELECT is_conditional FROM duels WHERE id = 'd1'"))[0].is_conditional, 0);
  await assert.rejects(insert("d2", { group_id: "G-A" }), /UNIQUE/);
  await insert("d3", { group_id: "g-b" });
  await run(db, "UPDATE duels SET deleted_at = '2026-11-15' WHERE id = 'd1'");
  await insert("d4");
  await assert.rejects(insert("d5", { bracket_no: 2, bracket_side: "middle" }), /CHECK/);
  // Duels outside a group (single CCL matches, team duels) are not limited.
  await run(db, "INSERT INTO duels (id) VALUES ('x1'), ('x2')");
});
