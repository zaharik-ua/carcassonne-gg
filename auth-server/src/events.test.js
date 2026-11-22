import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import {
  canManageEvent,
  ensureEventsSchema,
  loadManagedTournamentIds,
  normalizeEventLink,
  normalizeEventTime,
  registerEventRoutes,
  validateEventInput,
} from "./events.js";

const USERS = {
  globalAdmin: { id: 1, admin: 1, player_id: "p-admin" },
  tournamentAdmin: { id: 2, admin: 0, player_id: "p-ta" },
  captain: { id: 3, admin: 0, player_id: "p-captain" },
};

const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (error) => (error ? reject(error) : resolve())));
const all = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows))));

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    CREATE TABLE tournaments (id TEXT PRIMARY KEY, name TEXT, short_title TEXT, logo TEXT, link TEXT, type TEXT,
      is_test INTEGER DEFAULT 0);
    CREATE TABLE tournament_access_users (tournament_entity_type TEXT, tournament_id TEXT, user_id INTEGER, role TEXT);
    CREATE TABLE icons (id INTEGER PRIMARY KEY, name TEXT, icon_link TEXT);
    INSERT INTO tournaments VALUES
      ('WTCOC', 'WTCOC 2026', 'WTCOC', 'https://x/wtcoc.png', 'https://carcassonne.gg/wtcoc', 'teams', 0),
      ('CCL-2027', 'CCL 2027', 'CCL', 'https://x/ccl.png', 'https://carcassonne.gg/ccl', 'individuals', 0),
      ('TEST', 'Test cup', NULL, NULL, NULL, 'teams', 1);
    INSERT INTO tournament_access_users VALUES
      ('tournament', 'wtcoc', 2, 'admin'),
      ('tournament', 'CCL-2027', 2, 'captain'),
      ('tournament', 'CCL-2027', 3, 'captain');
    INSERT INTO icons VALUES (1, 'Stream', 'https://carcassonne.gg/gallery/stream.png'), (2, 'Local', '/images/icons/local.png');
  `);
  await ensureEventsSchema(db);
  return db;
}

function createRoutes(db) {
  const routes = new Map();
  const audits = [];
  const register = (method) => (path, ...handlers) => routes.set(`${method} ${path}`, handlers);
  registerEventRoutes(
    { get: register("GET"), post: register("POST"), patch: register("PATCH"), delete: register("DELETE") },
    {
      db,
      requireAuthenticated: () => {},
      logAuditEvent: (entry, done) => { audits.push(entry); done(); },
      logger: { error() {} },
    }
  );
  const call = async (route, { user = null, body = {}, params = {} } = {}) => {
    const result = { status: 200 };
    await routes.get(route).at(-1)(
      { user, body, params, query: {} },
      { status(code) { result.status = code; return this; }, json(payload) { result.body = payload; return this; } }
    );
    return result;
  };
  return { call, audits };
}

test("normalizes time and links", () => {
  assert.equal(normalizeEventTime("2026-12-01T18:30:00.000Z"), "2026-12-01T18:30:00.000Z");
  assert.equal(normalizeEventTime("not a date"), null);
  assert.equal(normalizeEventTime(""), null);
  assert.deepEqual(normalizeEventLink(""), { value: null });
  assert.deepEqual(normalizeEventLink("https://youtube.com/live"), { value: "https://youtube.com/live" });
  assert.ok(normalizeEventLink("javascript:alert(1)").error);
  assert.ok(normalizeEventLink("youtube.com").error);
});

test("tournament admins manage only events of their tournaments", async (t) => {
  const db = await createDatabase(t);
  assert.equal(await loadManagedTournamentIds(db, USERS.globalAdmin), null);
  assert.deepEqual([...await loadManagedTournamentIds(db, USERS.tournamentAdmin)], ["wtcoc"]);
  assert.equal((await loadManagedTournamentIds(db, USERS.captain)).size, 0);

  const managed = new Set(["wtcoc"]);
  assert.equal(canManageEvent(null, { tournament_id: null }), true);
  assert.equal(canManageEvent(managed, { tournament_id: "WTCOC" }), true);
  assert.equal(canManageEvent(managed, { tournament_id: "CCL-2027" }), false);
  assert.equal(canManageEvent(managed, { tournament_id: null }), false);
});

test("validates event input", async (t) => {
  const db = await createDatabase(t);
  const base = { title: "Final stream", time_utc: "2026-12-01T18:00:00Z" };
  assert.equal((await validateEventInput(db, { ...base, title: " " }, null)).error, "Event name is required");
  assert.match((await validateEventInput(db, { ...base, time_utc: "" }, null)).error, /required/);
  assert.equal((await validateEventInput(db, { ...base, tournament_id: "NOPE" }, null)).error, "Tournament not found");
  assert.equal((await validateEventInput(db, { ...base, icon: "https://evil/x.png" }, null)).error, "Choose an icon from the catalog");

  const standalone = await validateEventInput(db, { ...base, icon: "/images/icons/local.png", main_event: true }, null);
  assert.deepEqual(standalone.values, {
    title: "Final stream",
    time_utc: "2026-12-01T18:00:00.000Z",
    tournament_id: null,
    link: null,
    icon: "/images/icons/local.png",
    main_event: 1,
  });

  const managed = new Set(["wtcoc"]);
  const byTournamentAdmin = { ...base, tournament_id: "WTCOC", main_event: true };
  assert.equal((await validateEventInput(db, byTournamentAdmin, managed)).values.main_event, 0);
  assert.equal((await validateEventInput(db, { ...byTournamentAdmin, main_event: false }, managed, { main_event: 1 })).values.main_event, 1);
  assert.equal((await validateEventInput(db, { ...base, main_event: false }, null, { main_event: 1 })).values.main_event, 0);
  assert.equal((await validateEventInput(db, base, managed)).error, "Choose one of your tournaments");
  assert.equal((await validateEventInput(db, { ...base, tournament_id: "CCL-2027" }, managed)).status, 403);
  assert.equal((await validateEventInput(db, { ...base, tournament_id: "wtcoc" }, managed)).values.tournament_id, "WTCOC");
});

test("event routes create, list, update, delete and publish events", async (t) => {
  const db = await createDatabase(t);
  const { call, audits } = createRoutes(db);
  const future = new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString();
  const old = new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString();

  assert.equal((await call("GET /events", { user: USERS.captain })).status, 403);

  const created = await call("POST /events", {
    user: USERS.tournamentAdmin,
    body: { title: "WTCOC draw", time_utc: future, tournament_id: "WTCOC", link: "https://youtube.com/live", main_event: true },
  });
  assert.equal(created.status, 201);
  assert.equal(created.body.event.tournament_name, "WTCOC 2026");
  assert.equal(created.body.event.tournament_type, "TEAM");
  assert.equal(created.body.event.main_event, false);
  assert.equal(audits[0].event_type, "event.created");

  const promoted = await call("PATCH /events/:id", {
    user: USERS.globalAdmin,
    params: { id: String(created.body.event.id) },
    body: { main_event: true },
  });
  assert.equal(promoted.body.event.main_event, true);

  const standaloneByTa = await call("POST /events", { user: USERS.tournamentAdmin, body: { title: "X", time_utc: future } });
  assert.equal(standaloneByTa.status, 400);

  const standalone = await call("POST /events", { user: USERS.globalAdmin, body: { title: "GG stream", time_utc: future } });
  assert.equal(standalone.status, 201);
  await call("POST /events", { user: USERS.globalAdmin, body: { title: "Old", time_utc: old } });
  await call("POST /events", { user: USERS.globalAdmin, body: { title: "Test cup", time_utc: future, tournament_id: "TEST" } });

  const taList = await call("GET /events", { user: USERS.tournamentAdmin });
  assert.deepEqual(taList.body.events.map((row) => row.title), ["WTCOC draw"]);
  assert.equal((await call("GET /events", { user: USERS.globalAdmin })).body.events.length, 4);

  const options = await call("GET /events/options", { user: USERS.tournamentAdmin });
  assert.deepEqual(options.body.tournaments.map((row) => row.id), ["WTCOC"]);
  assert.equal(options.body.is_admin, false);

  const forbidden = await call("PATCH /events/:id", {
    user: USERS.tournamentAdmin,
    params: { id: String(standalone.body.event.id) },
    body: { title: "Mine now" },
  });
  assert.equal(forbidden.status, 403);

  const updated = await call("PATCH /events/:id", {
    user: USERS.tournamentAdmin,
    params: { id: String(created.body.event.id) },
    body: { title: "WTCOC final draw", main_event: false },
  });
  assert.equal(updated.status, 200);
  assert.equal(updated.body.event.title, "WTCOC final draw");
  assert.equal(updated.body.event.main_event, true);
  assert.equal(updated.body.event.link, "https://youtube.com/live");
  assert.deepEqual(Object.keys(audits.at(-1).changes), ["title"]);

  const published = await call("GET /public/events");
  assert.deepEqual(published.body.events.map((row) => row.title), ["WTCOC final draw", "GG stream"]);
  assert.equal(published.body.events[0].tournament_logo, "https://x/wtcoc.png");
  assert.equal(published.body.events[1].tournament_id, null);

  const deleted = await call("DELETE /events/:id", { user: USERS.globalAdmin, params: { id: String(standalone.body.event.id) } });
  assert.equal(deleted.status, 200);
  assert.deepEqual((await call("GET /public/events")).body.events.map((row) => row.title), ["WTCOC final draw"]);
  const rows = await all(db, "SELECT deleted_by FROM events WHERE id = ?", [standalone.body.event.id]);
  assert.equal(rows[0].deleted_by, "p-admin");
});
