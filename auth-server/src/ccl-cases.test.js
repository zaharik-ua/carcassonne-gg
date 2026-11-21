import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import { getCclCaseScenario, isCclGamePlayed, registerCclCaseRoutes } from "./ccl-cases.js";
import { ensureTournamentCasesSchema } from "./tournament-cases.js";

const NOW = new Date("2026-11-21T12:00:00.000Z");
const USERS = {
  olena: { id: 3, player_id: "olena" },
  taras: { id: 4, player_id: "taras" },
  stranger: { id: 6, player_id: "max" },
};

const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (error) => (error ? reject(error) : resolve())));
const get = (db, sql, params = []) => new Promise((resolve, reject) => db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null))));
const all = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || []))));

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    PRAGMA foreign_keys = ON;
    CREATE TABLE users (id INTEGER PRIMARY KEY);
    CREATE TABLE profiles (id TEXT PRIMARY KEY, bga_nickname TEXT, name TEXT);
    CREATE TABLE matches (id TEXT PRIMARY KEY);
    CREATE TABLE challenge_periods (id TEXT PRIMARY KEY);
    CREATE TABLE tournaments (id TEXT PRIMARY KEY COLLATE NOCASE, name TEXT, type TEXT, category TEXT);
    CREATE TABLE duels (
      id TEXT PRIMARY KEY, tournament_id TEXT, source_type TEXT, status TEXT, time_utc TEXT,
      player_1_id TEXT, player_2_id TEXT, deadline_utc TEXT, dw1 INTEGER, dw2 INTEGER,
      proposed_time_utc TEXT, proposed_time_by_player_id TEXT, proposed_time_status TEXT,
      results_last_error TEXT, results_checked_at TEXT, updated_by TEXT, updated_at TEXT, deleted_at TEXT
    );
    CREATE TABLE games (
      id INTEGER PRIMARY KEY, duel_id TEXT, player_1_score INTEGER, player_2_score INTEGER,
      player_1_rank INTEGER, player_2_rank INTEGER, status TEXT, deleted_at TEXT
    );
    INSERT INTO users (id) VALUES (3), (4), (6);
    INSERT INTO profiles VALUES ('olena', 'Olena', NULL), ('taras', 'taras', NULL), ('max', 'Max', NULL);
    INSERT INTO tournaments VALUES ('CCL-2027', 'CCL 2027', 'Individuals', 'CCL'), ('ETCOC', 'ETCOC', 'Teams', 'ETCOC');
    INSERT INTO duels (id, tournament_id, source_type, status, time_utc, player_1_id, player_2_id, proposed_time_status, results_last_error) VALUES
      ('no-time', 'CCL-2027', 'ccl', 'Planned', NULL, 'olena', 'taras', 'pending', NULL),
      ('timed', 'CCL-2027', 'ccl', 'Planned', '2026-11-25T18:00:00Z', 'olena', 'taras', 'accepted', NULL),
      ('error-empty', 'CCL-2027', 'ccl', 'Error', '2026-11-20T18:00:00Z', 'olena', 'taras', 'accepted', 'No games found'),
      ('error-partial', 'CCL-2027', 'ccl', 'Error', '2026-11-20T18:00:00Z', 'olena', 'taras', 'accepted', NULL),
      ('done-old', 'CCL-2027', 'ccl', 'Done', '2026-11-19T18:00:00Z', 'olena', 'taras', 'accepted', NULL),
      ('done-fresh', 'CCL-2027', 'ccl', 'Done', '2026-11-21T10:00:00Z', 'olena', 'taras', 'accepted', NULL),
      ('team-duel', 'ETCOC', 'match', 'Error', '2026-11-20T18:00:00Z', 'olena', 'taras', NULL, NULL);
    INSERT INTO games (duel_id, player_1_score, player_1_rank, status) VALUES ('error-partial', 120, 1, NULL);
  `);
  await ensureTournamentCasesSchema(db);
  return db;
}

function createApi(db, { runDuelResultsUpdate = null } = {}) {
  const routes = new Map();
  const app = { post: (routePath, ...handlers) => routes.set(`POST ${routePath}`, handlers.at(-1)) };
  const audit = [];
  registerCclCaseRoutes(app, {
    db,
    requireAuthenticated: () => {},
    getAuditActor: (user) => ({ actor_user_id: user?.id ?? null }),
    logAuditEvent: (entry, done) => {
      audit.push(entry);
      done();
    },
    logger: { error() {} },
    now: () => NOW,
    runDuelResultsUpdate,
  });
  const call = async (route, { user, id, body = {} }) => {
    const result = { status: 200, body: null };
    const res = {
      status(code) {
        result.status = code;
        return this;
      },
      json(value) {
        result.body = value;
        return this;
      },
    };
    await routes.get(route)({ user, params: { id }, body }, res);
    return result;
  };
  return { call, audit };
}

const report = (api, id, body, user = USERS.olena) => api.call("POST /ccl/matches/:id/cases", { user, id, body });
const resolve = (api, id, body, user = USERS.olena) => api.call("POST /ccl/matches/:id/resolve", { user, id, body });
const adjust = (api, id, body, user = USERS.olena) => api.call("POST /ccl/matches/:id/adjust-time", { user, id, body });
const run = (db, sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, (error) => (error ? reject(error) : resolve())));

test("the case a player may send depends on the match state", () => {
  const base = { player_1_id: "olena", player_2_id: "taras" };
  const scenario = (match, games = [], playerId = "olena") => getCclCaseScenario({ match: { ...base, ...match }, games, playerId, now: NOW });
  assert.equal(scenario({ status: "Planned" }), "scheduling");
  assert.equal(scenario({ status: "Planned", time_utc: "2026-11-25T18:00:00Z" }), null);
  // An agreed time that a player asked to change.
  assert.equal(scenario({ status: "Planned", time_utc: "2026-11-25T18:00:00Z", proposed_time_status: "change_pending" }), "scheduling");
  assert.equal(scenario({ status: "Error" }), "not_played");
  assert.equal(scenario({ status: "Error" }, [{ player_1_score: 0 }]), "incomplete");
  assert.equal(scenario({ status: "Error" }, [{ status: "No show" }]), "incomplete");
  // Players edit the result themselves for 24 hours after the match start.
  assert.equal(scenario({ status: "Done", time_utc: "2026-11-21T10:00:00Z" }), null);
  assert.equal(scenario({ status: "Done", time_utc: "2026-11-19T18:00:00Z" }), "result_dispute");
  assert.equal(scenario({ status: "Draft", player_2_id: null }), null);
  assert.equal(scenario({ status: "Cancelled" }), null);
  assert.equal(scenario({ status: "Error" }, [], "max"), null);
  assert.equal(isCclGamePlayed({ player_1_score: null, status: "" }), false);
});

test("Report a problem saves a case of the match scenario", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const sent = await report(api, "no-time", { category: "scheduling", details: "Taras does not answer" });
  assert.equal(sent.status, 201, JSON.stringify(sent.body));
  assert.equal(sent.body.appended, false);
  assert.deepEqual(
    [sent.body.case.case_type, sent.body.case.category, sent.body.case.duel_id, sent.body.case.tournament_id],
    ["problem", "scheduling", "no-time", "CCL-2027"]
  );
  assert.equal(api.audit.at(-1).event_type, "tournament_case.created");

  // A second report on the same match adds to the open case.
  const again = await report(api, "no-time", { category: "scheduling", details: "Still nothing" });
  assert.equal(again.body.appended, true);
  assert.equal(again.body.case.id, sent.body.case.id);
  assert.equal(api.audit.at(-1).event_type, "tournament_case.updated");

  const partial = await report(api, "error-partial", { category: "incomplete", details: "Disconnected in game 2" });
  assert.equal(partial.body.case.category, "incomplete");
  const dispute = await report(api, "done-old", { details: "Game 2 was not ours" });
  assert.equal(dispute.body.case.category, "result_dispute");

  // The match does not change.
  const duel = await get(db, "SELECT status, time_utc FROM duels WHERE id = 'error-partial'");
  assert.deepEqual({ ...duel }, { status: "Error", time_utc: "2026-11-20T18:00:00Z" });
});

test("Report a problem is refused when the match state does not allow it", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  assert.equal((await report(api, "timed", { details: "x" })).status, 409);
  assert.equal((await report(api, "done-fresh", { details: "x" })).status, 409);
  assert.equal((await report(api, "no-time", { category: "incomplete", details: "x" })).status, 409);
  assert.equal((await report(api, "no-time", { details: " " })).status, 400);
  assert.equal((await report(api, "no-time", { details: "x" }, USERS.stranger)).status, 403);
  assert.equal((await report(api, "team-duel", { details: "x" })).status, 404);
  assert.equal((await report(api, "missing", { details: "x" })).status, 404);
  assert.deepEqual(await all(db, "SELECT id FROM tournament_cases"), []);
});

test("Resolve → Other saves a Match was not played case and keeps the match in Error", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const sent = await report(api, "error-empty", { category: "not_played", details: "BGA was down" });
  assert.equal(sent.status, 201, JSON.stringify(sent.body));
  assert.deepEqual([sent.body.case.case_type, sent.body.case.category], ["problem", "not_played"]);
  // A type or category from the client does not change the case.
  const again = await report(api, "error-empty", { case_type: "complaint", details: "More" });
  assert.equal(again.body.case.id, sent.body.case.id);
  assert.equal(again.body.case.case_type, "problem");
  assert.equal((await report(api, "error-empty", { category: "conduct", details: "x" })).status, 409);
  assert.equal((await get(db, "SELECT status FROM duels WHERE id = 'error-empty'")).status, "Error");
});

test("Resolve no-show saves a case and resets the match time", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const result = await resolve(api, "error-empty", { action: "no_show", no_show_player_id: "taras" });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.equal(result.body.case.category, "not_played");
  assert.equal(result.body.case.details, "Player taras did not show up for the match.");

  const duel = await get(db, "SELECT * FROM duels WHERE id = 'error-empty'");
  assert.equal(duel.status, "Planned");
  assert.equal(duel.time_utc, null);
  assert.equal(duel.proposed_time_status, null);
  assert.equal(duel.results_last_error, null);
  assert.equal(duel.updated_by, "olena");
  const events = api.audit.map((entry) => entry.event_type);
  assert.deepEqual(events, ["ccl.match.issue_resolved", "tournament_case.created"]);
  assert.deepEqual(api.audit[0].changes.status, { old: "Error", new: "Planned" });

  // The match is no longer in Error.
  assert.equal((await resolve(api, "error-empty", { action: "reschedule" })).status, 409);
});

test("Resolve no-show keeps the Details the player edited", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const result = await resolve(api, "error-empty", {
    action: "no_show", no_show_player_id: "taras", details: "Player taras did not show up. Waited 20 minutes.",
  });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.equal(result.body.case.details, "Player taras did not show up. Waited 20 minutes.");
  assert.equal((await resolve(createApi(await createDatabase(t)), "error-empty", {
    action: "no_show", no_show_player_id: "taras", details: "x".repeat(5001),
  })).status, 400);
});

test("Resolve reschedule resets the time without a case", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const result = await resolve(api, "error-empty", { action: "reschedule" }, USERS.taras);
  assert.equal(result.status, 200);
  assert.equal(result.body.case, null);
  assert.equal((await get(db, "SELECT status FROM duels WHERE id = 'error-empty'")).status, "Planned");
  assert.deepEqual(await all(db, "SELECT id FROM tournament_cases"), []);
});

test("Resolve is refused for other matches, actions and players", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  assert.equal((await resolve(api, "error-partial", { action: "reschedule" })).status, 409);
  assert.equal((await resolve(api, "error-empty", { action: "cancel" })).status, 400);
  assert.equal((await resolve(api, "error-empty", { action: "no_show", no_show_player_id: "max" })).status, 400);
  assert.equal((await resolve(api, "error-empty", { action: "reschedule" }, USERS.stranger)).status, 403);
  assert.equal((await get(db, "SELECT status FROM duels WHERE id = 'error-empty'")).status, "Error");
});

test("Adjust the match time keeps the new time when BGA gives a winner", async (t) => {
  const db = await createDatabase(t);
  const calls = [];
  const api = createApi(db, {
    async runDuelResultsUpdate(duelId) {
      calls.push(duelId);
      const duel = await get(db, "SELECT time_utc FROM duels WHERE id = ?", [duelId]);
      assert.equal(duel.time_utc, "2026-11-20T17:30:00.000Z");
      await run(db, "UPDATE duels SET status = 'Done', dw1 = 2, dw2 = 1 WHERE id = ?", [duelId]);
      await run(db, "INSERT INTO games (duel_id, player_1_score, player_1_rank) VALUES (?, 90, 1)", [duelId]);
    },
  });
  const result = await adjust(api, "error-partial", { time_utc: "2026-11-20T17:30:00Z" });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.deepEqual(calls, ["error-partial"]);
  assert.deepEqual(result.body.match, { id: "error-partial", time_utc: "2026-11-20T17:30:00.000Z", status: "Done", dw1: 2, dw2: 1 });
  assert.equal((await all(db, "SELECT id FROM games WHERE duel_id = 'error-partial'")).length, 2);
  assert.equal(api.audit.at(-1).event_type, "ccl.match.time_adjusted");
});

test("Adjust the match time puts the match and its games back when there is still no winner", async (t) => {
  const db = await createDatabase(t);
  await run(db, "UPDATE duels SET dw1 = 1, dw2 = 0 WHERE id = 'error-partial'");
  const before = await all(db, "SELECT * FROM games WHERE duel_id = 'error-partial' ORDER BY id");
  const api = createApi(db, {
    async runDuelResultsUpdate(duelId) {
      await run(db, "UPDATE duels SET status = 'Error', dw1 = 1, dw2 = 1, results_last_error = 'x' WHERE id = ?", [duelId]);
      await run(db, "UPDATE games SET player_1_score = 5 WHERE duel_id = ?", [duelId]);
      await run(db, "INSERT INTO games (duel_id, player_2_score, player_2_rank) VALUES (?, 80, 1)", [duelId]);
    },
  });
  const result = await adjust(api, "error-partial", { time_utc: "2026-11-20T18:45:00Z" });
  assert.equal(result.status, 409);
  assert.equal(result.body.code, "still_incomplete");
  const duel = await get(db, "SELECT * FROM duels WHERE id = 'error-partial'");
  assert.deepEqual(
    [duel.time_utc, duel.status, duel.dw1, duel.dw2, duel.results_last_error],
    ["2026-11-20T18:00:00Z", "Error", 1, 0, null]
  );
  assert.deepEqual(await all(db, "SELECT * FROM games WHERE duel_id = 'error-partial' ORDER BY id"), before);
});

test("Adjust the match time accepts up to an hour, only for a partly played match", async (t) => {
  const db = await createDatabase(t);
  let called = false;
  const api = createApi(db, { async runDuelResultsUpdate() { called = true; } });
  assert.equal((await adjust(api, "error-partial", { time_utc: "2026-11-20T19:01:00Z" })).status, 400);
  assert.equal((await adjust(api, "error-partial", { time_utc: "2026-11-20T18:00:00Z" })).status, 400);
  assert.equal((await adjust(api, "error-partial", { time_utc: "soon" })).status, 400);
  assert.equal((await adjust(api, "error-empty", { time_utc: "2026-11-20T17:30:00Z" })).status, 409);
  assert.equal((await adjust(api, "error-partial", { time_utc: "2026-11-20T17:30:00Z" }, USERS.stranger)).status, 403);
  assert.equal(called, false);
  assert.equal((await adjust(createApi(db), "error-partial", { time_utc: "2026-11-20T17:30:00Z" })).status, 503);
});
