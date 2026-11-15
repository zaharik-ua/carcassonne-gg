import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import { getCclFormat } from "./ccl-formats.js";
import {
  buildCclStructurePlan,
  normalizeStageStartDate,
  registerCclStructureRoutes,
  weekDeadlineUtc,
} from "./ccl-structure.js";
import { ensureDuelBracketColumns, ensureTournamentStructureSchema } from "./tournament-structure-schema.js";

const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (error) => (error ? reject(error) : resolve())));
const all = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || []))));
const run = (db, sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, (error) => (error ? reject(error) : resolve())));

const USERS = {
  admin: { id: 1, admin: 1, player_id: "admin-player" },
  tournamentAdmin: { id: 2, admin: 0, player_id: "ta" },
  player: { id: 3, admin: 0, player_id: "olena" },
};
const START_DATES = { Q1: "2026-11-23", Q2: "2026-11-23", Q3: "2026-11-16" };

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    PRAGMA foreign_keys = ON;
    CREATE TABLE tournaments (
      id TEXT PRIMARY KEY COLLATE NOCASE, name TEXT, type TEXT, category TEXT,
      ranking INTEGER, is_test INTEGER, format_key TEXT
    );
    CREATE TABLE tournament_access_users (tournament_entity_type TEXT, tournament_id TEXT, user_id INTEGER, role TEXT);
    CREATE TABLE profiles (id TEXT PRIMARY KEY, bga_nickname TEXT, name TEXT, avatar TEXT, association TEXT,
      gg_elo REAL, gg_rating_position INTEGER, deleted_at TEXT);
    CREATE TABLE duels (
      id TEXT PRIMARY KEY, tournament_id TEXT, match_id TEXT, duel_number INTEGER, duel_format TEXT, time_utc TEXT,
      player_1_id TEXT, player_2_id TEXT, dw1 INTEGER, dw2 INTEGER, ranking INTEGER NOT NULL DEFAULT 1, status TEXT,
      source_type TEXT, is_test INTEGER NOT NULL DEFAULT 0, stage TEXT, group_name TEXT, round_name TEXT,
      round_order INTEGER, deadline_utc TEXT, created_by TEXT, updated_by TEXT, deleted_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO tournaments VALUES
      ('CCL-2027', 'CCL 2027', 'Individuals', 'CCL', 1, 0, 'ccl-2027'),
      ('CCL-2028', 'CCL 2028', 'Individuals', 'CCL', 1, 0, NULL),
      ('ETCOC-2026', 'ETCOC', 'Teams', 'ETCOC', 1, 0, NULL);
    INSERT INTO tournament_access_users VALUES ('tournament', 'CCL-2027', 2, 'admin');
    INSERT INTO profiles (id, bga_nickname) VALUES ('olena', 'Olena'), ('taras', 'taras');
  `);
  await ensureTournamentStructureSchema(db);
  await ensureDuelBracketColumns(db);
  return db;
}

function createApi(db) {
  const routes = new Map();
  const app = {};
  for (const method of ["get", "post", "patch", "delete"]) {
    app[method] = (routePath, ...handlers) => routes.set(`${method.toUpperCase()} ${routePath}`, handlers.at(-1));
  }
  const audit = [];
  registerCclStructureRoutes(app, {
    db,
    requireAuthenticated: () => {},
    logAuditEvent: (entry, done) => { audit.push(entry); done(); },
    logger: { error() {} },
  });
  const call = async (route, { user = USERS.admin, params = { tournamentId: "CCL-2027" }, body = {} } = {}) => {
    const handler = routes.get(route);
    assert.ok(handler, `route ${route} is not registered`);
    const result = { status: 200, body: null };
    const res = { status(code) { result.status = code; return this; }, json(value) { result.body = value; return this; } };
    await handler({ user, params, body, query: {} }, res);
    return result;
  };
  return { call, audit };
}

const generate = (api, body = { start_dates: START_DATES }, options = {}) => api.call("POST /ccl/:tournamentId/structure", { body, ...options });

test("start dates are Mondays; a week ends on Sunday 23:59 UTC", () => {
  assert.deepEqual(normalizeStageStartDate("2026-11-16"), { value: "2026-11-16" });
  assert.deepEqual(normalizeStageStartDate(""), { value: null });
  assert.match(normalizeStageStartDate("2026-11-17").error, /Monday/);
  assert.match(normalizeStageStartDate("2026-02-30").error, /YYYY-MM-DD/);
  assert.equal(weekDeadlineUtc("2026-11-16", 1), "2026-11-22T23:59:00.000Z");
  assert.equal(weekDeadlineUtc("2026-11-23", 5), "2026-12-27T23:59:00.000Z");
  assert.equal(weekDeadlineUtc(null, 1), null);
});

test("CCL 2027 plan: 3 top stages, 3 paths, 12 groups, 162 matches", () => {
  const plan = buildCclStructurePlan({ tournamentId: "CCL-2027", format: getCclFormat("ccl-2027"), startDates: START_DATES });
  assert.deepEqual(plan.stages.map((stage) => [stage.stage_key, stage.parent_id]), [
    ["qualifiers", null], ["Q1", "CCL-2027|qualifiers"], ["Q2", "CCL-2027|qualifiers"], ["Q3", "CCL-2027|qualifiers"],
    ["league", null], ["playoffs", null],
  ]);
  assert.equal(plan.groups.map((group) => group.group_key).join(""), "ABCDEFGHIXYZ");
  assert.equal(plan.matches.length, 5 * 13 + 4 * 13 + 3 * 15);
  // Matches that send a player to the League Stage carry its label.
  const groupA = plan.matches.filter((match) => match.group_key === "A");
  assert.deepEqual(groupA.filter((match) => match.bracket_label).map((match) => [match.bracket_no, match.bracket_label]), [
    [11, "Winner"], [13, "2nd Place"],
  ]);
  const groupX = plan.matches.filter((match) => match.group_key === "X");
  assert.deepEqual(groupX.filter((match) => match.bracket_label).map((match) => match.bracket_no), [14, 15]);
  // Deadlines follow the path calendar: Q1 Consolidated Final by 27.12, Q3 Group Final by 27.12.
  assert.equal(groupA.at(-1).deadline_utc, "2026-12-27T23:59:00.000Z");
  assert.equal(groupX.at(-1).deadline_utc, "2026-12-27T23:59:00.000Z");
  assert.equal(groupX[0].deadline_utc, "2026-11-22T23:59:00.000Z");
});

test("admins generate the structure; matches are empty Drafts with placeholders", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  assert.equal((await generate(api, undefined, { user: USERS.player })).status, 403);
  assert.equal((await generate(api, undefined, { params: { tournamentId: "ETCOC-2026" } })).status, 404);
  assert.match((await generate(api, undefined, { params: { tournamentId: "CCL-2028" } })).body.message, /format/);
  assert.match((await generate(api, { start_dates: { Q3: "2026-11-17" } })).body.message, /Q3: .*Monday/);
  assert.match((await generate(api, { start_dates: { Q9: "2026-11-16" } })).body.message, /Unknown stage/);

  const created = await generate(api, undefined, { user: USERS.tournamentAdmin });
  assert.equal(created.status, 200, JSON.stringify(created.body));
  assert.deepEqual(created.body.generated, ["Q1", "Q2", "Q3"]);
  assert.equal(api.audit.at(-1).event_type, "ccl.structure.generated");

  const duels = await all(db, "SELECT * FROM duels ORDER BY stage, group_name, bracket_no");
  assert.equal(duels.length, 162);
  assert.ok(duels.every((duel) => duel.status === "Draft" && duel.source_type === "ccl" && duel.duel_format === "Bo3"
    && duel.player_1_id === null && duel.player_2_id === null && duel.tournament_id === "CCL-2027"));
  const x15 = duels.find((duel) => duel.group_name === "X" && duel.bracket_no === 15);
  assert.equal(x15.is_conditional, 1);
  assert.equal(x15.round_name, "Group Final Reset");
  assert.equal(x15.stage, "Q3");

  const structure = (await api.call("GET /ccl/:tournamentId/structure")).body;
  assert.deepEqual(structure.stages.map((stage) => stage.name), ["Qualifiers", "League Stage", "Playoffs"]);
  const [q1, , q3] = structure.stages[0].children;
  assert.equal(q1.status, "structure");
  assert.equal(q1.start_date, "2026-11-23");
  assert.equal(q1.settings.grand_final, "none");
  const first = q1.groups[0].matches[0];
  assert.deepEqual([first.player_1.placeholder, first.player_2.placeholder], ["Seeded #1", "Unseeded #1"]);
  assert.deepEqual([first.next_win, first.next_lose], [7, 5]);
  const final = q3.groups[0].matches.find((match) => match.bracket_no === 14);
  assert.deepEqual([final.next_win, final.next_lose], [15, 15]);
  assert.equal(q1.groups[0].matches.find((match) => match.bracket_no === 11).next_win, null);
});

test("regeneration replaces only paths that are not drawn and not played", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await generate(api);
  const q3Before = await all(db, "SELECT id FROM duels WHERE stage = 'Q3'");

  // Q1 is drawn: it stays as it is. Q2 keeps its start date when no new one is sent.
  await run(db, "UPDATE tournament_stages SET status = 'drawn' WHERE stage_key = 'Q1'");
  await run(db, "UPDATE duels SET player_1_id = 'olena', status = 'Planned' WHERE stage = 'Q1' AND group_name = 'A' AND bracket_no = 1");
  const again = await generate(api, { start_dates: { Q3: "2026-11-23" } });
  assert.equal(again.status, 200, JSON.stringify(again.body));
  assert.deepEqual(again.body.generated, ["Q2", "Q3"]);
  assert.deepEqual(again.body.skipped, [{ stage: "Q1", status: "drawn" }]);
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM duels")).at(0).n, 162);
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM duels WHERE player_1_id = 'olena'")).at(0).n, 1);
  const q3After = await all(db, "SELECT id, deadline_utc FROM duels WHERE stage = 'Q3' ORDER BY bracket_no");
  assert.equal(q3After.some((duel) => q3Before.some((old) => old.id === duel.id)), false);
  assert.equal(q3After[0].deadline_utc, "2026-11-29T23:59:00.000Z");
  const q2 = (await all(db, "SELECT start_date FROM tournament_stages WHERE stage_key = 'Q2'"))[0];
  assert.equal(q2.start_date, "2026-11-23");

  // A path still in "structure" but with a played match is not wiped.
  await run(db, "UPDATE duels SET time_utc = '2026-11-20T18:00:00Z' WHERE stage = 'Q2' AND bracket_no = 1 AND group_name = 'F'");
  const blocked = await generate(api, {});
  assert.equal(blocked.status, 409);
  assert.equal(blocked.body.blockers.length, 1);
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM duels")).at(0).n, 162);

  // Everything drawn: nothing to regenerate.
  await run(db, "UPDATE tournament_stages SET status = 'drawn' WHERE format IS NOT NULL AND format <> 'league' AND format <> 'single_elimination'");
  assert.equal((await generate(api, {})).status, 409);
});

test("a new start date moves the deadlines of a path", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await generate(api, { start_dates: {} });
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM duels WHERE deadline_utc IS NOT NULL")).at(0).n, 0);

  const route = "PATCH /ccl/:tournamentId/stages/:stageKey";
  const moved = await api.call(route, { params: { tournamentId: "CCL-2027", stageKey: "q3" }, body: { start_date: "2026-11-16" } });
  assert.equal(moved.status, 200, JSON.stringify(moved.body));
  assert.equal(moved.body.matches_updated, 45);
  const deadlines = await all(db, "SELECT DISTINCT round_order, deadline_utc FROM duels WHERE stage = 'Q3' ORDER BY round_order");
  assert.deepEqual(deadlines.map((row) => row.deadline_utc.slice(0, 10)), [
    "2026-11-22", "2026-11-29", "2026-12-06", "2026-12-13", "2026-12-20", "2026-12-27",
  ]);
  assert.equal((await api.call(route, { params: { tournamentId: "CCL-2027", stageKey: "Q3" }, body: { start_date: "2026-11-18" } })).status, 400);
  assert.equal((await api.call(route, { params: { tournamentId: "CCL-2027", stageKey: "qualifiers" }, body: { start_date: "2026-11-16" } })).status, 404);
  assert.equal((await api.call(route, { user: USERS.player, params: { tournamentId: "CCL-2027", stageKey: "Q3" }, body: { start_date: "2026-11-16" } })).status, 403);
});
