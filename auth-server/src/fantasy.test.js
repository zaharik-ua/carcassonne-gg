import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import sqlite3 from "sqlite3";

import {
  describeTeamState,
  ensureFantasySchema,
  getFantasyStatus,
  matchPoints,
  normalizeLeagueSettings,
  normalizeTeamInput,
  rankTeams,
  registerFantasyRoutes,
  resultCounts,
  SEED_LEAGUES,
} from "./fantasy.js";
import {
  applyFantasyPricePlan,
  buildFantasyPricePlan,
  parseCsv,
  readPriceRows,
} from "./fantasy-import.js";

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function close(db) {
  return new Promise((resolve) => db.close(resolve));
}

const LEAGUE_ID = "CCL-2027-Q";
const LEAGUE = { ...SEED_LEAGUES[0], stages: ["Q1", "Q2", "Q3"] };
const UPCOMING = new Date("2026-11-23T12:00:00Z");
const OPEN = new Date("2026-11-25T12:00:00Z");
const LATE = new Date("2026-11-30T12:00:00Z");
const CLOSED = new Date("2026-12-05T00:00:00Z");

const USERS = {
  fan: { id: 1, name: "Fan One" },
  player: { id: 2, name: "Jan Account", player_id: "p1", bga_nickname: "Jan" },
  other: { id: 3, name: "Other Fan" },
  admin: { id: 10, admin: 1, email: "admin@example.com" },
  leagueAdmin: { id: 11, admin: 0, email: "matt@example.com" },
  stranger: { id: 12, admin: 0 },
};

// Slots s1–s7 are qualifiers with prices; m1 is a Main slot (priced, but not in the league stages);
// s8 has no player yet; x1 belongs to another tournament.
async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => close(db));
  await exec(db, `
    PRAGMA foreign_keys = ON;
    CREATE TABLE tournaments (id TEXT PRIMARY KEY COLLATE NOCASE, name TEXT NOT NULL, type TEXT, category TEXT);
    INSERT INTO tournaments VALUES
      ('CCL-2027', 'Carcassonne Champions League 2027', 'Individuals', 'CCL'),
      ('CCL-2026', 'Carcassonne Champions League 2026', 'Individuals', 'CCL');
    CREATE TABLE associations (association_row_id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, name TEXT NOT NULL, flag TEXT);
    INSERT INTO associations (code, name, flag) VALUES ('POL', 'Poland', 'pl.png'), ('UKR', 'Ukraine', 'ua.png');
    CREATE TABLE profiles (
      id TEXT PRIMARY KEY, bga_nickname TEXT, name TEXT, deleted_at TEXT,
      avatar TEXT, bga_elo REAL, gg_elo REAL, gg_rating_position INTEGER
    );
    INSERT INTO profiles (id, bga_nickname) VALUES
      ('p1', 'Jan'), ('p2', 'Kasia'), ('p3', 'Taras'), ('p4', 'Olena'), ('p5', 'Marek'), ('p6', 'Ivan'),
      ('p7', 'Zosia'), ('p9', 'Main Star'), ('p10', 'Old Timer');
    CREATE TABLE tournament_players (
      id TEXT PRIMARY KEY COLLATE NOCASE, tournament_id TEXT NOT NULL, team_id TEXT, player_id TEXT,
      association_id TEXT, entry_stage TEXT, league_status TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO tournament_players (id, tournament_id, player_id, association_id, entry_stage, league_status) VALUES
      ('s1', 'CCL-2027', 'p1', 'POL', 'Q1', NULL),
      ('s2', 'CCL-2027', 'p2', 'POL', 'Q1', NULL),
      ('s3', 'CCL-2027', 'p3', 'UKR', 'Q2', NULL),
      ('s4', 'CCL-2027', 'p4', 'UKR', 'Q2', NULL),
      ('s5', 'CCL-2027', 'p5', 'POL', 'Q3', NULL),
      ('s6', 'CCL-2027', 'p6', 'UKR', 'Q3', 'eliminated'),
      ('s7', 'CCL-2027', 'p7', 'POL', 'Q3', NULL),
      ('s8', 'CCL-2027', NULL, 'POL', 'Q3', NULL),
      ('m1', 'CCL-2027', 'p9', 'POL', 'Main', NULL),
      ('x1', 'CCL-2026', 'p10', 'POL', 'Q1', NULL);
    CREATE TABLE ccl_stage_entries (tournament_id TEXT, stage TEXT, slot_id TEXT, seed TEXT);
    INSERT INTO ccl_stage_entries VALUES ('CCL-2027', 'Q1', 's1', 'Seeded'), ('CCL-2027', 'Q1', 's2', 'Unseeded');
    CREATE TABLE duels (
      id TEXT PRIMARY KEY, tournament_id TEXT, source_type TEXT, deleted_at TEXT, stage TEXT, group_name TEXT,
      round_name TEXT, player_1_id TEXT, player_2_id TEXT, status TEXT, dw1 INTEGER, dw2 INTEGER, time_utc TEXT,
      is_test INTEGER NOT NULL DEFAULT 0, deadline_utc TEXT, duel_format TEXT
    );
    INSERT INTO duels (id, tournament_id, source_type, deleted_at, stage, group_name, player_1_id, player_2_id, status) VALUES
      ('d1', 'CCL-2027', 'ccl', NULL, 'Q1', 'A', 'p1', 'p2', 'Planned'),
      ('d2', 'CCL-2027', 'ccl', NULL, 'Q3', 'X', 'p5', 'p6', 'Planned');
    CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT, bga_id TEXT);
    INSERT INTO users (id, email, name, bga_id) VALUES
      (1, 'fan@example.com', 'Fan One', NULL), (2, 'jan@example.com', 'Jan Account', 'p1'), (3, NULL, 'Other Fan', NULL),
      (10, 'admin@example.com', 'Admin', NULL), (11, 'Matt@Example.com', 'Matt', NULL), (12, 'x@example.com', 'X', NULL),
      (13, 'twin@example.com', 'Twin A', NULL), (14, 'twin@example.com', 'Twin B', NULL);
  `);
  await ensureFantasySchema(db);
  await ensureFantasySchema(db);
  await exec(db, `
    INSERT INTO fantasy_league_admins (league_id, user_id, added_by) VALUES ('CCL-2027-Q', 11, 'test');
    INSERT INTO fantasy_prices (league_id, slot_id, stage, price, effective_from, source_record_id) VALUES
      ('CCL-2027-Q', 's1', 'Q1', 20, '2026-11-23T00:00:00.000Z', '101'),
      ('CCL-2027-Q', 's2', 'Q1', 18.5, '2026-11-23T00:00:00.000Z', '102'),
      ('CCL-2027-Q', 's3', 'Q2', 10, '2026-11-23T00:00:00.000Z', '103'),
      ('CCL-2027-Q', 's3', 'Q2', 12, '2026-11-26T00:00:00.000Z', '103'),
      ('CCL-2027-Q', 's4', 'Q2', 8, '2026-11-23T00:00:00.000Z', '104'),
      ('CCL-2027-Q', 's5', 'Q3', 6, '2026-11-23T00:00:00.000Z', '105'),
      ('CCL-2027-Q', 's6', 'Q3', 5, '2026-11-23T00:00:00.000Z', '106'),
      ('CCL-2027-Q', 's7', 'Q3', 3.5, '2026-11-23T00:00:00.000Z', '107'),
      ('CCL-2027-Q', 'm1', 'Main', 10, '2026-11-23T00:00:00.000Z', '109');
  `);
  return db;
}

function createApi(db, { now = OPEN } = {}) {
  const routes = new Map();
  const app = {};
  for (const method of ["get", "post", "put", "patch", "delete"]) {
    app[method] = (routePath, ...handlers) => routes.set(`${method.toUpperCase()} ${routePath}`, handlers.at(-1));
  }
  const audit = [];
  const clock = { now };
  registerFantasyRoutes(app, {
    db,
    getAuditActor: (user) => ({ actor_user_id: user?.id ?? null }),
    logAuditEvent: (entry, done) => {
      audit.push(entry);
      done();
    },
    logger: { error() {} },
    now: () => clock.now,
  });
  const call = async (route, { user = null, leagueId = LEAGUE_ID, params = {}, body = {}, query = {} } = {}) => {
    const handler = routes.get(route);
    assert.ok(handler, `route ${route} is not registered`);
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
    await handler({ user, params: { leagueId, ...params }, body, query }, res);
    return result;
  };
  return { call, audit, clock };
}

const state = (api, user = null) => api.call("GET /public/fantasy/:leagueId", { user });
// Accounts without a linked BGA profile must give a BGA nickname; a linked account's one is ignored.
const save = (api, user, slotIds, teamName = "Meeple Masters", nickname = "fan_bga") => (
  api.call("PUT /fantasy/:leagueId/team", { user, body: { team_name: teamName, slot_ids: slotIds, bga_nickname: nickname } })
);
const commit = (api, user, slotIds, teamName = "Meeple Masters", nickname = "fan_bga") => (
  api.call("POST /fantasy/:leagueId/team/commit", { user, body: { team_name: teamName, slot_ids: slotIds, bga_nickname: nickname } })
);
const teams = (api, user) => api.call("GET /fantasy/:leagueId/teams", { user });

const CHEAP_FIVE = ["s3", "s4", "s5", "s6", "s7"]; // 10 + 8 + 6 + 5 + 3.5 = 32.5

test("the qualifiers league is created once its tournament exists", async (t) => {
  const db = await createDatabase(t);
  const rows = await all(db, "SELECT id, tournament_id, stages, team_size, budget, main_deadline FROM fantasy_leagues");
  assert.deepEqual(rows, [{
    id: "CCL-2027-Q", tournament_id: "CCL-2027", stages: "Q1,Q2,Q3", team_size: 5, budget: 50,
    main_deadline: "2026-11-27T23:59:59.000Z",
  }]);

  const empty = new sqlite3.Database(":memory:");
  t.after(() => close(empty));
  await exec(empty, "CREATE TABLE tournaments (id TEXT PRIMARY KEY COLLATE NOCASE, name TEXT);");
  await ensureFantasySchema(empty);
  assert.deepEqual(await all(empty, "SELECT id FROM fantasy_leagues"), []);
});

test("league status moves through upcoming, open, late and closed at the deadlines", () => {
  assert.equal(getFantasyStatus(LEAGUE, UPCOMING), "upcoming");
  assert.equal(getFantasyStatus(LEAGUE, new Date(LEAGUE.opens_at)), "open");
  assert.equal(getFantasyStatus(LEAGUE, new Date(Date.parse(LEAGUE.main_deadline) - 1000)), "open");
  assert.equal(getFantasyStatus(LEAGUE, new Date(LEAGUE.main_deadline)), "late");
  assert.equal(getFantasyStatus(LEAGUE, new Date(Date.parse(LEAGUE.late_deadline) - 1000)), "late");
  assert.equal(getFantasyStatus(LEAGUE, new Date(LEAGUE.late_deadline)), "closed");
});

test("team state: full main teams lock at the main deadline, late teams only by Commit", () => {
  const main = { entry_type: "main", locked_at: null };
  const late = { entry_type: "late", locked_at: null };
  const committed = { entry_type: "late", locked_at: "2026-11-30T12:00:00.000Z" };
  assert.deepEqual(describeTeamState(main, 5, LEAGUE, "open"), { state: "complete", locked: false, can_edit: true, can_commit: false });
  assert.deepEqual(describeTeamState(main, 3, LEAGUE, "open"), { state: "draft", locked: false, can_edit: true, can_commit: false });
  assert.deepEqual(describeTeamState(main, 5, LEAGUE, "late"), { state: "locked", locked: true, can_edit: false, can_commit: false });
  assert.deepEqual(describeTeamState(main, 3, LEAGUE, "late"), { state: "draft", locked: false, can_edit: true, can_commit: false });
  assert.deepEqual(describeTeamState(late, 5, LEAGUE, "late"), { state: "draft", locked: false, can_edit: true, can_commit: true });
  assert.deepEqual(describeTeamState(committed, 5, LEAGUE, "late"), { state: "committed", locked: true, can_edit: false, can_commit: false });
  assert.equal(describeTeamState(late, 5, LEAGUE, "closed").state, "not_entered");
  assert.equal(describeTeamState(main, 5, LEAGUE, "closed").state, "locked");
});

test("team input needs a name and at most 5 different players", () => {
  assert.equal(normalizeTeamInput({ team_name: "  ", slot_ids: [] }, LEAGUE).error, "Enter a team name");
  assert.match(normalizeTeamInput({ team_name: "x".repeat(41), slot_ids: [] }, LEAGUE).error, /at most 40/);
  assert.equal(normalizeTeamInput({ team_name: "A", slot_ids: "s1" }, LEAGUE).error, "slot_ids must be a list");
  assert.equal(normalizeTeamInput({ team_name: "A", slot_ids: ["s1", "S1"] }, LEAGUE).error, "A player can be picked only once");
  assert.match(normalizeTeamInput({ team_name: "A", slot_ids: ["s1", "s2", "s3", "s4", "s5", "s6"] }, LEAGUE).error, /at most 5/);
  assert.deepEqual(
    normalizeTeamInput({ team_name: "  My   team ", slot_ids: [" s1 "] }, LEAGUE).value,
    { team_name: "My team", slot_ids: ["s1"] }
  );
});

test("league settings: name, budget and dates in order, nothing else", () => {
  assert.deepEqual(
    normalizeLeagueSettings({ budget: 55.5, main_deadline: "2026-11-28T23:59:59Z" }, LEAGUE).value,
    { budget: 55.5, main_deadline: "2026-11-28T23:59:59.000Z" }
  );
  assert.match(normalizeLeagueSettings({ budget: 50.3 }, LEAGUE).error, /steps of 0.5/);
  assert.match(normalizeLeagueSettings({ opens_at: "2026-11-24" }, LEAGUE).error, /date and time in UTC/);
  assert.match(normalizeLeagueSettings({ late_deadline: "2026-11-26T00:00:00Z" }, LEAGUE).error, /in order/);
  assert.equal(normalizeLeagueSettings({ name: " " }, LEAGUE).error, "Enter the league name");
  assert.equal(normalizeLeagueSettings({ team_size: 6, stages: "Main" }, LEAGUE).error, "Nothing to change");
});

test("state lists priced players of the league stages with the current price, group, seed and elimination", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const result = await state(api);
  assert.equal(result.status, 200);
  assert.equal(result.body.league.status, "open");
  assert.equal(result.body.league.budget, 50);
  assert.deepEqual(result.body.league.stages, ["Q1", "Q2", "Q3"]);
  assert.deepEqual(result.body.viewer, { authenticated: false, name: null, team: null });
  // The Main slot, the empty slot and the other tournament are left out; prices are the ones in force now.
  assert.deepEqual(
    result.body.players.map((player) => [player.slot_id, player.price]),
    [["s1", 20], ["s2", 18.5], ["s3", 10], ["s4", 8], ["s5", 6], ["s6", 5], ["s7", 3.5]]
  );
  const jan = result.body.players[0];
  assert.equal(jan.nickname, "Jan");
  assert.equal(jan.group, "A");
  assert.equal(jan.seed, "Seeded");
  assert.deepEqual(jan.association, { id: "POL", name: "Poland", flag: "pl.png" });
  assert.equal(result.body.players.find((player) => player.slot_id === "s6").eliminated, true);

  api.clock.now = new Date("2026-11-27T12:00:00Z");
  const later = await state(api);
  assert.equal(later.body.players.find((player) => player.slot_id === "s3").price, 12);
  assert.equal((await api.call("GET /public/fantasy/:leagueId", { leagueId: "NOPE" })).status, 404);
});

test("a user saves a draft, completes it and sees only their own team", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const draft = await save(api, USERS.fan, ["s1", "s3"]);
  assert.equal(draft.status, 201);
  assert.equal(draft.body.team.state, "draft");
  assert.equal(draft.body.team.cost, 30);
  assert.equal(draft.body.team.entry_type, "main");

  const full = await save(api, USERS.fan, ["s1", "s3", "s4", "s5", "s7"], "Final Name");
  assert.equal(full.status, 200);
  assert.equal(full.body.team.id, draft.body.team.id);
  assert.equal(full.body.team.state, "complete");
  assert.equal(full.body.team.team_name, "Final Name");
  assert.equal(full.body.team.cost, 47.5);

  const mine = await state(api, USERS.fan);
  assert.equal(mine.body.viewer.name, "Fan One");
  assert.deepEqual(mine.body.viewer.team.players.map((player) => player.slot_id).sort(), ["s1", "s3", "s4", "s5", "s7"]);
  assert.equal((await state(api, USERS.other)).body.viewer.team, null);
  assert.equal((await state(api, USERS.player)).body.viewer.name, "Jan");
  assert.equal(api.audit.length, 2);
  assert.equal(api.audit[0].event_type, "fantasy_team.saved");
});

test("saving checks the budget and the players", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const over = await save(api, USERS.fan, ["s1", "s2", "s3", "s4"]);
  assert.equal(over.status, 400);
  assert.match(over.body.message, /costs 56\.5, over the budget of 50/);
  // Exactly the budget is allowed.
  assert.equal((await save(api, USERS.fan, ["s1", "s2", "s7", "s4"])).status, 201);
  for (const slot of ["m1", "x1", "s8", "nope"]) {
    const result = await save(api, USERS.other, [slot]);
    assert.equal(result.status, 400, slot);
    assert.match(result.body.message, /Player not available/);
  }
  assert.equal((await save(api, USERS.other, ["s1", "s1"])).status, 400);
  assert.equal((await api.call("PUT /fantasy/:leagueId/team", { user: null, body: {} })).status, 401);
  assert.equal((await all(db, "SELECT COUNT(*) AS count FROM fantasy_teams"))[0].count, 1);
});

test("players kept in a team keep the price they were picked at", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await save(api, USERS.fan, ["s3", "s4"]);
  api.clock.now = new Date("2026-11-27T12:00:00Z");
  const result = await save(api, USERS.fan, ["s3", "s4", "s5"]);
  assert.equal(result.status, 200);
  assert.deepEqual(
    result.body.team.players.map((player) => [player.slot_id, player.price_at_pick]),
    [["s3", 10], ["s4", 8], ["s5", 6]]
  );
  // Dropped and picked again: bought at today's price.
  await save(api, USERS.fan, ["s4"]);
  const again = await save(api, USERS.fan, ["s3", "s4"]);
  assert.deepEqual(again.body.team.players.map((player) => [player.slot_id, player.price_at_pick]), [["s3", 12], ["s4", 8]]);
});

test("nothing can be saved before opening or after the late deadline", async (t) => {
  const db = await createDatabase(t);
  const before = await save(createApi(db, { now: UPCOMING }), USERS.fan, ["s3"]);
  assert.equal(before.status, 409);
  assert.equal(before.body.message, "Team building has not started yet");
  const after = await save(createApi(db, { now: CLOSED }), USERS.fan, ["s3"]);
  assert.equal(after.status, 409);
  assert.equal(after.body.message, "Team building is closed");
});

test("a full main team is locked from the main deadline", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await save(api, USERS.fan, CHEAP_FIVE);
  api.clock.now = new Date(LEAGUE.main_deadline);
  const result = await save(api, USERS.fan, ["s1"]);
  assert.equal(result.status, 409);
  assert.equal(result.body.message, "Your team is locked and cannot be changed");
  const view = await state(api, USERS.fan);
  assert.equal(view.body.viewer.team.state, "locked");
  assert.equal(view.body.viewer.team.scoring_from, null);
  assert.equal((await commit(api, USERS.fan, CHEAP_FIVE)).status, 409);
});

test("a locked team can still be renamed; its players stay", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const rename = (user, teamName) => api.call("PATCH /fantasy/:leagueId/team/name", { user, body: { team_name: teamName } });
  assert.equal((await rename(USERS.fan, "Nobody")).status, 404);
  await save(api, USERS.fan, CHEAP_FIVE);
  api.clock.now = new Date(LEAGUE.late_deadline);
  assert.equal((await rename(USERS.fan, "  ")).status, 400);
  const result = await rename(USERS.fan, "  Road  Builders ");
  assert.equal(result.status, 200);
  assert.equal(result.body.team.team_name, "Road Builders");
  assert.equal(result.body.team.state, "locked");
  assert.deepEqual(result.body.team.players.map((player) => player.slot_id).sort(), [...CHEAP_FIVE].sort());
  assert.equal(api.audit.at(-1).event_type, "fantasy_team.renamed");
});

test("an account without a linked BGA profile gives its BGA nickname; league admins see it", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  assert.equal((await state(api, USERS.fan)).body.viewer.bga_linked, false);
  assert.equal((await state(api, USERS.player)).body.viewer.bga_linked, true);

  const missing = await save(api, USERS.fan, ["s3"], "Meeple Masters", "  ");
  assert.equal(missing.status, 400);
  assert.match(missing.body.message, /BGA nickname/);
  assert.equal((await save(api, USERS.fan, ["s3"], "Meeple Masters", "x".repeat(41))).status, 400);
  const saved = await save(api, USERS.fan, ["s3"], "Meeple Masters", "  Ludo  J ");
  assert.equal(saved.status, 201);
  assert.equal(saved.body.team.claimed_bga_nickname, "Ludo J");
  // A later save without the field keeps the nickname given before.
  const kept = await api.call("PUT /fantasy/:leagueId/team", { user: USERS.fan, body: { team_name: "Meeple Masters", slot_ids: ["s3", "s4"] } });
  assert.equal(kept.body.team.claimed_bga_nickname, "Ludo J");
  // A linked account needs no nickname and stores none.
  const linked = await api.call("PUT /fantasy/:leagueId/team", { user: USERS.player, body: { team_name: "Jan's Five", slot_ids: ["s3"], bga_nickname: "fake" } });
  assert.equal(linked.status, 201);
  assert.equal(linked.body.team.claimed_bga_nickname, null);

  // The rename route changes the nickname too, also of a locked team.
  api.clock.now = new Date(LEAGUE.late_deadline);
  const renamed = await api.call("PATCH /fantasy/:leagueId/team/name", {
    user: USERS.fan, body: { team_name: "Meeple Masters", bga_nickname: "LudoJ" },
  });
  assert.equal(renamed.status, 200);
  assert.equal(renamed.body.team.claimed_bga_nickname, "LudoJ");
  assert.deepEqual(api.audit.at(-1).changes, { claimed_bga_nickname: { old: "Ludo J", new: "LudoJ" } });

  const rows = Object.fromEntries((await teams(api, USERS.admin)).body.teams.map((row) => [row.team_name, row]));
  assert.equal(rows["Meeple Masters"].claimed_bga_nickname, "LudoJ");
  assert.equal(rows["Meeple Masters"].bga_nickname, null);
  assert.equal(rows["Meeple Masters"].email, "fan@example.com");
  assert.equal(rows["Jan's Five"].claimed_bga_nickname, null);
  assert.equal(rows["Jan's Five"].bga_nickname, "Jan");
});

test("a team can be deleted while it can be changed, not once it is locked", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const remove = (user) => api.call("DELETE /fantasy/:leagueId/team", { user });
  assert.equal((await remove(USERS.fan)).status, 404);
  await save(api, USERS.fan, CHEAP_FIVE);
  assert.equal((await remove(USERS.fan)).status, 200);
  assert.equal((await state(api, USERS.fan)).body.viewer.team, null);
  assert.equal(api.audit.at(-1).event_type, "fantasy_team.deleted");
  // A new team can be built after the delete.
  assert.equal((await save(api, USERS.fan, CHEAP_FIVE)).status, 201);
  api.clock.now = new Date(LEAGUE.main_deadline);
  const locked = await remove(USERS.fan);
  assert.equal(locked.status, 409);
  assert.equal(locked.body.message, "Your team is locked and cannot be deleted");
});

test("late entry: save keeps a draft, Commit locks it and starts scoring", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  // An incomplete main draft becomes a late entry after the main deadline.
  await save(api, USERS.fan, ["s3"]);
  api.clock.now = LATE;

  assert.equal((await commit(createApi(db, { now: OPEN }), USERS.other, CHEAP_FIVE)).status, 409);

  const draft = await save(api, USERS.fan, CHEAP_FIVE);
  assert.equal(draft.status, 200);
  assert.equal(draft.body.team.entry_type, "late");
  assert.equal(draft.body.team.state, "draft");
  assert.equal(draft.body.team.can_commit, true);
  assert.equal(draft.body.team.scoring_from, null);

  const short = await commit(api, USERS.fan, ["s3", "s4"]);
  assert.equal(short.status, 400);
  assert.match(short.body.message, /Pick 5 players/);

  const committed = await commit(api, USERS.fan, CHEAP_FIVE, "Late Team");
  assert.equal(committed.status, 200);
  assert.equal(committed.body.team.state, "committed");
  assert.equal(committed.body.team.locked_at, LATE.toISOString());
  assert.equal(committed.body.team.scoring_from, LATE.toISOString());
  assert.equal(api.audit.at(-1).event_type, "fantasy_team.committed");

  assert.equal((await save(api, USERS.fan, ["s3"])).status, 409);
  assert.equal((await commit(api, USERS.fan, CHEAP_FIVE)).status, 409);

  // A new user can commit straight away, without a draft first (s3 now costs 12: 49.5 in all).
  const direct = await commit(api, USERS.other, ["s1", "s3", "s4", "s5", "s7"]);
  assert.equal(direct.status, 200);
  assert.equal(direct.body.team.state, "committed");
});

test("late draft is priced at today's prices: a price rise over the budget blocks Commit until players are replaced", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db, { now: LATE });
  const draftSlots = ["s1", "s3", "s4", "s5", "s7"]; // 20 + 12 + 8 + 6 + 3.5 = 49.5
  const draft = await save(api, USERS.fan, draftSlots);
  assert.equal(draft.status, 201);
  assert.equal(draft.body.team.cost, 49.5);
  assert.equal(draft.body.team.can_commit, true);

  await exec(db, `
    INSERT INTO fantasy_prices (league_id, slot_id, stage, price, effective_from, source_record_id)
    VALUES ('CCL-2027-Q', 's5', 'Q3', 7, '2026-12-01T00:00:00.000Z', '105');
  `);
  api.clock.now = new Date("2026-12-02T12:00:00Z");

  const view = (await state(api, USERS.fan)).body.viewer.team;
  assert.equal(view.cost, 50.5);
  assert.equal(view.over_budget, true);
  assert.equal(view.can_commit, false);
  assert.deepEqual(view.players.find((player) => player.slot_id === "s5"), { slot_id: "s5", price_at_pick: 6, price: 7 });

  const blocked = await commit(api, USERS.fan, draftSlots);
  assert.equal(blocked.status, 400);
  assert.match(blocked.body.message, /costs 50\.5, over the budget of 50\. Prices have gone up/);
  assert.equal((await save(api, USERS.fan, draftSlots)).status, 400);
  assert.equal((await all(db, "SELECT locked_at FROM fantasy_teams WHERE user_id = 1"))[0].locked_at, null);

  // s5 (now 7) replaced by s6 (5): 48.5, within budget.
  const committed = await commit(api, USERS.fan, ["s1", "s3", "s4", "s6", "s7"]);
  assert.equal(committed.status, 200);
  assert.equal(committed.body.team.state, "committed");
  assert.equal(committed.body.team.cost, 48.5);
});

test("a locked main team keeps the prices it was picked at", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await save(api, USERS.fan, CHEAP_FIVE); // s3 at 10
  api.clock.now = LATE; // s3 now costs 12
  const view = (await state(api, USERS.fan)).body.viewer.team;
  assert.equal(view.state, "locked");
  assert.equal(view.cost, 32.5);
  assert.equal(view.players.find((player) => player.slot_id === "s3").price, 10);
});

test("league admins and site admins get every team with its players", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await save(api, USERS.player, CHEAP_FIVE, "Jan's Five");
  await save(api, USERS.fan, ["s1"], "Fan Draft");
  api.clock.now = LATE;
  await commit(api, USERS.other, ["s1", "s3", "s4", "s5", "s7"], "Late One");

  assert.equal((await teams(api, USERS.stranger)).status, 403);
  const result = await teams(api, USERS.leagueAdmin);
  assert.equal(result.status, 200);
  assert.deepEqual(result.body.counts, { total: 3, complete: 2, drafts: 1, late: 1, committed: 1 });
  const jan = result.body.teams.find((team) => team.team_name === "Jan's Five");
  assert.equal(jan.owner, "Jan");
  assert.equal(jan.state, "locked");
  assert.equal(jan.locked_at, LEAGUE.main_deadline);
  assert.equal(jan.cost, 32.5);
  assert.deepEqual(jan.players[0], { slot_id: "s3", player: "Taras", stage: "Q2", record_id: "103", price_at_pick: 10, points: 0 });
  assert.equal(jan.points, 0);
  const fan = result.body.teams.find((team) => team.team_name === "Fan Draft");
  assert.equal(fan.owner, "Fan One");
  assert.equal(fan.state, "draft");
  assert.equal(fan.points, null);
  assert.equal(fan.players[0].points, null);
  assert.equal(result.body.teams.find((team) => team.team_name === "Late One").scoring_from, LATE.toISOString());
  assert.equal((await teams(api, USERS.admin)).status, 200);
});

test("managed leagues: site admins see all, league admins their own, others none", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const managed = (user) => api.call("GET /fantasy/leagues/managed", { user });
  const admin = await managed(USERS.admin);
  assert.equal(admin.body.can_manage_settings, true);
  assert.deepEqual(admin.body.leagues.map((league) => league.id), ["CCL-2027-Q"]);
  const matt = await managed(USERS.leagueAdmin);
  assert.equal(matt.body.can_manage_settings, false);
  assert.deepEqual(matt.body.leagues.map((league) => [league.id, league.status]), [["CCL-2027-Q", "open"]]);
  assert.deepEqual((await managed(USERS.fan)).body.leagues, []);
});

test("only site admins change league settings", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const patch = (user, body) => api.call("PATCH /fantasy/:leagueId", { user, body });
  assert.equal((await patch(USERS.leagueAdmin, { budget: 60 })).status, 403);
  assert.equal((await patch(USERS.admin, { budget: 60.2 })).status, 400);
  const result = await patch(USERS.admin, { budget: 55, late_deadline: "2026-12-06T23:59:59Z" });
  assert.equal(result.status, 200);
  assert.equal(result.body.league.budget, 55);
  assert.equal(result.body.league.late_deadline, "2026-12-06T23:59:59.000Z");
  assert.equal(api.audit.at(-1).event_type, "fantasy_league.updated");
  assert.deepEqual(api.audit.at(-1).changes.budget, { old: 50, new: 55 });
  // The new budget applies to saves at once: 20 + 18.5 + 8 + 6 = 52.5.
  assert.equal((await save(api, USERS.fan, ["s1", "s2", "s4", "s5"])).status, 201);
});

test("site admins add and remove league admins by the email of their site account", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const add = (user, email) => api.call("POST /fantasy/:leagueId/admins", { user, body: { email } });
  const list = (user) => api.call("GET /fantasy/:leagueId/admins", { user });
  const remove = (user, userId) => api.call("DELETE /fantasy/:leagueId/admins/:userId", { user, params: { userId: String(userId) } });

  assert.equal((await add(USERS.leagueAdmin, "fan@example.com")).status, 403);
  assert.equal((await list(USERS.leagueAdmin)).status, 403);
  assert.equal((await add(USERS.admin, "nobody@example.com")).status, 404);
  assert.equal((await add(USERS.admin, "twin@example.com")).status, 409);
  const added = await add(USERS.admin, " FAN@example.com ");
  assert.equal(added.status, 201);
  assert.equal(added.body.user_id, 1);
  assert.equal((await add(USERS.admin, "fan@example.com")).status, 200);
  assert.deepEqual((await list(USERS.admin)).body.admins.map((row) => [row.user_id, row.email]), [
    [11, "Matt@Example.com"], [1, "fan@example.com"],
  ]);
  // The new league admin sees the teams.
  assert.equal((await teams(api, USERS.fan)).status, 200);
  assert.equal((await remove(USERS.admin, 1)).status, 200);
  assert.equal((await remove(USERS.admin, 1)).status, 404);
  assert.equal((await teams(api, USERS.fan)).status, 403);
});

// ---- Phase 2: points, leaderboard, team details

test("match points: 2:0 → 3, 2:1 → 2, 1:2 → 1, 0:2 → 0", () => {
  assert.equal(matchPoints(2, 0), 3);
  assert.equal(matchPoints(2, 1), 2);
  assert.equal(matchPoints(1, 2), 1);
  assert.equal(matchPoints(0, 2), 0);
  // A technical win entered as 1:0 still gives the winner 3.
  assert.equal(matchPoints(1, 0), 3);
  assert.equal(matchPoints(0, 1), 0);
});

test("a result counts for a late team from the second of its Commit; for a main team always", () => {
  const commitAt = "2026-11-30T12:00:00.000Z";
  assert.equal(resultCounts({ time_utc: "2026-11-30T11:59:59Z" }, commitAt), false);
  assert.equal(resultCounts({ time_utc: "2026-11-30T12:00:00Z" }, commitAt), true);
  assert.equal(resultCounts({ time_utc: null }, commitAt), false);
  assert.equal(resultCounts({ time_utc: null }, null), true);
});

test("equal points share a place", () => {
  const ranked = rankTeams([
    { team_name: "B", points: 5 }, { team_name: "A", points: 5 }, { team_name: "C", points: 9 }, { team_name: "D", points: 1 },
  ]);
  assert.deepEqual(ranked.map((row) => [row.rank, row.team_name]), [[1, "C"], [2, "A"], [2, "B"], [4, "D"]]);
});

// p1 (s1) 3 · p2 (s2) 0 · p3 (s3) 2 · p4 (s4) 4 · p5 (s5) 2 · p6 (s6) 1 · p7 (s7) 3 over all counted matches.
const SCORED_MATCHES = `
  INSERT INTO duels (id, tournament_id, source_type, deleted_at, stage, group_name, round_name, player_1_id, player_2_id, status, dw1, dw2, time_utc, is_test) VALUES
    ('m1', 'CCL-2027', 'ccl', NULL, 'Q1', 'A', 'Round 1', 'p1', 'p2', 'Done', 2, 0, '2026-11-20T18:00:00.000Z', 0),
    ('m2', 'CCL-2027', 'ccl', NULL, 'Q2', 'B', 'Round 1', 'p3', 'p4', 'Done', 2, 1, '2026-11-21T18:00:00.000Z', 0),
    ('m4', 'CCL-2027', 'ccl', NULL, 'Q3', 'X', 'Round 1', 'p6', 'p5', 'Done', 1, 2, '2026-11-30T11:00:00.000Z', 0),
    ('m5', 'CCL-2027', 'ccl', NULL, 'Q2', 'B', 'Upper Final', 'p4', 'p3', 'done', 2, 0, '2026-11-30T12:00:00.000Z', 0),
    ('m3', 'CCL-2027', 'ccl', NULL, 'Q3', 'X', 'Upper Semifinals', 'p5', 'p7', 'Done', 0, 2, '2026-12-01T10:00:00.000Z', 0),
    -- None of these count: an Error, a planned match, a draw, a deleted match, a test match, the Main stage, another tournament.
    ('n1', 'CCL-2027', 'ccl', NULL, 'Q1', 'A', NULL, 'p1', 'p2', 'Error', 2, 0, '2026-11-22T18:00:00.000Z', 0),
    ('n2', 'CCL-2027', 'ccl', NULL, 'Q1', 'A', NULL, 'p1', 'p2', 'Planned', NULL, NULL, '2026-12-03T18:00:00.000Z', 0),
    ('n3', 'CCL-2027', 'ccl', NULL, 'Q1', 'A', NULL, 'p1', 'p2', 'Done', 1, 1, '2026-11-23T18:00:00.000Z', 0),
    ('n4', 'CCL-2027', 'ccl', '2026-11-24', 'Q1', 'A', NULL, 'p1', 'p2', 'Done', 2, 0, '2026-11-24T18:00:00.000Z', 0),
    ('n5', 'CCL-2027', 'ccl', NULL, 'Q1', 'A', NULL, 'p1', 'p2', 'Done', 2, 0, '2026-11-25T18:00:00.000Z', 1),
    ('n6', 'CCL-2027', 'ccl', NULL, 'Main', 'M', NULL, 'p9', 'p1', 'Done', 2, 0, '2026-11-26T18:00:00.000Z', 0),
    ('n7', 'CCL-2026', 'ccl', NULL, 'Q1', 'A', NULL, 'p1', 'p2', 'Done', 2, 0, '2026-11-26T18:00:00.000Z', 0);
`;

// Main teams: Jan's 14, the fan's 12 and the admin's 12 (same five); a late team that committed at LATE: 6;
// a draft that does not score.
async function scoredLeague(t) {
  const db = await createDatabase(t);
  await exec(db, SCORED_MATCHES);
  const api = createApi(db);
  await save(api, USERS.player, ["s1", "s3", "s4", "s5", "s7"], "Jan's Five");
  await save(api, USERS.fan, CHEAP_FIVE, "Meeple Masters");
  await save(api, USERS.admin, CHEAP_FIVE, "Admin Five");
  await save(api, USERS.other, ["s1"], "Just a Draft");
  api.clock.now = LATE;
  const late = await commit(api, USERS.stranger, ["s1", "s3", "s4", "s5", "s7"], "Late Five");
  assert.equal(late.status, 200);
  api.clock.now = new Date("2026-12-02T00:00:00Z");
  const ids = Object.fromEntries((await all(db, "SELECT id, team_name FROM fantasy_teams")).map((row) => [row.team_name, row.id]));
  return { db, api, ids };
}

const leaderboard = (api, user = null) => api.call("GET /public/fantasy/:leagueId/leaderboard", { user });
const teamDetails = (api, teamId) => api.call("GET /public/fantasy/:leagueId/teams/:teamId", { params: { teamId: String(teamId) } });

test("the leaderboard is hidden until the main deadline", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await save(api, USERS.fan, CHEAP_FIVE);
  const result = await leaderboard(api, USERS.fan);
  assert.equal(result.status, 200);
  assert.equal(result.body.available, false);
  assert.equal(result.body.opens_at, LEAGUE.main_deadline);
  assert.deepEqual(result.body.teams, []);
});

test("the leaderboard ranks locked teams by the points of their players' matches", async (t) => {
  const { api, ids } = await scoredLeague(t);
  const result = await leaderboard(api, USERS.fan);
  assert.equal(result.status, 200);
  assert.equal(result.body.available, true);
  assert.deepEqual(
    result.body.teams.map((row) => [row.rank, row.team_name, row.points, row.owner, row.entry_type]),
    [
      [1, "Jan's Five", 14, "Jan", "main"],
      [2, "Admin Five", 12, null, "main"],
      [2, "Meeple Masters", 12, null, "main"],
      // Only m5 (at the second of its Commit) and m3 count: p4 3 + p7 3.
      [4, "Late Five", 6, null, "late"],
    ]
  );
  assert.equal(result.body.teams.find((row) => row.team_name === "Late Five").committed_at, LATE.toISOString());
  assert.equal(result.body.my_team_id, ids["Meeple Masters"]);
  assert.equal(result.body.last_result_at, "2026-12-01T10:00:00.000Z");
  // No account names, emails or user ids in public.
  assert.ok(result.body.teams.every((row) => !("user_id" in row) && !("email" in row)));
  assert.equal((await leaderboard(api)).body.my_team_id, null);
  assert.equal((await leaderboard(api, USERS.other)).body.my_team_id, null);
});

test("team details list each player's matches and mark the ones a late team does not score", async (t) => {
  const { api, ids } = await scoredLeague(t);
  const result = await teamDetails(api, ids["Late Five"]);
  assert.equal(result.status, 200);
  const team = result.body.team;
  assert.equal(team.points, 6);
  assert.equal(team.spent, 49.5); // s3 bought at 12 in the late window
  assert.equal(team.committed_at, LATE.toISOString());
  const marek = team.players.find((player) => player.slot_id === "s5");
  assert.equal(marek.nickname, "Marek");
  assert.equal(marek.stage, "Q3");
  assert.equal(marek.group, "X");
  assert.deepEqual(marek.association, { id: "POL", name: "Poland", flag: "pl.png" });
  assert.equal(marek.status, "playing");
  assert.equal(marek.points, 0);
  assert.deepEqual(
    marek.matches.map((match) => [match.score, match.points, match.counted, match.opponent, match.round_name]),
    [["2-1", 2, false, "Ivan", "Round 1"], ["0-2", 0, true, "Zosia", "Upper Semifinals"]]
  );
  assert.equal(marek.player_id, "p5");
  // The duel as stored, for the popup: m4 is Ivan (p6) 1:2 Marek (p5).
  const m4 = marek.matches[0];
  assert.deepEqual([m4.duel_id, m4.player_1_id, m4.player_2_id, m4.dw1, m4.dw2, m4.status], ["m4", "p6", "p5", 1, 2, "Done"]);

  const main = (await teamDetails(api, ids["Meeple Masters"])).body.team;
  assert.equal(main.points, 12);
  assert.equal(main.committed_at, null);
  assert.equal(main.players.find((player) => player.slot_id === "s6").status, "eliminated");
  assert.ok(main.players.every((player) => player.matches.every((match) => match.counted)));
});

test("drafts, unknown teams and teams before the main deadline answer 404", async (t) => {
  const { db, api, ids } = await scoredLeague(t);
  assert.equal((await teamDetails(api, ids["Just a Draft"])).status, 404);
  assert.equal((await teamDetails(api, 99999)).status, 404);
  assert.equal((await teamDetails(api, "abc")).status, 404);
  const early = createApi(db, { now: OPEN });
  assert.equal((await teamDetails(early, ids["Meeple Masters"])).status, 404);
});

test("players carry their points and how many teams picked them", async (t) => {
  const { db, api } = await scoredLeague(t);
  const players = (await state(api)).body.players;
  const bySlot = Object.fromEntries(players.map((player) => [player.slot_id, player]));
  assert.deepEqual(
    ["s1", "s2", "s3", "s4", "s5", "s6", "s7"].map((slot) => [slot, bySlot[slot].points, bySlot[slot].picked_by]),
    [["s1", 3, 2], ["s2", 0, 0], ["s3", 2, 4], ["s4", 4, 4], ["s5", 2, 4], ["s6", 1, 2], ["s7", 3, 4]]
  );
  assert.equal((await state(api)).body.teams_formed, 4);
  const earlyState = (await state(createApi(db, { now: OPEN }))).body;
  // Complete saved teams: Jan's Five, Meeple Masters, Admin Five, Late Five (Just a Draft has one player).
  assert.equal(earlyState.teams_formed, 4);
  const early = earlyState.players;
  // Before the main deadline every saved team counts, drafts included (s1: Jan's Five, Just a Draft, Late Five).
  const earlyBySlot = Object.fromEntries(early.map((player) => [player.slot_id, player.picked_by]));
  assert.deepEqual(
    ["s1", "s2", "s3", "s4", "s5", "s6", "s7"].map((slot) => earlyBySlot[slot]),
    [3, 0, 4, 4, 4, 2, 4]
  );
  assert.equal(early.find((player) => player.slot_id === "s4").points, 4);
});

test("each player carries their next match with both players for the popup", async (t) => {
  const { db, api } = await scoredLeague(t);
  await exec(db, `
    UPDATE profiles SET name = 'Jan Kowalski', avatar = 'https://x.boardgamearena.net/a.jpg', bga_elo = 410, gg_elo = 1720, gg_rating_position = 12
    WHERE id = 'p1';
    UPDATE duels SET duel_format = 'Bo3', round_name = 'Round 2' WHERE id = 'n2';
    INSERT INTO duels (id, tournament_id, source_type, stage, group_name, player_1_id, player_2_id, status, time_utc) VALUES
      ('x1', 'CCL-2027', 'ccl', 'Q2', 'B', 'p3', 'p4', 'Draft', '2026-12-03T10:00:00.000Z'),
      ('x2', 'CCL-2027', 'ccl', 'Q2', 'B', 'p3', NULL, 'Planned', '2026-12-03T11:00:00.000Z');
  `);
  const players = (await state(api)).body.players;
  const bySlot = Object.fromEntries(players.map((player) => [player.slot_id, player]));
  // s1: the timed planned match n2 comes before d1, which has no time yet.
  const next = bySlot.s1.next_match;
  assert.equal(next.duel_id, "n2");
  assert.deepEqual([next.player_1_id, next.player_2_id], ["p1", "p2"]);
  assert.equal(next.time_utc, "2026-12-03T18:00:00.000Z");
  assert.equal(next.format, "Bo3");
  assert.equal(next.status, "Planned");
  assert.deepEqual([next.stage, next.group, next.round_name], ["Q1", "A", "Round 2"]);
  assert.deepEqual(next.player, {
    id: "p1", nickname: "Jan", name: "Jan Kowalski", avatar: "https://x.boardgamearena.net/a.jpg",
    flag: "pl.png", country: "Poland", elo: 1720, rank: 12, bgaElo: 410,
  });
  assert.equal(next.opponent.nickname, "Kasia");
  // From the other side too, with the opponent on the right.
  assert.equal(bySlot.s2.next_match.duel_id, "n2");
  assert.equal(bySlot.s2.next_match.opponent.nickname, "Jan");
  // s5: d2 has no time but is still the next match.
  assert.equal(bySlot.s5.next_match.duel_id, "d2");
  assert.equal(bySlot.s5.next_match.opponent.nickname, "Ivan");
  // s3: only a draft match and one without an opponent: no next match.
  assert.equal(bySlot.s3.next_match, null);
});

test("league admins see the points of every scoring team and player", async (t) => {
  const { api } = await scoredLeague(t);
  const result = await teams(api, USERS.leagueAdmin);
  const byName = Object.fromEntries(result.body.teams.map((team) => [team.team_name, team]));
  assert.equal(byName["Jan's Five"].points, 14);
  assert.equal(byName["Late Five"].points, 6);
  assert.equal(byName["Just a Draft"].points, null);
  assert.equal(byName["Late Five"].players.find((player) => player.slot_id === "s7").points, 3);
});

test("CSV parsing handles quotes, empty lines and a BOM", () => {
  const rows = parseCsv("﻿Record ID,country,player,qualifyingGroup,stage,Price\r\n1,Chile,\"Doe, \"\"JD\"\"\",A,Q1,10\r\n\r\n2,Poland,Kasia,\"\",Q1,4.5\n");
  assert.deepEqual(rows.map((row) => [row["Record ID"], row.player, row.qualifyingGroup, row.Price]), [
    ["1", "Doe, \"JD\"", "A", "10"],
    ["2", "Kasia", "", "4.5"],
  ]);
  assert.throws(() => parseCsv("player,stage\nx,Q1"), /no column Record ID, Price/);
});

const CSV = [
  "Record ID,country,currentPoints,elo,player,qualifyingGroup,seeded,stage,Price",
  "1,Poland,,1600,jan,A,1,Q1,20",
  "2,Poland,,1500,Kasia,B,0,Q1,18.5",
  "3,Ukraine,,1500,Taras,,1,Q2,10",
  "4,Ukraine,,1500,Olena,,0,Q2,8",
  "5,Poland,,1500,Marek,X,0,Q3,6",
  "6,Ukraine,,1500,Ivan,X,0,Q3,5",
  "7,Ukraine,,1500,Zosia,,0,Q3,3.5",
  "9,Poland,,1700,Main Star,,,Main,10",
  "10,Argentina,,1600,Argentina main draw contestant 1,,,Main,10",
].join("\n");

async function createImportDatabase(t) {
  const db = await createDatabase(t);
  await exec(db, "DELETE FROM fantasy_prices");
  return db;
}

test("the price plan matches players by nickname and stage and reports problems", async (t) => {
  const db = await createImportDatabase(t);
  const plan = await buildFantasyPricePlan(db, {
    rows: readPriceRows(parseCsv(CSV)), leagueId: LEAGUE_ID, effectiveFrom: "2026-11-24T00:00:00Z", source: "test.csv",
  });
  assert.equal(plan.summary.ready, true);
  assert.equal(plan.effective_from, "2026-11-24T00:00:00.000Z");
  assert.deepEqual(plan.summary.skipped_by_stage, { Main: 2 });
  assert.deepEqual(plan.prices.map((row) => [row.slot_id, row.price, row.source_record_id]), [
    ["s1", 20, "1"], ["s2", 18.5, "2"], ["s3", 10, "3"], ["s4", 8, "4"], ["s5", 6, "5"], ["s6", 5, "6"], ["s7", 3.5, "7"],
  ]);
  // Kasia plays in group A on the site; Zosia's country differs.
  assert.deepEqual(plan.warnings.map((warning) => [warning.code, warning.player]), [
    ["GROUP_MISMATCH", "Kasia"], ["COUNTRY_MISMATCH", "Zosia"],
  ]);

  const broken = CSV
    .replace("1,Poland,,1600,jan,A,1,Q1,20", "1,Poland,,1600,Janek,A,1,Q1,20")
    .replace("3,Ukraine,,1500,Taras,,1,Q2,10", "3,Ukraine,,1500,Taras,,1,Q2,10.3")
    .concat("\n11,Poland,,1500,Kasia,B,0,Q1,4");
  const bad = await buildFantasyPricePlan(db, {
    rows: readPriceRows(parseCsv(broken)), leagueId: LEAGUE_ID, effectiveFrom: "2026-11-24T00:00:00Z",
  });
  assert.equal(bad.summary.ready, false);
  assert.deepEqual(bad.issues.map((issue) => [issue.code, issue.player]), [
    ["UNMATCHED_PLAYER", "Janek"],
    ["INVALID_PRICE", "Taras"],
    ["DUPLICATE_PLAYER", "Kasia"],
    ["MISSING_PRICE", "Jan"],
    ["MISSING_PRICE", "Taras"],
  ]);
  await assert.rejects(
    buildFantasyPricePlan(db, { rows: [], leagueId: "NOPE", effectiveFrom: "2026-11-24T00:00:00Z" }),
    { code: "NO_LEAGUE" }
  );
});

test("applying a plan backs up, writes the prices and refuses stale or edited plans", async (t) => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "fantasy-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const db = await createImportDatabase(t);
  const build = (csv, effectiveFrom) => buildFantasyPricePlan(db, {
    rows: readPriceRows(parseCsv(csv)), leagueId: LEAGUE_ID, effectiveFrom,
  });

  const plan = await build(CSV, "2026-11-24T00:00:00Z");
  await assert.rejects(applyFantasyPricePlan(db, { ...plan, effective_from: "2026-11-25T00:00:00.000Z" }, { backupDir: dir }), { code: "INVALID_PLAN" });
  const result = await applyFantasyPricePlan(db, plan, { backupDir: dir });
  assert.equal(result.applied, true);
  assert.equal(result.prices, 7);
  assert.ok(result.backup_path.startsWith(dir));
  assert.equal((await all(db, "SELECT COUNT(*) AS count FROM fantasy_prices"))[0].count, 7);
  // The same plan again is stale: the prices it adds are now in the database.
  await assert.rejects(applyFantasyPricePlan(db, plan, { backupDir: dir }), { code: "STALE_PLAN" });

  // A later import adds only the changed price, as a new row of the history.
  const update = await build(CSV.replace("Olena,,0,Q2,8", "Olena,,0,Q2,9"), "2026-12-01T00:00:00Z");
  assert.deepEqual(update.prices.map((row) => [row.slot_id, row.old_price, row.price]), [["s4", 8, 9]]);
  assert.equal(update.unchanged.length, 6);
  await applyFantasyPricePlan(db, update, { backupDir: dir });
  assert.deepEqual(
    await all(db, "SELECT price, effective_from FROM fantasy_prices WHERE slot_id = 's4' ORDER BY effective_from"),
    [{ price: 8, effective_from: "2026-11-24T00:00:00.000Z" }, { price: 9, effective_from: "2026-12-01T00:00:00.000Z" }]
  );
});
