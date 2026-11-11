import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import { registerCclMatchRoutes } from "./ccl-match-routes.js";
import { ensureCclSchema } from "./ccl-schema.js";

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
  });
}

function close(db) {
  return new Promise((resolve) => db.close(resolve));
}

const NOW = "2026-11-11T12:00:00.000Z";
const USERS = {
  admin: { id: 1, admin: 1, email: "admin@example.com" },
  tournamentAdmin: { id: 2, admin: 0, email: "ta@example.com" },
  olena: { id: 3, admin: 0, player_id: "olena" },
  taras: { id: 4, admin: 0, player_id: "taras" },
  juan: { id: 5, admin: 0, player_id: "juan" },
  stranger: { id: 6, admin: 0, player_id: "max" },
};

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => close(db));
  await exec(db, `
    PRAGMA foreign_keys = ON;
    CREATE TABLE tournaments (
      id TEXT PRIMARY KEY COLLATE NOCASE,
      name TEXT NOT NULL,
      short_title TEXT,
      type TEXT,
      category TEXT,
      ranking INTEGER,
      is_test INTEGER
    );
    CREATE TABLE associations (
      association_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT UNIQUE COLLATE NOCASE,
      name TEXT NOT NULL,
      flag TEXT
    );
    CREATE TABLE profiles (
      id TEXT PRIMARY KEY,
      bga_nickname TEXT,
      name TEXT,
      avatar TEXT,
      association TEXT,
      gg_elo REAL,
      gg_rating_position INTEGER,
      deleted_at TEXT
    );
    CREATE TABLE teams (id TEXT PRIMARY KEY, name TEXT, timezone TEXT);
    CREATE TABLE tournament_teams (
      id TEXT PRIMARY KEY,
      tournament_id TEXT NOT NULL,
      team_id TEXT NOT NULL,
      UNIQUE (tournament_id, team_id)
    );
    CREATE TABLE tournament_access_users (
      tournament_entity_type TEXT,
      tournament_id TEXT,
      user_id INTEGER,
      role TEXT
    );
    CREATE TABLE duel_formats (format TEXT PRIMARY KEY);
    CREATE TABLE duels (
      id TEXT PRIMARY KEY,
      tournament_id TEXT,
      match_id TEXT,
      duel_number INTEGER,
      duel_format TEXT,
      time_utc TEXT,
      player_1_id TEXT,
      player_2_id TEXT,
      dw1 INTEGER,
      dw2 INTEGER,
      ranking INTEGER NOT NULL DEFAULT 1,
      status TEXT,
      source_type TEXT,
      is_test INTEGER NOT NULL DEFAULT 0,
      stage TEXT,
      group_name TEXT,
      round_name TEXT,
      round_order INTEGER,
      deadline_utc TEXT,
      proposed_time_utc TEXT,
      proposed_time_by_player_id TEXT,
      proposed_time_status TEXT,
      created_by TEXT,
      updated_by TEXT,
      deleted_by TEXT,
      deleted_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO tournaments VALUES
      ('CCL-2027', 'Carcassonne Champions League 2027', 'CCL 2027', 'Individuals', 'CCL', 1, 0),
      ('ETCOC-2026', 'ETCOC 2026', NULL, 'Teams', 'ETCOC', 1, 0);
    INSERT INTO associations (code, name, flag) VALUES ('AR', 'Argentina', 'ar.png'), ('UA', 'Ukraine', 'ua.png');
    INSERT INTO profiles (id, bga_nickname, association, gg_elo) VALUES
      ('juan', 'Juan123', 'AR', 1810), ('olena', 'Olena', 'UA', 1755),
      ('taras', 'taras', 'UA', NULL), ('max', 'Max', 'DE', 1600);
    INSERT INTO tournament_access_users VALUES ('tournament', 'CCL-2027', 2, 'admin');
    INSERT INTO duel_formats VALUES ('Bo3'), ('Bo5');
    INSERT INTO teams VALUES ('UA', 'Ukraine', 'Europe/Kyiv'), ('AR', 'Argentina', 'America/Argentina/Buenos_Aires');
  `);
  await ensureCclSchema(db);
  await exec(db, `
    INSERT INTO tournament_players (id, tournament_id, team_id, player_id, association_id, entry_stage) VALUES
      ('s1', 'CCL-2027', NULL, 'juan', 'AR', 'Main'),
      ('s2', 'CCL-2027', NULL, 'olena', 'UA', 'Q1'),
      ('s3', 'CCL-2027', NULL, 'taras', 'UA', 'Q1');
  `);
  return db;
}

function createApi(db) {
  const routes = new Map();
  const app = {};
  for (const method of ["get", "post", "patch", "delete"]) {
    app[method] = (routePath, ...handlers) => routes.set(`${method.toUpperCase()} ${routePath}`, handlers.at(-1));
  }
  const audit = [];
  registerCclMatchRoutes(app, {
    db,
    requireAuthenticated: () => {},
    getAuditActor: (user) => ({ actor_user_id: user?.id ?? null }),
    logAuditEvent: (entry, done) => {
      audit.push(entry);
      done();
    },
    logger: { error() {} },
    now: () => new Date(NOW),
  });
  const call = async (route, { user = null, params = {}, body = {} } = {}) => {
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
    await handler({ user, params, body, query: {} }, res);
    return result;
  };
  return { call, audit };
}

const Q1_MATCH = {
  stage: "Q1", group_name: "A", round_name: "Round 1", round_order: 1,
  player_1_id: "olena", player_2_id: "taras", duel_format: "bo3", deadline_utc: "2026-11-30T23:59:00Z",
};

async function createMatch(api, body = Q1_MATCH, user = USERS.tournamentAdmin) {
  const response = await api.call("POST /ccl/:tournamentId/matches", { user, params: { tournamentId: "ccl-2027" }, body });
  assert.equal(response.status, 201, JSON.stringify(response.body));
  return response.body.match;
}

const listMatches = (api, user) => api.call("GET /ccl/:tournamentId/matches", { user, params: { tournamentId: "CCL-2027" } });

test("tournament admins create matches between registered players", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const match = await createMatch(api);
  assert.equal(match.stage, "Q1");
  assert.equal(match.duel_format, "Bo3");
  assert.equal(match.status, "Planned");
  assert.equal(match.player_1.bga_nickname, "Olena");
  assert.equal(match.player_1.association_name, "Ukraine");
  assert.equal(match.player_1.timezone, "Europe/Kyiv");
  assert.equal(match.stage, "Q1");
  const duel = await get(db, "SELECT * FROM duels WHERE id = ?", [match.id]);
  assert.equal(duel.source_type, "ccl");
  assert.equal(duel.match_id, null);
  assert.equal(duel.tournament_id, "CCL-2027");
  assert.equal(api.audit.at(-1).event_type, "ccl.match.created");

  for (const [user, body, status] of [
    [USERS.olena, Q1_MATCH, 403],
    [USERS.admin, { ...Q1_MATCH, player_2_id: "juan" }, 400],
    [USERS.admin, { ...Q1_MATCH, duel_format: "Bo7" }, 400],
    [USERS.admin, { ...Q1_MATCH, stage: "Final" }, 400],
  ]) {
    const response = await api.call("POST /ccl/:tournamentId/matches", { user, params: { tournamentId: "CCL-2027" }, body });
    assert.equal(response.status, status, JSON.stringify(response.body));
  }
  const notCcl = await api.call("POST /ccl/:tournamentId/matches", {
    user: USERS.admin, params: { tournamentId: "ETCOC-2026" }, body: Q1_MATCH,
  });
  assert.equal(notCcl.status, 404);
});

test("players see only their own matches; admins see all with eligible players", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await createMatch(api);

  const admin = await listMatches(api, USERS.tournamentAdmin);
  assert.equal(admin.body.can_manage, true);
  assert.equal(admin.body.matches.length, 1);
  assert.deepEqual(admin.body.players.find((group) => group.stage === "Q1").players.map((player) => player.id), ["olena", "taras"]);
  assert.deepEqual(admin.body.duel_formats, ["Bo3", "Bo5"]);

  const olena = await listMatches(api, USERS.olena);
  assert.equal(olena.body.can_manage, false);
  assert.equal(olena.body.matches.length, 1);
  assert.equal(olena.body.matches[0].my_side, 1);
  assert.deepEqual(olena.body.players, []);

  const juan = await listMatches(api, USERS.juan);
  assert.equal(juan.status, 200);
  assert.equal(juan.body.matches.length, 0);
  assert.equal((await listMatches(api, USERS.stranger)).status, 403);
});

test("players agree on a time; the match gets it on acceptance", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const match = await createMatch(api);
  const propose = (user, time, snapshot = {}) => api.call("POST /ccl/matches/:id/time-proposal", {
    user, params: { id: match.id }, body: { proposed_time_utc: time, ...snapshot },
  });
  const accept = (user, snapshot = {}) => api.call("PATCH /ccl/matches/:id/time-proposal/accept", {
    user, params: { id: match.id }, body: snapshot,
  });

  assert.equal((await propose(USERS.juan, "2026-11-20T18:00:00Z")).status, 403);
  const first = await propose(USERS.olena, "2026-11-20T18:00:00Z");
  assert.equal(first.status, 200, JSON.stringify(first.body));
  assert.equal(first.body.match.proposed_time_status, "pending");
  assert.equal((await accept(USERS.olena)).status, 403);

  const stale = await accept(USERS.taras, {
    expected_proposed_time_utc: null, expected_proposed_time_by_player_id: null, expected_proposed_time_status: null,
  });
  assert.equal(stale.status, 409);

  const accepted = await accept(USERS.taras, {
    expected_proposed_time_utc: "2026-11-20T18:00:00.000Z",
    expected_proposed_time_by_player_id: "olena",
    expected_proposed_time_status: "pending",
  });
  assert.equal(accepted.status, 200, JSON.stringify(accepted.body));
  assert.equal(accepted.body.match.time_utc, "2026-11-20T18:00:00.000Z");
  assert.equal(accepted.body.match.proposed_time_status, "accepted");
  assert.equal((await get(db, "SELECT time_utc FROM duels WHERE id = ?", [match.id])).time_utc, "2026-11-20T18:00:00.000Z");
  assert.equal(api.audit.at(-1).event_type, "duel.time_proposal.accepted");

  const late = await propose(USERS.taras, "2026-12-05T18:00:00Z");
  assert.match(late.body.message, /deadline/);
});

test("admins edit and delete matches; a direct time clears the proposal", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const match = await createMatch(api);
  await api.call("POST /ccl/matches/:id/time-proposal", {
    user: USERS.olena, params: { id: match.id }, body: { proposed_time_utc: "2026-11-20T18:00:00Z" },
  });

  const edited = await api.call("PATCH /ccl/matches/:id", {
    user: USERS.admin, params: { id: match.id }, body: { time_utc: "2026-11-25T19:00:00Z", group_name: "B" },
  });
  assert.equal(edited.status, 200, JSON.stringify(edited.body));
  assert.equal(edited.body.match.time_utc, "2026-11-25T19:00:00.000Z");
  assert.equal(edited.body.match.group_name, "B");
  assert.equal(edited.body.match.proposed_time_utc, null);
  assert.equal(edited.body.match.proposed_time_status, "accepted");

  const badPlayers = await api.call("PATCH /ccl/matches/:id", {
    user: USERS.admin, params: { id: match.id }, body: { stage: "Main" },
  });
  assert.equal(badPlayers.status, 400);
  const byPlayer = await api.call("PATCH /ccl/matches/:id", {
    user: USERS.olena, params: { id: match.id }, body: { group_name: "C" },
  });
  assert.equal(byPlayer.status, 403);

  const removed = await api.call("DELETE /ccl/matches/:id", { user: USERS.tournamentAdmin, params: { id: match.id } });
  assert.equal(removed.status, 200);
  assert.ok((await get(db, "SELECT deleted_at FROM duels WHERE id = ?", [match.id])).deleted_at);
  assert.equal((await listMatches(api, USERS.admin)).body.matches.length, 0);
});
