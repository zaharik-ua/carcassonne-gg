import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import {
  ensureTournamentRegistrationsSchema,
  normalizeRegistrationInput,
  registerTournamentRegistrationRoutes,
} from "./tournament-registrations.js";

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

const USERS = {
  captainUa: { id: 4, team_captain: 1, association: "Ukraine", player_id: "olena" },
  captainDe: { id: 6, team_captain: 1, association: "DE", player_id: "max" },
  playerUa: { id: 7, team_captain: 0, association: "UA", player_id: "taras" },
};

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => close(db));
  await exec(db, `
    PRAGMA foreign_keys = ON;
    CREATE TABLE tournaments (
      id TEXT PRIMARY KEY COLLATE NOCASE,
      name TEXT NOT NULL,
      type TEXT,
      category TEXT
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
      team_captain INTEGER NOT NULL DEFAULT 0,
      deleted_at TEXT
    );
    INSERT INTO tournaments VALUES
      ('CCL-2027', 'Carcassonne Champions League 2027', 'Individuals', 'CCL'),
      ('ETCOC-2026', 'ETCOC 2026', 'Teams', 'ETCOC');
    INSERT INTO associations (code, name, flag) VALUES
      ('UA', 'Ukraine', 'ua.png'), ('DE', 'Germany', 'de.png'), ('AR', 'Argentina', 'ar.png');
    INSERT INTO profiles (id, bga_nickname, name, association, team_captain, deleted_at) VALUES
      ('olena', 'Olena', 'Olena K', 'Ukraine', 1, NULL),
      ('ivan', 'Ivan', NULL, 'UA', 1, NULL),
      ('taras', 'taras', NULL, 'UA', 0, NULL),
      ('max', 'Max', NULL, 'DE', 1, NULL),
      ('gone', 'Gone', NULL, 'AR', 1, '2026-01-01');
  `);
  await ensureTournamentRegistrationsSchema(db);
  await ensureTournamentRegistrationsSchema(db);
  return db;
}

function createApi(db) {
  const routes = new Map();
  const app = {};
  for (const method of ["get", "post"]) {
    app[method] = (routePath, ...handlers) => routes.set(`${method.toUpperCase()} ${routePath}`, handlers.at(-1));
  }
  const audit = [];
  registerTournamentRegistrationRoutes(app, {
    db,
    getAuditActor: (user) => ({ actor_user_id: user?.id ?? null }),
    logAuditEvent: (entry, done) => {
      audit.push(entry);
      done();
    },
    logger: { error() {} },
  });
  const call = async (route, { user = null, params = { tournamentId: "CCL-2027" }, body = {} } = {}) => {
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

const register = (api, body, user = null) => api.call("POST /public/ccl/:tournamentId/registrations", { user, body });
const PLAYER = { bga_nickname: "  Leo   AR ", real_name: "Leo Messi", contact: "Telegram @leo" };

test("registration options list associations with their captains and the viewer's captaincy", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const anonymous = await api.call("GET /public/ccl/:tournamentId/registration");
  assert.equal(anonymous.status, 200);
  assert.deepEqual(anonymous.body.viewer, { authenticated: false, player_id: null, captain_association_id: null });
  assert.deepEqual(
    anonymous.body.associations.map((row) => [row.id, row.captains.map((captain) => captain.player_id)]),
    [["AR", []], ["DE", ["max"]], ["UA", ["ivan", "olena"]]]
  );

  const captain = await api.call("GET /public/ccl/:tournamentId/registration", { user: USERS.captainUa });
  assert.equal(captain.body.viewer.captain_association_id, "UA");
  const player = await api.call("GET /public/ccl/:tournamentId/registration", { user: USERS.playerUa });
  assert.equal(player.body.viewer.captain_association_id, null);
  assert.equal(player.body.viewer.player_id, "taras");

  const notCcl = await api.call("GET /public/ccl/:tournamentId/registration", { params: { tournamentId: "ETCOC-2026" } });
  assert.equal(notCcl.status, 404);
});

test("players of an association without captains or of another country register themselves", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const ar = await register(api, { ...PLAYER, association_id: "ar" }, USERS.playerUa);
  assert.equal(ar.status, 201, JSON.stringify(ar.body));
  assert.equal(ar.body.registration.association_id, "AR");
  assert.equal(ar.body.registration.bga_nickname, "Leo AR");

  const other = await register(api, { ...PLAYER, bga_nickname: "kiwi", country_name: " New  Zealand " });
  assert.equal(other.status, 201, JSON.stringify(other.body));
  // A typed country that is a known association is stored as that association.
  const typed = await register(api, { ...PLAYER, bga_nickname: "gaucho", country_name: "argentina" });
  assert.equal(typed.status, 201, JSON.stringify(typed.body));

  assert.deepEqual(
    await all(db, `
      SELECT tournament_id, association_id, country_name, bga_nickname, user_id, player_id, status
      FROM tournament_registrations ORDER BY id
    `),
    [
      { tournament_id: "CCL-2027", association_id: "AR", country_name: null, bga_nickname: "Leo AR", user_id: 7, player_id: "taras", status: "pending" },
      { tournament_id: "CCL-2027", association_id: null, country_name: "New Zealand", bga_nickname: "kiwi", user_id: null, player_id: null, status: "pending" },
      { tournament_id: "CCL-2027", association_id: "AR", country_name: null, bga_nickname: "gaucho", user_id: null, player_id: null, status: "pending" },
    ]
  );
  assert.equal(api.audit.length, 3);
  assert.equal(api.audit[0].event_type, "tournament_registration.created");
  assert.equal(api.audit[0].actor_user_id, 7);
});

test("self-registration is refused for associations with captains, duplicates and bad input", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const ua = await register(api, { ...PLAYER, association_id: "UA" });
  assert.equal(ua.status, 409);
  assert.deepEqual(ua.body.captains.map((captain) => captain.player_id), ["ivan", "olena"]);
  const typedUa = await register(api, { ...PLAYER, country_name: "Ukraine" });
  assert.equal(typedUa.status, 409);
  assert.equal(typedUa.body.association_id, "UA");

  assert.equal((await register(api, { ...PLAYER, association_id: "AR" })).status, 201);
  const duplicate = await register(api, { ...PLAYER, bga_nickname: "leo ar", country_name: "Peru" });
  assert.equal(duplicate.status, 409);
  assert.match(duplicate.body.message, /already registered/);

  for (const body of [
    { ...PLAYER, association_id: "XX" },
    { ...PLAYER },
    { ...PLAYER, association_id: "AR", bga_nickname: " " },
    { ...PLAYER, association_id: "AR", contact: "" },
    { ...PLAYER, country_name: "x".repeat(81) },
  ]) {
    const response = await register(api, body);
    assert.equal(response.status, 400, JSON.stringify(body));
  }
  const notCcl = await api.call("POST /public/ccl/:tournamentId/registrations", {
    params: { tournamentId: "ETCOC-2026" },
    body: { ...PLAYER, country_name: "Peru" },
  });
  assert.equal(notCcl.status, 404);
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM tournament_registrations")).at(0).n, 1);
});

test("normalizeRegistrationInput trims fields and requires a country", () => {
  assert.deepEqual(normalizeRegistrationInput({ ...PLAYER, country_name: " Peru " }).value, {
    bga_nickname: "Leo AR",
    real_name: "Leo Messi",
    contact: "Telegram @leo",
    country_name: "Peru",
    association_id: "",
  });
  assert.equal(normalizeRegistrationInput(PLAYER).error, "Country is required");
});
