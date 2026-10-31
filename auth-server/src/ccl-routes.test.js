import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import { registerCclRoutes } from "./ccl-routes.js";
import { ensureCclSchema } from "./ccl-schema.js";

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

const NOW = "2026-11-05T12:00:00.000Z";
const USERS = {
  admin: { id: 1, admin: 1, email: "admin@example.com" },
  tournamentAdmin: { id: 2, admin: 0, email: "ta@example.com" },
  captainAr: { id: 3, admin: 0, team_captain: 1, association: "Argentina", player_id: "juan" },
  captainUa: { id: 4, admin: 0, team_captain: 1, association: "UA", player_id: "olena" },
  playerAr: { id: 5, admin: 0, team_captain: 0, association: "AR", player_id: "leo" },
  captainDe: { id: 6, admin: 0, team_captain: 1, association: "DE", player_id: "max" },
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
      category TEXT,
      ccl_main_registration_ends_at TEXT,
      ccl_q12_registration_ends_at TEXT,
      ccl_q3_registration_ends_at TEXT
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
    CREATE TABLE teams (id TEXT PRIMARY KEY);
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
    INSERT INTO tournaments VALUES
      ('CCL-2027', 'Carcassonne Champions League 2027', 'Individuals', 'CCL',
       '2026-12-30T23:59:00Z', '2026-11-11T23:59:00Z', '2026-11-01T23:59:00Z'),
      ('ETCOC-2026', 'ETCOC 2026', 'Teams', 'ETCOC', NULL, NULL, NULL);
    INSERT INTO associations (code, name, flag) VALUES
      ('AR', 'Argentina', 'ar.png'), ('UA', 'Ukraine', 'ua.png'), ('DE', 'Germany', 'de.png');
    INSERT INTO profiles (id, bga_nickname, association, gg_elo, gg_rating_position) VALUES
      ('juan', 'Juan123', 'AR', 1810, 3),
      ('pablo', 'pablo_ar', 'Argentina', 1500, NULL),
      ('leo', 'Leo_ar', 'AR', 1700.5, 9),
      ('olena', 'Olena', 'UA', 1755, 5),
      ('taras', 'taras', 'UA', NULL, NULL),
      ('max', 'Max', 'DE', 1600, 20);
    INSERT INTO tournament_access_users VALUES ('tournament', 'CCL-2027', 2, 'admin');
  `);
  await ensureCclSchema(db);
  return db;
}

function createApi(db) {
  const routes = new Map();
  const app = {};
  for (const method of ["get", "post", "patch", "delete"]) {
    app[method] = (routePath, ...handlers) => routes.set(`${method.toUpperCase()} ${routePath}`, handlers.at(-1));
  }
  const audit = [];
  registerCclRoutes(app, {
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

async function createSlot(api, body, user = USERS.admin) {
  const response = await api.call("POST /ccl/:tournamentId/slots", {
    user,
    params: { tournamentId: "ccl-2027" },
    body,
  });
  assert.equal(response.status, 201, JSON.stringify(response.body));
  return response.body.slots;
}

const patchSlot = (api, user, id, body) => api.call("PATCH /ccl/slots/:id", { user, params: { id }, body });
const setResult = (api, id, body) => api.call("PATCH /ccl/slots/:id/result", { user: USERS.admin, params: { id }, body });
const setElo = (api, phase, user = USERS.admin) => api.call("POST /ccl/:tournamentId/elo/:phase", {
  user,
  params: { tournamentId: "CCL-2027", phase },
});
const formPots = (api, phase, user = USERS.admin) => api.call("POST /ccl/:tournamentId/pots/:phase", {
  user,
  params: { tournamentId: "CCL-2027", phase },
});
const getSlots = (api, user) => api.call("GET /ccl/:tournamentId/slots", { user, params: { tournamentId: "CCL-2027" } });

test("admins create slots with or without an association; others cannot", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const [main] = await createSlot(api, {
    association_id: "ar",
    entry_stage: "Main",
    qualified_by_name: "Association ranking",
  });
  assert.equal(main.association_id, "AR");
  assert.equal(main.association_name, "Argentina");
  assert.equal(main.tournament_id, "CCL-2027");
  assert.deepEqual(main.entries.map((entry) => [entry.stage, entry.qualified_by_name, entry.elo_fixed]), [
    ["Main", "Association ranking", false],
  ]);

  const fans = await createSlot(api, { entry_stage: "Q3", qualified_by_name: "Fans Choice", count: 2 }, USERS.tournamentAdmin);
  assert.equal(fans.length, 2);
  assert.ok(fans.every((slot) => slot.association_id === null));
  const [ua] = await createSlot(api, { association_id: "Ukraine", entry_stage: "q1", qualified_by_name: "ETCOC" });
  assert.equal(ua.association_id, "UA");
  assert.equal(ua.entry_stage, "Q1");

  const audit = api.audit.find((entry) => entry.event_type === "ccl_slots.created" && entry.metadata.slot_ids.length === 2);
  assert.equal(audit.actor_user_id, 2);

  const denied = await api.call("POST /ccl/:tournamentId/slots", {
    user: USERS.captainAr,
    params: { tournamentId: "CCL-2027" },
    body: { association_id: "AR", entry_stage: "Q1", qualified_by_name: "X" },
  });
  assert.equal(denied.status, 403);
  for (const [params, body, status] of [
    [{ tournamentId: "CCL-2027" }, { association_id: "XX", entry_stage: "Q1", qualified_by_name: "X" }, 400],
    [{ tournamentId: "CCL-2027" }, { entry_stage: "Q4", qualified_by_name: "X" }, 400],
    [{ tournamentId: "ETCOC-2026" }, { entry_stage: "Q1", qualified_by_name: "X" }, 404],
  ]) {
    const response = await api.call("POST /ccl/:tournamentId/slots", { user: USERS.admin, params, body });
    assert.equal(response.status, status, JSON.stringify(response.body));
  }
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM tournament_players")).at(0).n, 4);
});

test("captains see and fill only their association's slots before the stage deadline", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const [arQ1] = await createSlot(api, { association_id: "AR", entry_stage: "Q1", qualified_by_name: "Association ranking" });
  const [arQ3] = await createSlot(api, { association_id: "AR", entry_stage: "Q3", qualified_by_name: "Copa America Winner" });
  const [uaQ1] = await createSlot(api, { association_id: "UA", entry_stage: "Q1", qualified_by_name: "ETCOC" });

  const view = await getSlots(api, USERS.captainAr);
  assert.equal(view.status, 200);
  assert.equal(view.body.can_manage, false);
  assert.equal(view.body.captain_association_id, "AR");
  assert.deepEqual(view.body.slots.map((slot) => slot.id).sort(), [arQ1.id, arQ3.id].sort());
  assert.equal(view.body.phases.qualification.pots_blockers, undefined);
  assert.deepEqual(
    view.body.tournament.stages.map((stage) => [stage.stage, stage.registration_open]),
    [["Main", true], ["Q1", true], ["Q2", true], ["Q3", false]]
  );
  assert.equal((await getSlots(api, USERS.playerAr)).status, 403);
  assert.equal((await getSlots(api, USERS.captainDe)).status, 403);
  assert.ok((await getSlots(api, USERS.admin)).body.phases.qualification.pots_blockers.length > 0);

  // Profile association may be stored as a name ("Argentina").
  const filled = await patchSlot(api, USERS.captainAr, arQ1.id, {
    player_id: "pablo",
    qualified_by_name: "Argentina Online 2026",
    qualified_by_link: "https://carcassonne.gg/ARCOC-2026/",
  });
  assert.equal(filled.status, 200, JSON.stringify(filled.body));
  assert.equal(filled.body.slot.player.bga_nickname, "pablo_ar");
  assert.equal(filled.body.slot.entries[0].qualified_by_name, "Argentina Online 2026");
  assert.equal(filled.body.slot.entries[0].qualified_by_link, "https://carcassonne.gg/ARCOC-2026/");

  for (const [user, id, body, status, message] of [
    [USERS.captainAr, uaQ1.id, { player_id: "juan" }, 403, /Forbidden/],
    [USERS.captainAr, arQ3.id, { player_id: "juan" }, 400, /closed/],
    [USERS.captainAr, arQ1.id, { player_id: "olena" }, 400, /does not belong/],
    [USERS.captainAr, arQ1.id, { entry_stage: "Q2" }, 400, /cannot change entry_stage/],
    [USERS.captainAr, arQ1.id, { player_id: "ghost" }, 400, /Player not found/],
    [USERS.playerAr, arQ1.id, { player_id: "leo" }, 403, /Forbidden/],
  ]) {
    const response = await patchSlot(api, user, id, body);
    assert.equal(response.status, status, JSON.stringify(response.body));
    assert.match(response.body.message, message);
  }

  // Admin may fill a closed stage, but a player holds one slot per tournament.
  assert.equal((await patchSlot(api, USERS.admin, arQ3.id, { player_id: "juan" })).status, 200);
  const duplicate = await patchSlot(api, USERS.admin, arQ3.id, { player_id: "pablo" });
  assert.equal(duplicate.status, 400);
  assert.match(duplicate.body.message, /already has a slot/);

  const updated = api.audit.find((entry) => entry.event_type === "ccl_slot.updated" && entry.actor_user_id === 3);
  assert.deepEqual(updated.changes.player_id, { old: null, new: "pablo" });
  assert.deepEqual(updated.changes.qualified_by_name, { old: "Association ranking", new: "Argentina Online 2026" });
});

test("admins assign associations, move stages and delete slots with their stage entries", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const [fans] = await createSlot(api, { entry_stage: "Q3", qualified_by_name: "Fans Choice" });

  const noAssociation = await patchSlot(api, USERS.admin, fans.id, { player_id: "max" });
  assert.match(noAssociation.body.message, /without an association/);
  const assigned = await patchSlot(api, USERS.admin, fans.id, { association_id: "Germany", player_id: "max" });
  assert.equal(assigned.status, 200, JSON.stringify(assigned.body));
  assert.equal(assigned.body.slot.association_id, "DE");
  assert.equal((await getSlots(api, USERS.captainDe)).body.slots.length, 1);

  const moved = await patchSlot(api, USERS.admin, fans.id, { entry_stage: "Q2" });
  assert.deepEqual(moved.body.slot.entries.map((entry) => [entry.stage, entry.qualified_by_name]), [["Q2", "Fans Choice"]]);

  const deleted = await api.call("DELETE /ccl/slots/:id", { user: USERS.admin, params: { id: fans.id } });
  assert.equal(deleted.status, 200);
  assert.deepEqual(await all(db, "SELECT * FROM ccl_stage_entries"), []);
  assert.deepEqual(await all(db, "SELECT * FROM tournament_players"), []);
  assert.equal((await api.call("DELETE /ccl/slots/:id", { user: USERS.admin, params: { id: fans.id } })).status, 404);
});

async function seedFullTournament(api) {
  const [arMain] = await createSlot(api, { association_id: "AR", entry_stage: "Main", qualified_by_name: "Association ranking" });
  const [arQ1] = await createSlot(api, { association_id: "AR", entry_stage: "Q1", qualified_by_name: "Association ranking" });
  const [uaQ1] = await createSlot(api, { association_id: "UA", entry_stage: "Q1", qualified_by_name: "ETCOC" });
  const [uaQ2] = await createSlot(api, { association_id: "UA", entry_stage: "Q2", qualified_by_name: "ETCOC" });
  await patchSlot(api, USERS.admin, arMain.id, { player_id: "juan" });
  await patchSlot(api, USERS.admin, arQ1.id, { player_id: "pablo" });
  await patchSlot(api, USERS.admin, uaQ1.id, { player_id: "olena" });
  await patchSlot(api, USERS.admin, uaQ2.id, { player_id: "taras" });
  return { arMain, arQ1, uaQ1, uaQ2 };
}

test("qualification Elo, results, league Elo and pots work end to end", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const [empty] = await createSlot(api, { association_id: "DE", entry_stage: "Q2", qualified_by_name: "X" });
  const slots = await seedFullTournament(api);

  // Elo can be issued early, while slots are still open; empty slots only block pots.
  const early = await setElo(api, "qualification");
  assert.equal(early.status, 200, JSON.stringify(early.body));
  assert.deepEqual(early.body.skipped, [{ slot_id: empty.id, reason: "player_missing" }]);
  assert.equal(early.body.phase.elo_set, false);
  const blockedPots = await formPots(api, "qualification");
  assert.equal(blockedPots.status, 409);
  assert.deepEqual(blockedPots.body.blockers, [{ slot_id: empty.id, reason: "player_missing" }]);
  assert.equal((await setElo(api, "qualification", USERS.captainAr)).status, 403);
  assert.equal((await setElo(api, "groups")).status, 400);
  await api.call("DELETE /ccl/slots/:id", { user: USERS.admin, params: { id: empty.id } });

  const qualification = await setElo(api, "qualification");
  assert.equal(qualification.status, 200, JSON.stringify(qualification.body));
  assert.equal(qualification.body.phase.elo_set, true);
  const entries = await all(db, "SELECT slot_id, stage, player_id, elo, elo_source, fixed_at, fixed_by FROM ccl_stage_entries WHERE stage <> 'Main' ORDER BY stage, player_id");
  assert.deepEqual(entries.map((row) => [row.stage, row.player_id, row.elo, row.elo_source]), [
    ["Q1", "olena", 1755, "gg_elo"],
    ["Q1", "pablo", null, "gg_elo"],
    ["Q2", "taras", null, "gg_elo"],
  ]);
  assert.ok(entries.every((row) => row.fixed_at === NOW && row.fixed_by === "admin@example.com"));

  // Issued Elo does not lock captains before the deadline; it follows the new player.
  const swapped = await patchSlot(api, USERS.captainAr, slots.arQ1.id, { player_id: "leo" });
  assert.equal(swapped.status, 200, JSON.stringify(swapped.body));
  assert.deepEqual(
    swapped.body.slot.entries.map((entry) => [entry.stage, entry.player_id, entry.elo, entry.elo_fixed, entry.fixed_by]),
    [["Q1", "leo", 1700.5, true, "juan"]]
  );
  assert.equal((await patchSlot(api, USERS.captainAr, slots.arQ1.id, { player_id: "pablo" })).status, 200);
  assert.equal((await patchSlot(api, USERS.captainAr, slots.arQ1.id, { qualified_by_name: "Online 2026" })).status, 200);

  // Qualification pot sizes are fixed per path: Q1 needs 20 rated players for Seeded.
  const smallPath = await formPots(api, "qualification");
  assert.equal(smallPath.status, 400);
  assert.match(smallPath.body.message, /Q1 needs 20 rated players for Seeded, but has 1/);

  // A manual Elo change resets the phase pots.
  const manual = await api.call("PATCH /ccl/slots/:id/stages/:stage/elo", {
    user: USERS.admin,
    params: { id: slots.arQ1.id, stage: "Q1" },
    body: { elo: "1690" },
  });
  assert.equal(manual.status, 200, JSON.stringify(manual.body));
  assert.deepEqual(
    manual.body.slot.entries.map((entry) => [entry.stage, entry.elo, entry.elo_source, entry.seed]),
    [["Q1", 1690, "manual", null]]
  );
  assert.deepEqual(await all(db, "SELECT seed FROM ccl_stage_entries WHERE seed IS NOT NULL"), []);

  // An empty value clears the Elo of the stage.
  const clearedElo = await api.call("PATCH /ccl/slots/:id/stages/:stage/elo", {
    user: USERS.admin,
    params: { id: slots.arQ1.id, stage: "Q1" },
    body: { elo: "" },
  });
  assert.equal(clearedElo.status, 200, JSON.stringify(clearedElo.body));
  assert.deepEqual(
    clearedElo.body.slot.entries.map((entry) => [entry.stage, entry.elo, entry.elo_source, entry.fixed_at, entry.elo_fixed]),
    [["Q1", null, null, null, false]]
  );
  assert.equal((await api.call("PATCH /ccl/slots/:id/stages/:stage/elo", {
    user: USERS.admin,
    params: { id: slots.arQ1.id, stage: "Q1" },
    body: { elo: "1690" },
  })).status, 200);
  assert.equal((await api.call("PATCH /ccl/slots/:id/stages/:stage/elo", {
    user: USERS.admin,
    params: { id: slots.arQ1.id, stage: "Main" },
    body: { elo: 1700 },
  })).status, 404);

  // League: every Q slot needs a result before pots can be formed.
  const pending = await formPots(api, "league");
  assert.equal(pending.status, 409);
  assert.equal(pending.body.blockers.filter((row) => row.reason === "league_status_pending").length, 3);
  assert.match((await setResult(api, slots.arMain.id, { league_status: "eliminated" })).body.message, /only to Q1/);
  assert.match((await setResult(api, slots.arQ1.id, { league_status: "qualified" })).body.message, /qualified_by_name/);
  const qualified = await setResult(api, slots.arQ1.id, {
    league_status: "qualified",
    league_qualified_by_name: "Q1, Group A Winner",
    league_qualified_by_link: "https://carcassonne.gg/CCL-2027/qualifications#A",
  });
  assert.equal(qualified.status, 200, JSON.stringify(qualified.body));
  assert.deepEqual(qualified.body.slot.entries.map((entry) => [entry.stage, entry.qualified_by_name]), [
    ["Main", "Q1, Group A Winner"],
    ["Q1", "Online 2026"],
  ]);
  await setResult(api, slots.uaQ1.id, { league_status: "qualified", league_qualified_by_name: "Q1, Group B Winner" });
  await setResult(api, slots.uaQ1.id, { league_status: "eliminated" });
  await setResult(api, slots.uaQ2.id, { league_status: "eliminated" });
  assert.deepEqual(
    (await all(db, "SELECT slot_id FROM ccl_stage_entries WHERE stage = 'Main' ORDER BY slot_id")).map((row) => row.slot_id),
    [slots.arMain.id, slots.arQ1.id].sort()
  );

  assert.equal((await setElo(api, "league")).status, 200);
  // Five pots of 12: Pot 1-4 need 48 rated players.
  const smallLeague = await formPots(api, "league");
  assert.equal(smallLeague.status, 400);
  assert.match(smallLeague.body.message, /needs 48 rated players for Pot 1-4, but has 1/);

  const publicLists = await api.call("GET /public/ccl/:tournamentId/players", { params: { tournamentId: "ccl-2027" } });
  assert.equal(publicLists.status, 200);
  assert.deepEqual(
    publicLists.body.league_players.map((row) => [row.player, row.stage, row.elo, row.pot, row.qualified_by_name]),
    [
      ["Juan123", "Main", 1810, null, "Association ranking"],
      ["pablo_ar", "Q1", null, null, "Q1, Group A Winner"],
    ]
  );
  assert.deepEqual(
    publicLists.body.all_players.map((row) => [row.country, row.stage, row.player, row.elo, row.seeding, row.league_status]),
    [
      ["Argentina", "Main", "Juan123", null, null, null],
      ["Argentina", "Q1", "pablo_ar", 1690, null, "qualified"],
      ["Ukraine", "Q1", "Olena", 1755, null, "eliminated"],
      ["Ukraine", "Q2", "taras", null, null, "eliminated"],
    ]
  );
  assert.deepEqual(publicLists.body.phases.league, { elo_set: true, elo_set_at: NOW, pots_formed: false });
  assert.equal(publicLists.body.phases.qualification.pots_formed, false);

  // Replacing a league player re-issues that slot's Elo.
  const replaced = await patchSlot(api, USERS.admin, slots.arMain.id, { player_id: "leo" });
  assert.equal(replaced.status, 200, JSON.stringify(replaced.body));
  assert.deepEqual(
    replaced.body.slot.entries.map((entry) => [entry.player_id, entry.elo, entry.elo_fixed, entry.seed]),
    [["leo", 1700.5, true, null]]
  );
  // Removing the player clears the slot's Elo, so pots are blocked.
  await patchSlot(api, USERS.admin, slots.arMain.id, { player_id: null });
  const cleared = await formPots(api, "league");
  assert.deepEqual(cleared.body.blockers, [{ slot_id: slots.arMain.id, reason: "player_missing" }]);
  assert.deepEqual(
    await all(db, "SELECT player_id, elo, fixed_at FROM ccl_stage_entries WHERE slot_id = ?", [slots.arMain.id]),
    [{ player_id: null, elo: null, fixed_at: null }]
  );

  const events = new Set(api.audit.map((entry) => entry.event_type));
  for (const event of ["ccl_elo.set", "ccl_elo.manual", "ccl_slot.result_updated", "ccl_slot.deleted"]) {
    assert.ok(events.has(event), `missing audit ${event}`);
  }
});

// Creates `count` slots of one stage and fills them with new AR players; eloFor(index) is their GG Elo.
async function fillStage(api, db, stage, count, eloFor) {
  const created = await createSlot(api, { association_id: "AR", entry_stage: stage, qualified_by_name: "Ranking", count });
  const playerIds = [];
  for (const [index, slot] of created.entries()) {
    const playerId = `${stage.toLowerCase()}-${String(index + 1).padStart(2, "0")}`;
    await new Promise((resolve, reject) => db.run(
      "INSERT INTO profiles (id, bga_nickname, association, gg_elo, gg_rating_position) VALUES (?, ?, 'AR', ?, ?)",
      [playerId, playerId, eloFor(index + 1), index + 1],
      (error) => (error ? reject(error) : resolve())
    ));
    assert.equal((await patchSlot(api, USERS.admin, slot.id, { player_id: playerId })).status, 200);
    playerIds.push(playerId);
  }
  return playerIds;
}

async function seedsByPlayer(db) {
  const rows = await all(db, `
    SELECT p.player_id, e.seed FROM ccl_stage_entries e
    JOIN tournament_players p ON p.id = e.slot_id
    ORDER BY p.player_id
  `);
  return new Map(rows.map((row) => [row.player_id, row.seed]));
}

test("a full Q3 path forms 12 Seeded and 12 Unseeded", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  // 14 rated players; the rest have the initial 1500 or no GG Elo.
  const players = await fillStage(api, db, "Q3", 24, (index) => (index <= 14 ? 2010 - index * 10 : index % 2 ? 1500 : null));
  assert.equal((await setElo(api, "qualification")).status, 200);
  const pots = await formPots(api, "qualification");
  assert.equal(pots.status, 200, JSON.stringify(pots.body));
  const seeds = await seedsByPlayer(db);
  assert.deepEqual(players.filter((id) => seeds.get(id) === "Seeded"), players.slice(0, 12));
  assert.deepEqual(players.filter((id) => seeds.get(id) === "Unseeded"), players.slice(12));
});

test("a full League Stage forms five pots of 12", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  // 54 rated players and 6 without rating.
  const players = await fillStage(api, db, "Main", 60, (index) => (index <= 54 ? 2200 - index : null));
  assert.equal((await setElo(api, "league")).status, 200);
  const pots = await formPots(api, "league");
  assert.equal(pots.status, 200, JSON.stringify(pots.body));
  const seeds = await seedsByPlayer(db);
  for (const [index, pot] of ["Pot 1", "Pot 2", "Pot 3", "Pot 4", "Pot 5"].entries()) {
    assert.deepEqual(players.filter((id) => seeds.get(id) === pot), players.slice(index * 12, index * 12 + 12), pot);
  }
  const publicLists = await api.call("GET /public/ccl/:tournamentId/players", { params: { tournamentId: "CCL-2027" } });
  assert.equal(publicLists.body.phases.league.pots_formed, true);
  assert.equal(publicLists.body.league_players.filter((row) => row.pot === "Pot 5" && row.elo === null).length, 6);
  assert.ok(api.audit.some((entry) => entry.event_type === "ccl_pots.formed" && entry.metadata.phase === "league"));
});

test("public players endpoint serves only CCL tournaments", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const response = await api.call("GET /public/ccl/:tournamentId/players", { params: { tournamentId: "ETCOC-2026" } });
  assert.equal(response.status, 404);
  const empty = await api.call("GET /public/ccl/:tournamentId/players", { params: { tournamentId: "CCL-2027" } });
  assert.deepEqual(
    { all: empty.body.all_players, league: empty.body.league_players, stages: empty.body.stages.length },
    { all: [], league: [], stages: 4 }
  );
});
