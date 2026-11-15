import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import { registerCclDrawRoutes, runCclDraw } from "./ccl-draw.js";
import { registerCclRoutes } from "./ccl-routes.js";
import { ensureCclSchema } from "./ccl-schema.js";
import { generateCclStructure } from "./ccl-structure.js";
import { ensureDuelBracketColumns, ensureTournamentStructureSchema } from "./tournament-structure-schema.js";

const groupsOf = (...keys) => keys.map((key) => ({ id: `g-${key}`, group_key: key }));

// Players of the named associations alternate between the pots (Seeded, Unseeded, Seeded, ...);
// the rest of the field are single representatives.
function field({ seeded, unseeded, associations = {} }) {
  const pots = [...Array(seeded).fill("Seeded"), ...Array(unseeded).fill("Unseeded")];
  const codes = pots.map((_, index) => `SOLO${index}`);
  Object.entries(associations).flatMap(([code, count]) => Array(count).fill(code)).forEach((code, k) => {
    codes[k % 2 ? seeded + Math.floor(k / 2) : Math.floor(k / 2)] = code;
  });
  return pots.map((seed, index) => ({ slot_id: `slot-${index + 1}`, player_id: `p${index + 1}`, seed, association_id: codes[index] }));
}

function assertValidDraw(result, entries, groups, { maxPerGroup = () => 1 } = {}) {
  assert.equal(result.error, undefined, result.error);
  const bySlot = new Map(entries.map((entry) => [entry.slot_id, entry]));
  const players = result.placements.filter((placement) => !placement.bye);
  assert.equal(players.length, entries.length);
  assert.equal(new Set(players.map((placement) => placement.slot_id)).size, entries.length);
  assert.deepEqual(result.placements.map((placement) => placement.draw_order), result.placements.map((_, index) => index + 1));
  for (const group of groups) {
    const inGroup = result.placements.filter((placement) => placement.group_id === group.id);
    assert.deepEqual(inGroup.map((placement) => placement.draw_position).sort((a, b) => a - b), [1, 2, 3, 4, 5, 6, 7, 8], group.group_key);
    for (const placement of inGroup.filter((item) => !item.bye)) {
      const entry = bySlot.get(placement.slot_id);
      // Every Round 1 match is Seeded (odd position) vs Unseeded (even position).
      assert.equal(entry.seed, placement.draw_position % 2 ? "Seeded" : "Unseeded");
    }
    const counts = new Map();
    for (const placement of inGroup.filter((item) => !item.bye)) {
      const association = bySlot.get(placement.slot_id).association_id;
      counts.set(association, (counts.get(association) || 0) + 1);
    }
    for (const [association, count] of counts) {
      assert.ok(count <= maxPerGroup(association), `${group.group_key}: ${count} × ${association}`);
    }
    // No Round 1 match between two players of one association.
    for (let position = 1; position <= 7; position += 2) {
      const a = inGroup.find((item) => item.draw_position === position);
      const b = inGroup.find((item) => item.draw_position === position + 1);
      if (!a.bye && !b.bye) {
        assert.notEqual(bySlot.get(a.slot_id).association_id, bySlot.get(b.slot_id).association_id, `${group.group_key} pair ${position}`);
      }
    }
  }
}

test("Q1: 40 players in 5 groups; associations are spread over different groups", () => {
  const groups = groupsOf("A", "B", "C", "D", "E");
  const entries = field({ seeded: 20, unseeded: 20, associations: { ARG: 5, UKR: 5, FRA: 4, ESP: 3, ITA: 2 } });
  for (const seed of [1, 7, 42, 2026, 99999]) {
    const result = runCclDraw({ entries, groups, seed });
    assertValidDraw(result, entries, groups);
    assert.deepEqual(result.conflicts, []);
    assert.equal(result.byes, 0);
  }
  // The same seed replays the same draw; another seed gives another one.
  assert.deepEqual(runCclDraw({ entries, groups, seed: 5 }), runCclDraw({ entries, groups, seed: 5 }));
  assert.notDeepEqual(runCclDraw({ entries, groups, seed: 5 }).placements, runCclDraw({ entries, groups, seed: 6 }).placements);
});

test("big associations are placed first, both pots together", () => {
  const groups = groupsOf("A", "B", "C", "D", "E");
  const entries = field({ seeded: 20, unseeded: 20, associations: { ARG: 5, UKR: 3 } });
  const result = runCclDraw({ entries, groups, seed: 11 });
  const association = (placement) => entries.find((entry) => entry.slot_id === placement.slot_id).association_id;
  assert.deepEqual(result.placements.slice(0, 5).map(association), Array(5).fill("ARG"));
  assert.deepEqual(result.placements.slice(5, 8).map(association), Array(3).fill("UKR"));
  const pots = new Set(result.placements.slice(0, 5).map((placement) => placement.draw_position % 2));
  assert.equal(pots.size, 2);
});

test("Q3: more players of an association than groups → at most ceil(n / groups) per group", () => {
  const groups = groupsOf("X", "Y", "Z");
  const entries = field({ seeded: 12, unseeded: 12, associations: { ARG: 5, UKR: 4 } });
  for (const seed of [3, 30, 300]) {
    const result = runCclDraw({ entries, groups, seed });
    assertValidDraw(result, entries, groups, { maxPerGroup: (code) => ({ ARG: 2, UKR: 2 }[code] || 1) });
    assert.deepEqual(result.conflicts, []);
  }
});

test("Q2: missing Unseeded become Byes, one per group, placed first", () => {
  const groups = groupsOf("F", "G", "H", "I");
  const entries = field({ seeded: 16, unseeded: 13, associations: { ARG: 3 } });
  const result = runCclDraw({ entries, groups, seed: 8 });
  assertValidDraw(result, entries, groups);
  assert.equal(result.byes, 3);
  const byes = result.placements.filter((placement) => placement.bye);
  assert.deepEqual(byes.map((placement) => placement.draw_order), [1, 2, 3]);
  assert.equal(new Set(byes.map((placement) => placement.group_id)).size, 3);
  assert.ok(byes.every((placement) => placement.draw_position % 2 === 0));
});

test("an impossible field is still drawn, with the conflicts reported", () => {
  const groups = groupsOf("A");
  // Three Seeded and two Unseeded of one association: one Unseeded Argentinian must face an Argentinian.
  const entries = field({ seeded: 4, unseeded: 4, associations: { ARG: 5 } });
  const result = runCclDraw({ entries, groups, seed: 1 });
  assert.equal(result.attempts, 1001);
  assert.equal(result.conflicts.length, 1);
  assert.equal(result.conflicts[0].association_id, "ARG");
  assert.equal(result.conflicts[0].group_key, "A");
});

test("the field must fit the pots", () => {
  const groups = groupsOf("X", "Y", "Z");
  assert.match(runCclDraw({ entries: field({ seeded: 11, unseeded: 12 }), groups, seed: 1 }).error, /exactly 12 Seeded/);
  assert.match(runCclDraw({ entries: field({ seeded: 12, unseeded: 13 }), groups, seed: 1 }).error, /at most 12 Unseeded/);
  assert.match(runCclDraw({ entries: field({ seeded: 12, unseeded: 12 }), groups: [], seed: 1 }).error, /no groups/);
});

// ---------------------------------------------------------------------------------------------

const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (error) => (error ? reject(error) : resolve())));
const all = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || []))));
const run = (db, sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, (error) => (error ? reject(error) : resolve())));
const ADMIN = { id: 1, admin: 1, player_id: "admin-player" };
const PLAYER = { id: 3, admin: 0, player_id: "q3-1" };

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    CREATE TABLE tournaments (
      id TEXT PRIMARY KEY COLLATE NOCASE, name TEXT, type TEXT, category TEXT, ranking INTEGER, is_test INTEGER,
      format_key TEXT, ccl_main_registration_ends_at TEXT, ccl_q12_registration_ends_at TEXT, ccl_q3_registration_ends_at TEXT
    );
    CREATE TABLE associations (association_row_id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE COLLATE NOCASE, name TEXT NOT NULL, flag TEXT);
    CREATE TABLE profiles (id TEXT PRIMARY KEY, bga_nickname TEXT, name TEXT, avatar TEXT, association TEXT,
      gg_elo REAL, gg_rating_position INTEGER, email TEXT, deleted_at TEXT);
    CREATE TABLE teams (id TEXT PRIMARY KEY);
    CREATE TABLE tournament_teams (id TEXT PRIMARY KEY, tournament_id TEXT NOT NULL, team_id TEXT NOT NULL, UNIQUE (tournament_id, team_id));
    CREATE TABLE tournament_access_users (tournament_entity_type TEXT, tournament_id TEXT, user_id INTEGER, role TEXT);
    CREATE TABLE duels (
      id TEXT PRIMARY KEY, tournament_id TEXT, match_id TEXT, duel_number INTEGER, duel_format TEXT, time_utc TEXT,
      player_1_id TEXT, player_2_id TEXT, dw1 INTEGER, dw2 INTEGER, ranking INTEGER NOT NULL DEFAULT 1, status TEXT,
      source_type TEXT, is_test INTEGER NOT NULL DEFAULT 0, stage TEXT, group_name TEXT, round_name TEXT,
      round_order INTEGER, deadline_utc TEXT, proposed_time_utc TEXT, proposed_time_by_player_id TEXT,
      proposed_time_status TEXT, cancellation_reason TEXT, created_by TEXT, updated_by TEXT, deleted_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO tournaments (id, name, type, category, ranking, is_test, format_key)
      VALUES ('CCL-2027', 'CCL 2027', 'Individuals', 'CCL', 1, 0, 'ccl-2027');
    INSERT INTO associations (code, name, flag) VALUES ('AR', 'Argentina', 'ar.png'), ('UA', 'Ukraine', 'ua.png');
  `);
  await ensureCclSchema(db);
  await ensureTournamentStructureSchema(db);
  await ensureDuelBracketColumns(db);
  // A full Q3 field: 12 Seeded + 12 Unseeded, four Argentinians and three Ukrainians among them.
  for (let index = 1; index <= 24; index += 1) {
    const association = index <= 4 ? "AR" : index <= 7 ? "UA" : `S${index}`;
    const id = `q3-${index}`;
    await run(db, "INSERT INTO profiles (id, bga_nickname, association) VALUES (?, ?, ?)", [id, `Player ${index}`, association]);
    await run(db, "INSERT INTO tournament_players (id, tournament_id, association_id, entry_stage, player_id) VALUES (?, 'CCL-2027', ?, 'Q3', ?)", [`slot-${index}`, association, id]);
    await run(
      db,
      `INSERT INTO ccl_stage_entries (tournament_id, stage, slot_id, qualified_by_name, player_id, elo, seed, elo_source, fixed_at, fixed_by)
       VALUES ('CCL-2027', 'Q3', ?, 'Top GG Elo Player', ?, ?, ?, 'gg_elo', '2026-11-14T10:00:00Z', 'admin')`,
      [`slot-${index}`, id, 2000 - index, index % 2 ? "Seeded" : "Unseeded"]
    );
  }
  await generateCclStructure(db, { tournament: { id: "CCL-2027", format_key: "ccl-2027", ranking: 1, is_test: 0 }, startDates: { Q3: "2026-11-16" } });
  return db;
}

function createApi(db) {
  const routes = new Map();
  const app = {};
  for (const method of ["get", "post", "patch", "delete"]) {
    app[method] = (routePath, ...handlers) => routes.set(`${method.toUpperCase()} ${routePath}`, handlers.at(-1));
  }
  const audit = [];
  const options = {
    db,
    requireAuthenticated: () => {},
    logAuditEvent: (entry, done) => { audit.push(entry); done(); },
    logger: { error() {} },
    now: () => new Date("2026-11-15T18:00:00Z"),
  };
  let seed = 100;
  registerCclDrawRoutes(app, { ...options, nextSeed: () => { seed += 1; return seed; } });
  registerCclRoutes(app, options);
  const call = async (route, { user = ADMIN, params = {}, body = {} } = {}) => {
    const handler = routes.get(route);
    assert.ok(handler, `route ${route} is not registered`);
    const result = { status: 200, body: null };
    const res = { status(code) { result.status = code; return this; }, json(value) { result.body = value; return this; } };
    await handler({ user, params: { tournamentId: "CCL-2027", stageKey: "Q3", ...params }, body, query: {} }, res);
    return result;
  };
  return { call, audit };
}

const DRAW = "/ccl/:tournamentId/stages/:stageKey/draw";

test("test draws save nothing; the real draw fills Round 1 once", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  assert.equal((await api.call(`POST ${DRAW}/preview`, { user: PLAYER })).status, 403);

  const first = await api.call(`POST ${DRAW}/preview`);
  assert.equal(first.status, 200, JSON.stringify(first.body));
  assert.equal(first.body.test, true);
  assert.equal(first.body.groups.length, 3);
  assert.equal(first.body.placements.length, 24);
  assert.equal(first.body.placements[0].player.association_name !== undefined, true);
  const second = await api.call(`POST ${DRAW}/preview`);
  assert.notEqual(second.body.seed, first.body.seed);
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM ccl_stage_entries WHERE group_id IS NOT NULL")).at(0).n, 0);
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM duels WHERE player_1_id IS NOT NULL")).at(0).n, 0);

  const made = await api.call(`POST ${DRAW}`);
  assert.equal(made.status, 200, JSON.stringify(made.body));
  assert.equal(made.body.drawn, true);
  assert.equal(api.audit.at(-1).event_type, "ccl.draw.made");
  assert.equal(api.audit.at(-1).metadata.seed, made.body.seed);
  assert.equal(api.audit.at(-1).metadata.placements.length, 24);

  const roundOne = await all(db, "SELECT * FROM duels WHERE stage = 'Q3' AND bracket_no <= 4 ORDER BY group_name, bracket_no");
  assert.equal(roundOne.length, 12);
  assert.ok(roundOne.every((duel) => duel.status === "Planned" && duel.player_1_id && duel.player_2_id));
  const seeds = new Map((await all(db, "SELECT player_id, seed FROM ccl_stage_entries")).map((row) => [row.player_id, row.seed]));
  assert.ok(roundOne.every((duel) => seeds.get(duel.player_1_id) === "Seeded" && seeds.get(duel.player_2_id) === "Unseeded"));
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM duels WHERE stage = 'Q3' AND bracket_no > 4 AND player_1_id IS NOT NULL")).at(0).n, 0);
  assert.equal((await all(db, "SELECT status FROM tournament_stages WHERE stage_key = 'Q3'")).at(0).status, "drawn");
  // The other paths are untouched.
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM duels WHERE stage <> 'Q3' AND status <> 'Draft'")).at(0).n, 0);

  // The saved draw reads back in the same order, for the stream animation.
  const saved = (await api.call(`GET ${DRAW}`)).body;
  assert.equal(saved.drawn, true);
  assert.deepEqual(
    saved.placements.map((placement) => [placement.group_key, placement.draw_position, placement.slot_id]),
    made.body.placements.map((placement) => [placement.group_key, placement.draw_position, placement.slot_id]),
  );

  assert.equal((await api.call(`POST ${DRAW}`)).status, 409);
  assert.equal((await api.call(`POST ${DRAW}/preview`)).status, 409);
});

test("after the draw the Q3 field is locked; a reset is possible until a match is scheduled", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await api.call(`POST ${DRAW}`);

  // Changing pots or a player of Q3 would change the drawn field.
  const pots = await api.call("POST /ccl/:tournamentId/pots/:phase", { params: { phase: "q3" } });
  assert.equal(pots.status, 409);
  assert.match(pots.body.message, /Q3 is already made/);
  const replaced = await api.call("PATCH /ccl/slots/:id", { params: { id: "slot-1" }, body: { player_id: null } });
  assert.equal(replaced.status, 409);
  assert.equal((await all(db, "SELECT player_id FROM tournament_players WHERE id = 'slot-1'")).at(0).player_id, "q3-1");

  await run(db, "UPDATE duels SET proposed_time_status = 'accepted', time_utc = '2026-11-18T18:00:00Z' WHERE stage = 'Q3' AND group_name = 'Y' AND bracket_no = 2");
  const blocked = await api.call(`DELETE ${DRAW}`);
  assert.equal(blocked.status, 409);
  assert.equal(blocked.body.blockers.length, 1);

  await run(db, "UPDATE duels SET proposed_time_status = NULL, time_utc = NULL");
  const reset = await api.call(`DELETE ${DRAW}`);
  assert.equal(reset.status, 200, JSON.stringify(reset.body));
  assert.equal(reset.body.drawn, false);
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM duels WHERE player_1_id IS NOT NULL OR status <> 'Draft'")).at(0).n, 0);
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM ccl_stage_entries WHERE group_id IS NOT NULL")).at(0).n, 0);
  assert.equal((await api.call("POST /ccl/:tournamentId/pots/:phase", { params: { phase: "q3" } })).status, 200);
});

test("the draw waits for every player and pot", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await run(db, "UPDATE ccl_stage_entries SET seed = NULL WHERE slot_id = 'slot-5'");
  const blocked = await api.call(`POST ${DRAW}`);
  assert.equal(blocked.status, 409);
  assert.deepEqual(blocked.body.blockers, [{ slot_id: "slot-5", reason: "seed_missing" }]);
  assert.equal((await api.call(`GET ${DRAW}`)).body.blockers.length, 1);
  assert.equal((await api.call(`POST ${DRAW}`, { params: { stageKey: "qualifiers" } })).status, 404);
});
