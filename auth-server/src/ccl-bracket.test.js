import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import { BYE, registerCclBracketRoutes, resolveBracket, syncCclBrackets } from "./ccl-bracket.js";
import { registerCclDrawRoutes } from "./ccl-draw.js";
import { getCclFormat } from "./ccl-formats.js";
import { registerCclMatchRoutes } from "./ccl-match-routes.js";
import { ensureCclSchema } from "./ccl-schema.js";
import { generateCclStructure } from "./ccl-structure.js";
import { buildDoubleEliminationGroup } from "./double-elimination.js";
import { ensureDuelBracketColumns, ensureTournamentStructureSchema } from "./tournament-structure-schema.js";

const PATHS = Object.fromEntries(getCclFormat("ccl-2027").stages[0].children.map((path) => [path.key, path.settings]));

// One group in memory: the generated matches, played by a script of winners.
function group(grandFinal) {
  return buildDoubleEliminationGroup({ groupSize: 8, grandFinal }).map((template) => ({
    ...template,
    id: `m${template.bracket_no}`,
    player_1_id: null,
    player_2_id: null,
    status: "Draft",
    dw1: null,
    dw2: null,
  }));
}
const positionsOf = (count = 8, missing = []) => new Map(
  Array.from({ length: count }, (_, index) => index + 1).filter((position) => !missing.includes(position)).map((position) => [position, `p${position}`])
);

// Applies the expected players, then plays the given match: side 1 or 2 wins 2:0 / 2:1.
function settle(matches, resolved) {
  for (const match of matches) {
    const state = resolved.expected.get(match.bracket_no);
    if (match.status === "Done") continue;
    match.player_1_id = state.player_1_id === BYE ? null : state.player_1_id;
    match.player_2_id = state.player_2_id === BYE ? null : state.player_2_id;
  }
}
function play(matches, bracketNo, winnerSide, settings, positions = positionsOf()) {
  settle(matches, resolveBracket({ matches, positions, drawn: true, advance: settings.advance }));
  const match = matches.find((item) => item.bracket_no === bracketNo);
  assert.ok(match.player_1_id && match.player_2_id, `match ${bracketNo} has two players`);
  Object.assign(match, { status: "Done", dw1: winnerSide === 1 ? 2 : 1, dw2: winnerSide === 1 ? 1 : 2 });
  return resolveBracket({ matches, positions, drawn: true, advance: settings.advance });
}

test("Q1/Q2: winners of Upper Final and Consolidated Final leave the group, six are eliminated", () => {
  const matches = group("none");
  let resolved = resolveBracket({ matches, positions: positionsOf(), drawn: false, advance: PATHS.Q1.advance });
  assert.equal(resolved.expected.get(1).kind, "pending");
  resolved = resolveBracket({ matches, positions: positionsOf(), drawn: true, advance: PATHS.Q1.advance });
  assert.deepEqual([resolved.expected.get(1).player_1_id, resolved.expected.get(1).player_2_id], ["p1", "p2"]);
  assert.equal(resolved.expected.get(7).kind, "pending");

  // Seeded players win Round 1; p1 wins the upper bracket; p6 comes through the lower bracket.
  for (const [no, side] of [[1, 1], [2, 1], [3, 1], [4, 1], [5, 1], [6, 2], [7, 1], [8, 2], [9, 1], [10, 2], [11, 1], [12, 2]]) {
    resolved = play(matches, no, side, PATHS.Q1);
  }
  assert.deepEqual([resolved.expected.get(9).player_1_id, resolved.expected.get(9).player_2_id], ["p5", "p2"]);
  assert.deepEqual([resolved.expected.get(13).player_1_id, resolved.expected.get(13).player_2_id], ["p7", "p8"]);
  assert.equal(resolved.complete, false);
  resolved = play(matches, 13, 2, PATHS.Q1);
  assert.deepEqual(resolved.qualified.map((row) => [row.player_id, row.label]), [["p1", "Winner"], ["p8", "2nd Place"]]);
  assert.deepEqual(resolved.eliminated.sort(), ["p2", "p3", "p4", "p5", "p6", "p7"]);
  assert.equal(resolved.complete, true);
});

test("Q3: the reset is not needed when the upper finalist wins the Group Final", () => {
  const matches = group("reset");
  let resolved;
  for (const [no, side] of [[1, 1], [2, 1], [3, 1], [4, 1], [5, 1], [6, 1], [7, 1], [8, 1], [9, 1], [10, 1], [11, 1], [12, 1], [13, 1], [14, 1]]) {
    resolved = play(matches, no, side, PATHS.Q3);
  }
  assert.equal(resolved.expected.get(15).kind, "not_needed");
  assert.deepEqual(resolved.qualified.map((row) => row.player_id), ["p1"]);
  assert.equal(resolved.eliminated.length, 7);
  assert.ok(resolved.eliminated.includes(resolved.outcomes.get(14).loser));
  assert.equal(resolved.complete, true);
});

test("Q3: a lower finalist who wins the Group Final forces the reset", () => {
  const matches = group("reset");
  let resolved;
  for (const [no, side] of [[1, 1], [2, 1], [3, 1], [4, 1], [5, 1], [6, 1], [7, 1], [8, 1], [9, 1], [10, 1], [11, 1], [12, 1], [13, 1], [14, 2]]) {
    resolved = play(matches, no, side, PATHS.Q3);
  }
  const final = resolved.expected.get(14);
  assert.equal(resolved.expected.get(15).kind, "play");
  assert.deepEqual([resolved.expected.get(15).player_1_id, resolved.expected.get(15).player_2_id], [final.player_1_id, final.player_2_id]);
  // Nobody has left yet: the upper finalist has lost only once.
  assert.deepEqual(resolved.qualified, []);
  assert.equal(resolved.eliminated.includes(final.player_1_id), false);
  resolved = play(matches, 15, 1, PATHS.Q3);
  assert.deepEqual(resolved.qualified.map((row) => row.player_id), [final.player_1_id]);
  assert.ok(resolved.eliminated.includes(final.player_2_id));
});

test("a Bye sends the Seeded player on and drops to the lower bracket", () => {
  const matches = group("none");
  const positions = positionsOf(8, [2]);
  const resolved = resolveBracket({ matches, positions, drawn: true, advance: PATHS.Q2.advance });
  assert.equal(resolved.expected.get(1).kind, "bye");
  assert.deepEqual(resolved.outcomes.get(1), { winner: "p1", loser: BYE, bye: true });
  assert.equal(resolved.expected.get(7).player_1_id, "p1");
  assert.equal(resolved.expected.get(5).player_1_id, BYE);
  // Once match 2 is played, its loser walks through match 5 (against the Bye) to match 9.
  const after = play(matches, 2, 1, PATHS.Q2, positions);
  assert.equal(after.expected.get(5).kind, "bye");
  assert.equal(after.expected.get(9).player_2_id, "p4");
  assert.deepEqual(after.eliminated, []);
});

test("a started match keeps its players; the bracket after it does not move", () => {
  const matches = group("none");
  play(matches, 1, 1, PATHS.Q1);
  play(matches, 2, 1, PATHS.Q1);
  const resolved = play(matches, 7, 1, PATHS.Q1);
  assert.equal(resolved.expected.get(11).player_1_id, "p1");
  // Match 1 is corrected to a win for p2, but match 7 has already been played by p1.
  Object.assign(matches[0], { dw1: 0, dw2: 2 });
  const corrected = resolveBracket({ matches, positions: positionsOf(), drawn: true, advance: PATHS.Q1.advance });
  assert.deepEqual(corrected.conflicts.map((conflict) => conflict.bracket_no), [7]);
  assert.equal(corrected.outcomes.has(7), false);
  assert.equal(corrected.expected.get(11).player_1_id, null);
});

// ---------------------------------------------------------------------------------------------

const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (error) => (error ? reject(error) : resolve())));
const all = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || []))));
const run = (db, sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, (error) => (error ? reject(error) : resolve())));
const ADMIN = { id: 1, admin: 1, player_id: "admin-player" };

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    CREATE TABLE tournaments (
      id TEXT PRIMARY KEY COLLATE NOCASE, name TEXT, short_title TEXT, type TEXT, category TEXT, ranking INTEGER, is_test INTEGER,
      format_key TEXT, ccl_main_registration_ends_at TEXT, ccl_q12_registration_ends_at TEXT, ccl_q3_registration_ends_at TEXT
    );
    CREATE TABLE associations (association_row_id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE COLLATE NOCASE, name TEXT NOT NULL, flag TEXT);
    CREATE TABLE profiles (id TEXT PRIMARY KEY, bga_nickname TEXT, name TEXT, avatar TEXT, association TEXT,
      gg_elo REAL, gg_rating_position INTEGER, email TEXT, deleted_at TEXT);
    CREATE TABLE teams (id TEXT PRIMARY KEY, name TEXT, timezone TEXT);
    CREATE TABLE tournament_teams (id TEXT PRIMARY KEY, tournament_id TEXT NOT NULL, team_id TEXT NOT NULL, UNIQUE (tournament_id, team_id));
    CREATE TABLE tournament_access_users (tournament_entity_type TEXT, tournament_id TEXT, user_id INTEGER, role TEXT);
    CREATE TABLE duel_formats (format TEXT PRIMARY KEY, games_to_win INTEGER);
    INSERT INTO duel_formats VALUES ('Bo3', 2), ('Bo5', 3);
    CREATE TABLE duels (
      id TEXT PRIMARY KEY, tournament_id TEXT, match_id TEXT, duel_number INTEGER, duel_format TEXT, time_utc TEXT,
      player_1_id TEXT, player_2_id TEXT, dw1 INTEGER, dw2 INTEGER, ranking INTEGER NOT NULL DEFAULT 1, status TEXT,
      source_type TEXT, is_test INTEGER NOT NULL DEFAULT 0, stage TEXT, group_name TEXT, round_name TEXT,
      round_order INTEGER, deadline_utc TEXT, proposed_time_utc TEXT, proposed_time_by_player_id TEXT,
      proposed_time_status TEXT, cancellation_reason TEXT, created_by TEXT, updated_by TEXT, deleted_by TEXT, deleted_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO tournaments (id, name, type, category, ranking, is_test, format_key)
      VALUES ('CCL-2027', 'CCL 2027', 'Individuals', 'CCL', 1, 0, 'ccl-2027');
  `);
  await ensureCclSchema(db);
  await ensureTournamentStructureSchema(db);
  await ensureDuelBracketColumns(db);
  // Q3: a full field. Q2: 16 Seeded and 13 Unseeded, so three groups get a Bye.
  const fields = [["Q3", 12, 12], ["Q2", 16, 13]];
  for (const [stage, seeded, unseeded] of fields) {
    for (let index = 1; index <= seeded + unseeded; index += 1) {
      const id = `${stage.toLowerCase()}-${index}`;
      await run(db, "INSERT INTO profiles (id, bga_nickname, association) VALUES (?, ?, ?)", [id, id, `A${index}`]);
      await run(db, "INSERT INTO tournament_players (id, tournament_id, association_id, entry_stage, player_id) VALUES (?, 'CCL-2027', ?, ?, ?)", [`slot-${id}`, `A${index}`, stage, id]);
      await run(
        db,
        `INSERT INTO ccl_stage_entries (tournament_id, stage, slot_id, qualified_by_name, player_id, elo, seed, elo_source, fixed_at, fixed_by)
         VALUES ('CCL-2027', ?, ?, 'Associations', ?, 1800, ?, 'gg_elo', '2026-11-14T10:00:00Z', 'admin')`,
        [stage, `slot-${id}`, id, index <= seeded ? "Seeded" : "Unseeded"]
      );
    }
  }
  await generateCclStructure(db, {
    tournament: { id: "CCL-2027", format_key: "ccl-2027", ranking: 1, is_test: 0 },
    startDates: { Q2: "2026-11-23", Q3: "2026-11-16" },
  });
  return db;
}

function createApi(db) {
  const routes = new Map();
  const app = {};
  for (const method of ["get", "post", "patch", "delete"]) {
    app[method] = (routePath, ...handlers) => routes.set(`${method.toUpperCase()} ${routePath}`, handlers.at(-1));
  }
  const audit = [];
  const options = { db, requireAuthenticated: () => {}, logAuditEvent: (entry, done) => { audit.push(entry); done(); }, logger: { error() {}, warn() {} } };
  registerCclDrawRoutes(app, { ...options, nextSeed: () => 77 });
  registerCclBracketRoutes(app, options);
  registerCclMatchRoutes(app, options);
  const call = async (route, { params = {}, body = {} } = {}) => {
    const handler = routes.get(route);
    assert.ok(handler, `route ${route} is not registered`);
    const result = { status: 200, body: null };
    const res = { status(code) { result.status = code; return this; }, json(value) { result.body = value; return this; } };
    await handler({ user: ADMIN, params: { tournamentId: "CCL-2027", ...params }, body, query: {} }, res);
    return result;
  };
  return { call, audit };
}

const groupMatches = (db, group) => all(db, "SELECT * FROM duels WHERE group_name = ? ORDER BY bracket_no", [group]);
const result = (api, id, dw1, dw2) => api.call("POST /ccl/matches/:id/result", { params: { id }, body: { dw1, dw2 } });

test("Q2 draw with Byes: Bye matches are settled at once", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const drawn = await api.call("POST /ccl/:tournamentId/stages/:stageKey/draw", { params: { stageKey: "Q2" } });
  assert.equal(drawn.status, 200, JSON.stringify(drawn.body));
  const byes = await all(db, "SELECT * FROM duels WHERE stage = 'Q2' AND status = 'Cancelled' AND cancellation_reason = 'bye'");
  assert.equal(byes.length, 3);
  for (const bye of byes) {
    // The Seeded player is already in the Upper Semifinal.
    const semifinal = (await groupMatches(db, bye.group_name)).find((match) => match.bracket_no === (bye.bracket_no <= 2 ? 7 : 8));
    const side = bye.bracket_no % 2 ? "player_1_id" : "player_2_id";
    assert.equal(semifinal[side], bye.player_1_id);
    assert.equal(semifinal.status, "Draft");
  }
  // Reset stays possible: Bye matches do not count as played.
  assert.equal((await api.call("DELETE /ccl/:tournamentId/stages/:stageKey/draw", { params: { stageKey: "Q2" } })).status, 200);
  assert.equal((await all(db, "SELECT COUNT(*) AS n FROM duels WHERE stage = 'Q2' AND status <> 'Draft'")).at(0).n, 0);
});

test("Q3 group played to the end through admin results", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await api.call("POST /ccl/:tournamentId/stages/:stageKey/draw", { params: { stageKey: "Q3" } });
  const byNo = async () => new Map((await groupMatches(db, "X")).map((match) => [match.bracket_no, match]));

  let matches = await byNo();
  assert.equal((await result(api, matches.get(7).id, 2, 0)).status, 409); // no players yet
  assert.equal((await result(api, matches.get(1).id, 2, 2)).status, 400);
  assert.equal((await result(api, matches.get(1).id, 3, 0)).status, 400);

  for (const no of [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]) {
    matches = await byNo();
    const saved = await result(api, matches.get(no).id, 2, 1);
    assert.equal(saved.status, 200, `match ${no}: ${JSON.stringify(saved.body)}`);
  }
  matches = await byNo();
  assert.equal(matches.get(15).status, "Cancelled");
  assert.equal(matches.get(15).cancellation_reason, "not_needed");
  const winner = matches.get(14).player_1_id;
  const slots = new Map((await all(db, "SELECT player_id, league_status FROM tournament_players WHERE entry_stage = 'Q3'")).map((row) => [row.player_id, row.league_status]));
  assert.equal(slots.get(winner), "qualified");
  assert.equal([...slots.values()].filter((status) => status === "eliminated").length, 7);
  const main = await all(db, "SELECT e.qualified_by_name FROM ccl_stage_entries e JOIN tournament_players tp ON tp.id = e.slot_id WHERE e.stage = 'Main'");
  assert.deepEqual(main.map((row) => row.qualified_by_name), ["Q3, Group X Winner"]);
  const stage = (await all(db, "SELECT status FROM tournament_stages WHERE stage_key = 'Q3'")).at(0);
  assert.equal(stage.status, "in_progress"); // groups Y and Z are still playing
  assert.ok(api.audit.some((entry) => entry.event_type === "ccl.bracket.advanced"));
});

test("a result is refused when it would move a player out of a started match", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await api.call("POST /ccl/:tournamentId/stages/:stageKey/draw", { params: { stageKey: "Q3" } });
  let matches = new Map((await groupMatches(db, "Y")).map((match) => [match.bracket_no, match]));
  await result(api, matches.get(1).id, 2, 0);
  await result(api, matches.get(2).id, 2, 0);
  matches = new Map((await groupMatches(db, "Y")).map((match) => [match.bracket_no, match]));
  assert.equal(matches.get(7).status, "Planned");
  await run(db, "UPDATE duels SET time_utc = '2026-11-25T18:00:00Z', proposed_time_status = 'accepted' WHERE id = ?", [matches.get(7).id]);

  const flipped = await result(api, matches.get(1).id, 0, 2);
  assert.equal(flipped.status, 409);
  assert.deepEqual(flipped.body.conflicts.map((conflict) => conflict.bracket_no), [7]);
  assert.equal((await all(db, "SELECT dw1 FROM duels WHERE id = ?", [matches.get(1).id])).at(0).dw1, 2);

  // Clearing a result before anything depends on it moves the bracket back.
  const cleared = await api.call("POST /ccl/matches/:id/result", { params: { id: matches.get(2).id }, body: { clear: true } });
  assert.equal(cleared.status, 409);
  await run(db, "UPDATE duels SET time_utc = NULL, proposed_time_status = NULL WHERE id = ?", [matches.get(7).id]);
  assert.equal((await api.call("POST /ccl/matches/:id/result", { params: { id: matches.get(2).id }, body: { clear: true } })).status, 200);
  matches = new Map((await groupMatches(db, "Y")).map((match) => [match.bracket_no, match]));
  assert.equal(matches.get(7).player_2_id, null);
  assert.equal(matches.get(7).status, "Draft");
  assert.equal(matches.get(5).player_2_id, null);
});

test("BGA results written straight into duels are picked up by the sync", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await api.call("POST /ccl/:tournamentId/stages/:stageKey/draw", { params: { stageKey: "Q3" } });
  const first = (await groupMatches(db, "Z"))[0];
  await run(db, "UPDATE duels SET dw1 = 1, dw2 = 2, status = 'Done', time_utc = '2026-11-18T18:00:00Z' WHERE id = ?", [first.id]);
  const synced = await syncCclBrackets(db, "CCL-2027");
  assert.equal(synced.changed, true);
  const matches = await groupMatches(db, "Z");
  assert.equal(matches.find((match) => match.bracket_no === 7).player_1_id, first.player_2_id);
  assert.equal(matches.find((match) => match.bracket_no === 5).player_1_id, first.player_1_id);
  assert.equal((await syncCclBrackets(db, "CCL-2027")).changed, false);
});

test("bracket matches keep their players and place; they cannot be deleted", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await api.call("POST /ccl/:tournamentId/stages/:stageKey/draw", { params: { stageKey: "Q3" } });
  const match = (await groupMatches(db, "X"))[0];
  const moved = await api.call("PATCH /ccl/matches/:id", { params: { id: match.id }, body: { player_2_id: "q3-1" } });
  assert.equal(moved.status, 409);
  assert.equal((await api.call("DELETE /ccl/matches/:id", { params: { id: match.id } })).status, 409);
  // The time and the same players are fine.
  const timed = await api.call("PATCH /ccl/matches/:id", {
    params: { id: match.id },
    body: { player_1_id: match.player_1_id, player_2_id: match.player_2_id, time_utc: "2026-11-19T18:00:00Z" },
  });
  assert.equal(timed.status, 200, JSON.stringify(timed.body));
  assert.equal(timed.body.match.is_bracket, true);
});
