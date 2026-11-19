import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import sqlite3 from "sqlite3";

import { syncCclBrackets } from "./ccl-bracket.js";
import { registerCclDrawRoutes } from "./ccl-draw.js";
import { getCclFormat } from "./ccl-formats.js";
import {
  ADVANCE_LABEL,
  buildPublicGroupRows,
  formatMatchTime,
  formatRoundDates,
  registerCclPublicBracketRoutes,
} from "./ccl-public-brackets.js";
import { ensureCclSchema } from "./ccl-schema.js";
import { buildCclStructurePlan, generateCclStructure } from "./ccl-structure.js";
import { ensureDuelBracketColumns, ensureTournamentStructureSchema } from "./tournament-structure-schema.js";

const fixture = JSON.parse(readFileSync(new URL("./fixtures/ccl-2027q-brackets.json", import.meta.url), "utf8")).duels;
const RENAMED_ROUNDS = { "Losers Round 1": "Lower Round 1", "GF rematch (if needed)": "Group Final Reset" };
const format = getCclFormat("ccl-2027");
const plan = buildCclStructurePlan({
  tournamentId: "CCL-2027",
  format,
  startDates: { Q1: "2026-11-23", Q2: "2026-11-23", Q3: "2026-11-16" },
});
const settingsOf = (stageKey) => plan.stages.find((stage) => stage.stage_key === stageKey).settings;
const groupRows = (stageKey, groupKey) => buildPublicGroupRows({
  tournamentId: "CCL-2027",
  stageKey,
  groupKey,
  settings: settingsOf(stageKey),
  matches: plan.matches
    .filter((match) => match.group_key === groupKey)
    .map((match) => ({ ...match, id: `${groupKey}${match.bracket_no}`, status: "Draft", player_1_id: null, player_2_id: null })),
});

test("dates and times in the page's formats", () => {
  assert.equal(formatRoundDates("2026-11-29T23:59:00.000Z"), "23-29.Nov");
  assert.equal(formatRoundDates("2026-12-06T23:59:00.000Z"), "30.Nov-6.Dec");
  assert.equal(formatRoundDates(null), "");
  assert.equal(formatMatchTime("2026-11-20T18:05:00.000Z"), "20.11.2026 18:05:00");
  assert.equal(formatMatchTime(""), "");
});

for (const [stageKey, groupKey] of [["Q1", "A"], ["Q2", "F"], ["Q3", "X"]]) {
  test(`group ${groupKey}: rows match the Google Sheets bracket the page was built for`, () => {
    const rows = groupRows(stageKey, groupKey);
    const sheet = fixture.filter((row) => row.group === groupKey);
    assert.deepEqual(rows.map((row) => row.knockout_id), sheet.map((row) => row.knockout_id));
    for (const row of rows) {
      const expected = sheet.find((item) => item.knockout_id === row.knockout_id);
      const label = `${groupKey}${row.knockout_id}`;
      assert.equal(row.round_order, expected.round_order, `${label} column`);
      assert.equal(row.multiple_elimination_stage, expected.multiple_elimination_stage, `${label} side`);
      assert.equal(row.next_game_win, expected.next_game_win, `${label} next`);
      if (expected.player1 === null) {
        // The advance row after the match that sends a player to the League Stage.
        assert.equal(row.knockout_stage, ADVANCE_LABEL, label);
        assert.equal(row.duel_format, null, label);
        continue;
      }
      const [name, dates] = expected.round.split(": ");
      assert.equal(row.round, `${RENAMED_ROUNDS[name] || name}: ${dates}`, `${label} round`);
      assert.equal(row.duel_format, "Bo3");
      for (const field of ["player1", "player2"]) {
        if (/^Loser of \d+$/.test(expected[field] || "")) assert.equal(row[field], expected[field], `${label} ${field}`);
      }
    }
  });
}

// ---------------------------------------------------------------------------------------------

const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (error) => (error ? reject(error) : resolve())));
const run = (db, sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, (error) => (error ? reject(error) : resolve())));
const all = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || []))));

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    CREATE TABLE tournaments (id TEXT PRIMARY KEY COLLATE NOCASE, name TEXT, short_title TEXT, type TEXT, category TEXT,
      ranking INTEGER, is_test INTEGER, format_key TEXT);
    CREATE TABLE associations (association_row_id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE COLLATE NOCASE, name TEXT NOT NULL, flag TEXT);
    CREATE TABLE profiles (id TEXT PRIMARY KEY, bga_nickname TEXT, name TEXT, avatar TEXT, association TEXT,
      gg_elo REAL, gg_rating_position INTEGER, email TEXT, deleted_at TEXT);
    CREATE TABLE tournament_teams (id TEXT PRIMARY KEY, tournament_id TEXT NOT NULL, team_id TEXT NOT NULL, UNIQUE (tournament_id, team_id));
    CREATE TABLE teams (id TEXT PRIMARY KEY);
    CREATE TABLE tournament_access_users (tournament_entity_type TEXT, tournament_id TEXT, user_id INTEGER, role TEXT);
    CREATE TABLE duels (
      id TEXT PRIMARY KEY, tournament_id TEXT, match_id TEXT, duel_number INTEGER, duel_format TEXT, time_utc TEXT,
      player_1_id TEXT, player_2_id TEXT, dw1 INTEGER, dw2 INTEGER, ranking INTEGER NOT NULL DEFAULT 1, status TEXT,
      source_type TEXT, is_test INTEGER NOT NULL DEFAULT 0, stage TEXT, group_name TEXT, round_name TEXT,
      round_order INTEGER, deadline_utc TEXT, proposed_time_utc TEXT, proposed_time_by_player_id TEXT,
      proposed_time_status TEXT, cancellation_reason TEXT, created_by TEXT, updated_by TEXT, deleted_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO tournaments VALUES ('CCL-2027', 'Carcassonne Champions League 2027', 'CCL 2027', 'Individuals', 'CCL', 1, 0, 'ccl-2027');
    INSERT INTO associations (code, name, flag) VALUES ('UA', 'Ukraine', 'https://carcassonne.gg/gallery/ukraine.png');
  `);
  await ensureCclSchema(db);
  await ensureTournamentStructureSchema(db);
  await ensureDuelBracketColumns(db);
  // Q2 with 16 Seeded and 15 Unseeded: one group gets a Bye.
  for (let index = 1; index <= 31; index += 1) {
    const id = `${90000000 + index}`;
    await run(db, "INSERT INTO profiles (id, bga_nickname, avatar, association) VALUES (?, ?, ?, 'UA')", [id, `Player${index}`, `a${index}.jpg`]);
    await run(db, "INSERT INTO tournament_players (id, tournament_id, association_id, entry_stage, player_id) VALUES (?, 'CCL-2027', 'UA', 'Q2', ?)", [`slot-${index}`, id]);
    await run(
      db,
      `INSERT INTO ccl_stage_entries (tournament_id, stage, slot_id, qualified_by_name, player_id, elo, seed, elo_source, fixed_at, fixed_by)
       VALUES ('CCL-2027', 'Q2', ?, 'Associations', ?, 1800, ?, 'gg_elo', '2026-11-14T10:00:00Z', 'admin')`,
      [`slot-${index}`, id, index <= 16 ? "Seeded" : "Unseeded"]
    );
  }
  await generateCclStructure(db, {
    tournament: { id: "CCL-2027", format_key: "ccl-2027", ranking: 1, is_test: 0 },
    startDates: { Q1: "2026-11-23", Q2: "2026-11-23", Q3: "2026-11-16" },
  });
  return db;
}

function createApi(db) {
  const routes = new Map();
  const app = {};
  for (const method of ["get", "post", "patch", "delete"]) {
    app[method] = (routePath, ...handlers) => routes.set(`${method.toUpperCase()} ${routePath}`, handlers.at(-1));
  }
  const options = { db, requireAuthenticated: () => {}, logger: { error() {} } };
  registerCclDrawRoutes(app, { ...options, nextSeed: () => 5 });
  registerCclPublicBracketRoutes(app, options);
  return async (route, params, user = null) => {
    const result = { status: 200, body: null };
    const res = { status(code) { result.status = code; return this; }, json(value) { result.body = value; return this; } };
    await routes.get(route)({ user, params, body: {}, query: {} }, res);
    return result;
  };
}

test("public brackets: placeholders before the draw, players, Byes and scores after it", async (t) => {
  const db = await createDatabase(t);
  const call = createApi(db);
  const route = "GET /public/ccl/:tournamentId/brackets";
  assert.equal((await call(route, { tournamentId: "ETCOC-2026" })).status, 404);

  const before = (await call(route, { tournamentId: "ccl-2027" })).body;
  assert.equal(before.ok, true);
  assert.equal(before.tournaments[0].tournament_id, "CCL-2027");
  // 162 matches + 2 advance rows in each of 9 Q1/Q2 groups + 1 in each of 3 Q3 groups.
  assert.equal(before.duels.length, 162 + 18 + 3);
  const firstF = before.duels.find((row) => row.group === "F" && row.knockout_id === "1");
  assert.deepEqual([firstF.player1, firstF.player1_id, firstF.player2], ["Seeded #1", "", "Unseeded #1"]);
  // "Winner of N" only where a player crosses from the lower bracket to the upper one; no connector shows it.
  const slotsF = (no) => {
    const item = before.duels.find((row) => row.group === "F" && row.knockout_id === String(no));
    return [item.player1, item.player2];
  };
  assert.deepEqual(slotsF(7), ["", ""]);
  assert.deepEqual(slotsF(9), ["Loser of 8", ""]);
  assert.deepEqual(slotsF(13), ["Loser of 11", ""]);
  assert.deepEqual(before.players, []);

  await call("POST /ccl/:tournamentId/stages/:stageKey/draw", { tournamentId: "CCL-2027", stageKey: "Q2" }, { id: 1, admin: 1 });
  const played = (await all(db, "SELECT id, player_1_id FROM duels WHERE stage = 'Q2' AND bracket_no = 1 AND status = 'Planned' LIMIT 1"))[0];
  await run(db, "UPDATE duels SET dw1 = 2, dw2 = 1, status = 'Done', time_utc = '2026-11-25T19:30:00Z' WHERE id = ?", [played.id]);
  await syncCclBrackets(db, "CCL-2027");

  const after = (await call(route, { tournamentId: "CCL-2027" })).body;
  const row = after.duels.find((item) => item.duel_id === played.id);
  assert.equal(row.player1_id, played.player_1_id);
  assert.match(row.player1, /^Player\d+$/);
  assert.deepEqual([row.score1, row.score2, row.time], ["2", "1", "25.11.2026 19:30:00"]);
  assert.equal(row.round, "Round 1: 23-29.Nov");
  // The winner already appears in the Upper Semifinal.
  const semifinal = after.duels.find((item) => item.group === row.group && item.knockout_id === row.next_game_win);
  assert.ok([semifinal.player1_id, semifinal.player2_id].includes(played.player_1_id));
  // One Q2 match has a Bye.
  const bye = after.duels.filter((item) => item.player2 === "Bye" && Number(item.knockout_id) <= 4);
  assert.equal(bye.length, 1);
  assert.equal(bye[0].status, "Cancelled");
  // The Bye drops to the lower bracket: the next lower match shows it before match 1 or 3 is played.
  const lower = after.duels.find((item) => item.group === bye[0].group && item.knockout_id === (Number(bye[0].knockout_id) <= 2 ? "5" : "6"));
  assert.ok([lower.player1, lower.player2].includes("Bye"));
  assert.equal(after.players.length, 31);
  assert.equal(after.players[0].avatar.endsWith(".jpg"), true);
  assert.deepEqual(after.countries, [{ team: "Ukraine", flag: "https://carcassonne.gg/gallery/ukraine.png" }]);
  assert.equal(after.tournament_players.every((item) => item.country === "Ukraine"), true);
});
