import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import sqlite3 from "sqlite3";

const source = readFileSync(new URL("./server.js", import.meta.url), "utf8");
function serverFunction(name) {
  const start = source.search(new RegExp(`(?:async )?function ${name}\\(`));
  assert.ok(start >= 0, `Missing function ${name}`);
  return source.slice(start, source.indexOf("\n}", start) + 2);
}
function routeBetween(start, end) {
  const offset = source.indexOf(start);
  return source.slice(offset, source.indexOf(end, offset));
}
const helpers = [
  "normalizeNullableText", "normalizeIntegerOrNull", "normalizePositiveInteger",
  "normalizeStatusText", "normalizePlannedDuelScores", "normalizePlannedMatchScores",
  "normalizeTournamentAccessType", "normalizeTournamentCaptainTeamIds",
  "canClosedTournamentCaptainAccessMatch", "isCompletedMatchStatus",
  "hasStartedMoreThanHoursAgo", "canUserEditMatchResults", "recomputeDuelAggregates",
  "recomputeMatchAggregates", "normalizeStandingsScoring", "normalizeStandingsResultStage",
  "standingsScoreValue", "createEmptyStandingsStats", "applyStandingsResult",
  "compareCalculatedStandings", "recalculateTournamentStandings", "buildGeneratedMatchId",
].map(serverFunction).join("\n");
const routes = [
  routeBetween('app.post("/duels/:id/games/save",', 'app.post("/duels/:id/fix",'),
  routeBetween('app.patch("/matches/:id",', 'app.post("/matches/:id/time-proposal",'),
].join("\n");

const globalAdmin = { admin: 1, player_id: "admin" };
const tournamentAdmin = { role: "admin", player_id: "organizer" };
const captain = { role: "captain", team_captain: 1, association: "UA", player_id: "captain" };

async function createContext(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve, reject) => db.close((error) => error ? reject(error) : resolve())));
  const run = (sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, function (error) {
    if (error) reject(error); else resolve(this);
  }));
  const all = (sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows)));
  const get = async (sql, params = []) => (await all(sql, params))[0] || null;
  const schema = (name) => source.match(new RegExp(`CREATE TABLE IF NOT EXISTS ${name} \\([\\s\\S]*?\\n    \\)`))[0];
  await run(source.match(/CREATE TABLE matches \([\s\S]*?\n        \);/)[0]);
  for (const name of ["duels", "games", "standings"]) await run(schema(name));
  await run(`CREATE TABLE tournaments (
    id TEXT PRIMARY KEY, standings_scoring TEXT DEFAULT 'standard',
    tpr_target_games INTEGER, tpr_smoothing REAL, tpr_benchmark_percentile REAL
  )`);
  await run("CREATE TABLE duel_formats (format TEXT, games_to_win INTEGER, minutes_to_play INTEGER)");
  await run("INSERT INTO tournaments (id) VALUES ('T')");
  await run("INSERT INTO duel_formats VALUES ('Bo1', 1, 60)");
  await run(`INSERT INTO matches (id, tournament_id, team_1, team_2, status, dw1, dw2, gw1, gw2, stage, lineup_type, number_of_duels, time_utc)
    VALUES ('20261005UAFR', 'T', 'UA', 'FR', 'Done', 1, 0, 1, 0, 'Stage 1', 'Open', 1, '2026-10-05T12:00:00.000Z')`);
  await run(`INSERT INTO duels (id, tournament_id, match_id, duel_format, time_utc, player_1_id, player_2_id, status, dw1, dw2, ranking)
    VALUES ('duel', 'T', '20261005UAFR', 'Bo1', ?, 'p1', 'p2', 'Done', 1, 0, 0)`, [new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString()]);
  await run(`INSERT INTO games (id, duel_id, bga_table_id, game_number, player_1_score, player_2_score, player_1_rank, player_2_rank, status)
    VALUES ('game', 'duel', '123456789', 1, 100, 90, 1, 0, 'Done')`);
  for (const stage of ["Stage 1", "Stage 2"]) {
    for (const team of ["UA", "FR"]) {
      await run('INSERT INTO standings (tournament_id, stage, "group", team_id) VALUES (?, ?, ?, ?)', ["T", stage, "A", team]);
    }
    for (const player of ["p1", "p2"]) {
      await run('INSERT INTO standings (tournament_id, stage, "group", player_id) VALUES (?, ?, ?, ?)', ["T", stage, "A", player]);
    }
  }
  const handlers = {};
  const dependencies = {
    db, dbRunAsync: run, dbAllAsync: all, dbGetAsync: get,
    app: {
      post: (path, handler) => { handlers[path] = handler; },
      patch: (path, handler) => { handlers[path] = handler; },
    },
    TOURNAMENT_ACCESS_TYPES: { OFFICIAL: "Official", FRIENDLY: "Friendly" },
    TOURNAMENT_ACCESS_ROLES: { ADMIN: "admin", CAPTAIN: "captain" },
    STANDINGS_SCORING: { STANDARD: "standard", BOUNTY_TPR: "bounty_tpr" },
    loadTournamentAccessForUser(id, user, done) {
      done(null, {
        id, subtype: "Official", has_access: !!(user.admin || user.role),
        access_role: user.admin ? "admin" : user.role, captain_team_ids: ["UA"],
      });
    },
    // Ratings and response enrichment are independent of result aggregation and standings.
    recomputeDuelRatingsForMatch: async () => ({ matchRating: null }),
    updateMatchGgRating: async () => {},
    loadDuelsByIds: (ids, done) => db.all("SELECT * FROM duels WHERE id = ?", ids, done),
    isCompletedRankedDuel: () => false,
    isBlindLineupType: (value) => String(value).toLowerCase() === "blind",
    MATCH_AUDIT_FIELDS: [],
    buildAuditChanges: () => ({}),
    console,
  };
  const functions = new Function(...Object.keys(dependencies), `${helpers}\n${routes}\nreturn { recomputeMatchAggregates, recalculateTournamentStandings };`)(...Object.values(dependencies));
  await functions.recalculateTournamentStandings("T");

  const request = (path, user, body, id) => new Promise((resolve, reject) => {
    let status = 200;
    try {
      handlers[path]({ user, params: { id }, body }, {
        status(value) { status = value; return this; },
        json(value) { resolve({ status, ...value }); },
      });
    } catch (error) { reject(error); }
  });
  return {
    run, get, all, ...functions,
    standings: (stage = "Stage 1") => all("SELECT * FROM standings WHERE stage = ? ORDER BY id", [stage]),
    saveGames: (user, ranks = [0, 1]) => request("/duels/:id/games/save", user, {
      games: [{ id: "game", game_number: 1, bga_table_id: "123456789", player_1_score: 100, player_2_score: 90,
        player_1_rank: ranks[0], player_2_rank: ranks[1] }],
    }, "duel"),
    updateMatch: (payload) => request("/matches/:id", tournamentAdmin, {
      id: "20261005UAFR", team_1: "UA", team_2: "FR", time_utc: "2026-10-05T12:00:00.000Z", lineup_type: "Open",
      number_of_duels: 1, status: "Done", stage: "Stage 1", ...payload,
    }, "20261005UAFR"),
  };
}

function assertWinner(rows, team, player) {
  for (const row of rows) {
    const won = row.team_id ? row.team_id === team : row.player_id === player;
    assert.equal(row.mp, 1);
    assert.equal(row.mw, won ? 1 : 0);
    assert.equal(row.ml, won ? 0 : 1);
    assert.equal(row.gw, won ? 1 : 0);
    assert.equal(row.gl, won ? 0 : 1);
    assert.equal(row.mdif, won ? 1 : -1);
    assert.equal(row.gdif, won ? 1 : -1);
    assert.equal(row.position, won ? 1 : 2);
    if (row.team_id) {
      assert.equal(row.dw, won ? 1 : 0);
      assert.equal(row.dl, won ? 0 : 1);
      assert.equal(row.ddif, won ? 1 : -1);
    }
  }
}

for (const [role, user] of [["global admin", globalAdmin], ["tournament admin", tournamentAdmin], ["captain", captain]]) {
  test(`${role} result edits refresh team and player standings before returning success`, async (t) => {
    const ctx = await createContext(t);
    assertWinner(await ctx.standings(), "UA", "p1");
    const untouchedStage = await ctx.standings("Stage 2");
    const result = await ctx.saveGames(user);
    assert.equal(result.status, 200);
    assert.equal(result.ok, true);
    assert.equal(result.match.status, "Done");
    assert.equal(result.match.dw1, 0);
    assert.equal(result.match.dw2, 1);
    assertWinner(await ctx.standings(), "FR", "p2");
    assert.deepEqual(await ctx.standings("Stage 2"), untouchedStage);
    assert.equal((await ctx.saveGames(user)).ok, true);
    assertWinner(await ctx.standings(), "FR", "p2");
    assert.equal((await ctx.saveGames(user, [1, 0])).ok, true);
    assertWinner(await ctx.standings(), "UA", "p1");
  });
}

test("reopening a completed match removes its standings contribution and completing it restores it", async (t) => {
  const ctx = await createContext(t);
  const result = await ctx.saveGames(captain, [0, 0]);
  assert.equal(result.ok, true);
  assert.equal(result.match.status, "Error");
  for (const row of await ctx.standings()) {
    for (const metric of ["mp", "mw", "ml", "gw", "gl", "mdif", "gdif"]) assert.equal(row[metric], 0);
  }
  assert.equal((await ctx.saveGames(captain)).ok, true);
  assertWinner(await ctx.standings(), "FR", "p2");
});

test("direct match edits recalculate scores, stage membership and reopened matches", async (t) => {
  const ctx = await createContext(t);
  const updated = await ctx.updateMatch({ dw1: 0, dw2: 1, gw1: 2, gw2: 3 });
  assert.equal(updated.ok, true, updated.message);
  let rows = await ctx.standings();
  const team1 = rows.find((row) => row.team_id === "UA");
  const team2 = rows.find((row) => row.team_id === "FR");
  assert.equal(team1.mw, 0);
  assert.equal(team1.gw, 2);
  assert.equal(team1.gl, 3);
  assert.equal(team1.position, 2);
  assert.equal(team2.mw, 1);
  assert.equal(team2.position, 1);
  assert.equal((await ctx.updateMatch({ stage: "Stage 2", dw1: 1, dw2: 0, gw1: 1, gw2: 0 })).ok, true);
  assert.ok((await ctx.standings()).every((row) => row.mp === 0));
  assertWinner(await ctx.standings("Stage 2"), "UA", "p1");
  assert.equal((await ctx.updateMatch({ stage: "Stage 2", status: "Planned" })).ok, true);
  assert.ok((await ctx.standings("Stage 2")).every((row) => row.mp === 0));
  assert.equal((await ctx.updateMatch({ dw1: 1, dw2: 0, gw1: 1, gw2: 0 })).ok, true);
  assertWinner(await ctx.standings(), "UA", "p1");
});

test("unresolved match aggregation does not recalculate standings", async (t) => {
  const ctx = await createContext(t);
  await ctx.run("UPDATE matches SET status = 'Planned'");
  await ctx.run("UPDATE duels SET status = 'Planned', dw1 = NULL, dw2 = NULL");
  const before = await ctx.standings();
  await ctx.recomputeMatchAggregates("20261005UAFR");
  assert.deepEqual(await ctx.standings(), before);
});

test("failed standings recalculation rolls back saved games, duel and match results", async (t) => {
  const ctx = await createContext(t);
  await ctx.run("CREATE TRIGGER fail_standings BEFORE UPDATE ON standings BEGIN SELECT RAISE(ABORT, 'standings unavailable'); END");
  const before = await ctx.standings();
  const result = await ctx.saveGames(captain);
  assert.equal(result.status, 500);
  assert.equal(result.ok, false);
  assert.equal((await ctx.get("SELECT player_1_rank FROM games WHERE id = 'game'")).player_1_rank, 1);
  assert.equal((await ctx.get("SELECT dw1 FROM duels WHERE id = 'duel'")).dw1, 1);
  assert.equal((await ctx.get("SELECT dw1 FROM matches WHERE id = '20261005UAFR'")).dw1, 1);
  assert.deepEqual(await ctx.standings(), before);
});
