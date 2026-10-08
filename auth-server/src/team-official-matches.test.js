import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import sqlite3 from "sqlite3";
import { loadPublicGamesByDuelIds } from "./bga-replay-public.js";

const source = readFileSync(new URL("./server.js", import.meta.url), "utf8");
function between(start, end) {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from);
  assert.ok(from >= 0 && to > from);
  return source.slice(from, to);
}
const route = between('app.get("/public/team-official-matches",', 'app.get("/public/team-official-matches/filters",');
const streamsHelper = between("function loadStreamsByMatchIds(", "function loadStreamsByDuelIds(");

async function createApi(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise(resolve => db.close(resolve)));
  await new Promise((resolve, reject) => db.exec(`
    CREATE TABLE tournaments (
      id TEXT, name TEXT, short_title TEXT, logo TEXT, link TEXT,
      tournament_type TEXT, subtype TEXT, access_type TEXT
    );
    CREATE TABLE teams (id TEXT, name TEXT, flag TEXT, logo TEXT);
    CREATE TABLE matches (
      id TEXT, tournament_id TEXT, time_utc TEXT, lineup_type TEXT,
      lineup_deadline_h INTEGER, lineup_deadline_utc TEXT, number_of_duels INTEGER,
      team_1 TEXT, team_2 TEXT, status TEXT, dw1 INTEGER, dw2 INTEGER,
      gw1 INTEGER, gw2 INTEGER, rating INTEGER, deleted_at TEXT
    );
    CREATE TABLE profiles (
      id TEXT, bga_nickname TEXT, name TEXT, avatar TEXT, association TEXT,
      bga_elo INTEGER, gg_elo INTEGER, gg_rating_position INTEGER
    );
    CREATE TABLE duels (
      id TEXT, tournament_id TEXT, match_id TEXT, duel_number INTEGER,
      duel_format TEXT, time_utc TEXT, player_1_id TEXT, player_2_id TEXT,
      dw1 INTEGER, dw2 INTEGER, rating INTEGER, status TEXT, deleted_at TEXT
    );
    CREATE TABLE streams (
      id INTEGER, entity_type TEXT, entity_id TEXT, streamer_id INTEGER,
      link TEXT, created_at TEXT, updated_at TEXT, deleted_at TEXT
    );
    CREATE TABLE streamers (
      id INTEGER, profile_id TEXT, name TEXT, short_name TEXT,
      avatar TEXT, scoreboard_style TEXT, deleted_at TEXT
    );
    CREATE TABLE games (
      id TEXT, duel_id TEXT, bga_table_id TEXT, game_number INTEGER,
      player_1_score INTEGER, player_2_score INTEGER,
      player_1_rank INTEGER, player_2_rank INTEGER,
      player_1_clock INTEGER, player_2_clock INTEGER, status TEXT, deleted_at TEXT
    );
    CREATE TABLE game_replays (
      game_id TEXT, status TEXT, carcassonne_lab_url TEXT,
      board_stats_json TEXT, meeple_stats_json TEXT,
      scoring_json TEXT, player_time_json TEXT
    );
    INSERT INTO tournaments (id, name, tournament_type, subtype) VALUES
      ('CUP', 'Official Cup', 'Teams', 'Official'),
      ('FRIENDLY', 'Friendly', 'Teams', 'Friendly');
    INSERT INTO teams VALUES
      ('JP', 'Japan', 'japan.svg', NULL), ('CN', 'China', NULL, 'china.svg');
    INSERT INTO profiles VALUES
      ('101', 'Akira', 'Akira Tanaka', '_def_1', 'JP', 640, 1750, 12),
      ('102', 'Mei', 'Mei Lin', '_def_2', 'CN', 590, 1720, 19);
    INSERT INTO matches (id, tournament_id, time_utc, team_1, team_2, status) VALUES
      ('upcoming', 'CUP', '2099-01-01T12:00:00Z', 'JP', 'CN', 'Planned'),
      ('completed', 'CUP', '2025-01-01T12:00:00Z', 'JP', 'CN', 'Done'),
      ('older', 'CUP', '2024-01-01T12:00:00Z', 'JP', 'CN', 'Done'),
      ('friendly', 'FRIENDLY', '2099-01-02T12:00:00Z', 'JP', 'CN', 'Planned');
    INSERT INTO duels (id, tournament_id, match_id, duel_number, duel_format,
      player_1_id, player_2_id, dw1, dw2, status) VALUES
      ('duel', 'CUP', 'completed', 1, 'Bo3', '101', '102', 2, 0, 'Done');
    INSERT INTO streamers (id, name, avatar, deleted_at) VALUES
      (1, 'Streamer', 'streamer.png', NULL),
      (2, 'No avatar', NULL, NULL),
      (3, 'Deleted', 'deleted.png', '2025-01-01');
    INSERT INTO streams (id, entity_type, entity_id, streamer_id, link, deleted_at) VALUES
      (1, 'match', 'upcoming', 1, 'https://example.com/upcoming', NULL),
      (2, 'match', 'completed', 1, 'https://example.com/completed', NULL),
      (3, 'match', 'completed', 2, 'https://example.com/no-avatar', NULL),
      (4, 'match', 'completed', 1, 'https://example.com/deleted-stream', '2025-01-01'),
      (5, 'match', 'completed', 3, 'https://example.com/deleted-streamer', NULL),
      (6, 'match', 'older', 1, 'https://example.com/older', NULL),
      (7, 'duel', 'completed', 1, 'https://example.com/duel-only', NULL);
    INSERT INTO games (id, duel_id, bga_table_id, game_number, player_1_score, player_2_score) VALUES
      ('game1', 'duel', '1234567890', 1, 99, 88),
      ('game2', 'duel', '1234567891', 2, 95, 80);
    INSERT INTO game_replays (game_id, status, carcassonne_lab_url) VALUES
      ('game1', 'ready', 'https://www.carcassonnelab.com/#/game/first'),
      ('game2', 'pending', 'https://www.carcassonnelab.com/#/game/pending');
  `, error => error ? reject(error) : resolve()));

  const dbAllAsync = (sql, params) => new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows));
  });
  const dbGetAsync = (sql, params) => new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => error ? reject(error) : resolve(row));
  });
  let handler;
  new Function("app", "db", "dbAllAsync", "dbGetAsync", "loadGamesByDuelIds", `${streamsHelper}\n${route}`)(
    { get: (_path, callback) => { handler = callback; } }, db, dbAllAsync, dbGetAsync,
    (ids, callback) => loadPublicGamesByDuelIds(db, ids, callback)
  );
  return (query = {}) => new Promise((resolve, reject) => {
    handler({ query }, { json: resolve }, reject);
  });
}

test("official team feed returns aligned match streams and excludes deleted or duel streams", async t => {
  const request = await createApi(t);
  const result = await request();
  assert.deepEqual(result.matches.map(match => match.id), ["upcoming", "completed", "older"]);
  const match = result.matches.find(entry => entry.id === "completed");
  assert.deepEqual(match.streams, ["https://example.com/completed", "https://example.com/no-avatar"]);
  assert.deepEqual(match.streamers, ["streamer.png", null]);
  assert.deepEqual(result.matches[0].streams, ["https://example.com/upcoming"]);
});

test("official duel details include both profiles and only ready Lab replay links", async t => {
  const request = await createApi(t);
  const { duels } = await request();
  const duel = duels[0];
  assert.equal(duel.player_1_real_name, "Akira Tanaka");
  assert.equal(duel.player_1_avatar, "_def_1");
  assert.equal(duel.player_1_elo, 640);
  assert.equal(duel.player_1_gg_elo, 1750);
  assert.equal(duel.player_1_gg_rank, 12);
  assert.equal(duel.player_1_association_name, "Japan");
  assert.equal(duel.player_1_association_flag, "japan.svg");
  assert.equal(duel.player_2_real_name, "Mei Lin");
  assert.equal(duel.player_2_avatar, "_def_2");
  assert.equal(duel.player_2_elo, 590);
  assert.equal(duel.player_2_gg_elo, 1720);
  assert.equal(duel.player_2_gg_rank, 19);
  assert.equal(duel.player_2_association_name, "China");
  assert.equal(duel.player_2_association_flag, "china.svg");
  assert.equal(duel.games[0].carcassonne_lab_url, "https://www.carcassonnelab.com/#/game/first");
  assert.equal(duel.games[1].carcassonne_lab_url, null);
});

test("pagination and empty filters restrict stream data to returned matches", async t => {
  const request = await createApi(t);
  const query = { results_page_size: "1", results_before: "2090-01-01T00:00:00Z" };
  const firstPage = await request(query);
  assert.deepEqual(firstPage.matches.map(match => match.id), ["upcoming", "completed"]);
  assert.equal(firstPage.pagination.results.total, 2);
  const secondPage = await request({ ...query, results_page: "2" });
  assert.deepEqual(secondPage.matches.map(match => match.id), ["upcoming", "older"]);
  assert.deepEqual(secondPage.matches[1].streams, ["https://example.com/older"]);
  assert.deepEqual(secondPage.duels, []);
  const empty = await request({ tournament: "MISSING" });
  assert.deepEqual(empty.matches, []);
  assert.deepEqual(empty.duels, []);
});
