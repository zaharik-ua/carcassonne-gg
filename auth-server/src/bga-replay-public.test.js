import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import { loadPublicGamesByDuelIds } from "./bga-replay-public.js";
import { ensureGameReplaysSchema } from "./bga-replay-schema.js";

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

function loadGames(db, duelIds) {
  return new Promise((resolve, reject) => {
    loadPublicGamesByDuelIds(db, duelIds, (error, rows) => {
      if (error) reject(error);
      else resolve(rows || []);
    });
  });
}

function close(db) {
  return new Promise((resolve, reject) => {
    db.close((error) => (error ? reject(error) : resolve()));
  });
}

test("ready fallback replay is immediately exposed by the public games query", async (t) => {
  const db = new sqlite3.Database(":memory:");
  t.after(() => close(db));
  await exec(db, `
    CREATE TABLE games (
      id TEXT PRIMARY KEY,
      duel_id TEXT,
      bga_table_id TEXT,
      game_number INTEGER,
      player_1_score INTEGER,
      player_2_score INTEGER,
      player_1_rank INTEGER,
      player_2_rank INTEGER,
      player_1_clock INTEGER,
      player_2_clock INTEGER,
      status TEXT,
      deleted_at TEXT
    );
  `);
  await ensureGameReplaysSchema(db);
  await exec(db, `
    INSERT INTO games (
      id, duel_id, bga_table_id, game_number, status, deleted_at
    ) VALUES
      ('fallback-ready', 'duel-1', '101', 1, 'Done', NULL),
      ('fallback-pending', 'duel-1', '102', 2, 'Done', NULL);

    INSERT INTO game_replays (
      game_id, bga_table_id, status, color_source, carcassonne_lab_url,
      board_stats_json, retry_reason, queue_class
    ) VALUES
      (
        'fallback-ready', '101', 'ready', 'fallback',
        'https://www.carcassonnelab.com/#/fallback', '{"width":4,"height":5}',
        'colors', 'fresh'
      ),
      (
        'fallback-pending', '102', 'pending', 'fallback',
        'https://www.carcassonnelab.com/#/not-public-yet', '{"width":2,"height":2}',
        'initial', 'fresh'
      );
  `);

  const rows = await loadGames(db, ["duel-1", "duel-1", ""]);

  assert.equal(rows.length, 2);
  assert.equal(
    rows[0].carcassonne_lab_url,
    "https://www.carcassonnelab.com/#/fallback"
  );
  assert.equal(rows[0].board_stats_json, '{"width":4,"height":5}');
  assert.equal(rows[1].carcassonne_lab_url, null);
  assert.equal(rows[1].board_stats_json, null);
});
