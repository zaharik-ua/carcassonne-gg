import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import {
  ensureGameReplaysSchema,
  inferExistingReplayColorSource,
} from "./bga-replay-schema.js";

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

function all(db, sql) {
  return new Promise((resolve, reject) => {
    db.all(sql, (error, rows) => {
      if (error) reject(error);
      else resolve(rows || []);
    });
  });
}

function close(db) {
  return new Promise((resolve, reject) => {
    db.close((error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

test("replay schema migration is repeatable and does not enqueue legacy rows", async (t) => {
  const db = new sqlite3.Database(":memory:");
  t.after(() => close(db));
  await exec(db, `
    CREATE TABLE games (id TEXT PRIMARY KEY);
    CREATE TABLE game_replays (
      game_id TEXT PRIMARY KEY,
      bga_table_id TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      logs_json TEXT,
      events_json TEXT,
      players_json TEXT,
      event_count INTEGER NOT NULL DEFAULT 0,
      tile_count INTEGER NOT NULL DEFAULT 0,
      meeple_count INTEGER NOT NULL DEFAULT 0,
      archive_requested INTEGER NOT NULL DEFAULT 0,
      fetched_at TEXT,
      last_attempt_at TEXT,
      last_error TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    INSERT INTO game_replays (
      game_id, bga_table_id, status, players_json, events_json, archive_requested
    ) VALUES
      (
        'ready-bga',
        '1001',
        'ready',
        '[{"player_id":"1","color_hex":"ff0000"},{"player_id":"2","color_hex":"0000ff"}]',
        '[{"type":"playTile","player_id":"1"},{"type":"playTile","player_id":"2"}]',
        1
      ),
      (
        'ready-fallback',
        '1002',
        'ready',
        '[{"player_id":"1","color_hex":"ff0000"},{"player_id":"2","color_hex":null}]',
        '[{"type":"playTile","player_id":"1"},{"type":"playTile","player_id":"2"}]',
        0
      ),
      ('old-error', '1003', 'error', NULL, NULL, 0);
  `);

  await ensureGameReplaysSchema(db);
  await ensureGameReplaysSchema(db);

  const columns = new Set((await all(db, "PRAGMA table_info(game_replays)")).map((row) => row.name));
  for (const columnName of [
    "next_attempt_at",
    "retry_reason",
    "queue_class",
    "queued_at",
    "historical_batch_id",
    "history_request_count",
    "color_refresh_count",
    "color_source",
    "archive_requested_at",
    "last_account_label",
    "lease_owner",
    "lease_until",
  ]) {
    assert.ok(columns.has(columnName), `missing ${columnName}`);
  }
  for (const legacyColumn of [
    "logs_json",
    "event_count",
    "tile_count",
    "meeple_count",
    "archive_requested",
  ]) {
    assert.ok(!columns.has(legacyColumn), `legacy column remains: ${legacyColumn}`);
  }

  assert.deepEqual(
    await all(
      db,
      `
        SELECT game_id, status, color_source, queue_class, retry_reason,
               next_attempt_at, history_request_count, color_refresh_count,
               archive_requested_at IS NOT NULL AS has_archive_requested_at
        FROM game_replays
        ORDER BY game_id
      `
    ),
    [
      {
        game_id: "old-error",
        status: "error",
        color_source: null,
        queue_class: "fresh",
        retry_reason: null,
        next_attempt_at: null,
        history_request_count: 0,
        color_refresh_count: 0,
        has_archive_requested_at: 0,
      },
      {
        game_id: "ready-bga",
        status: "ready",
        color_source: "bga",
        queue_class: "fresh",
        retry_reason: null,
        next_attempt_at: null,
        history_request_count: 0,
        color_refresh_count: 0,
        has_archive_requested_at: 1,
      },
      {
        game_id: "ready-fallback",
        status: "ready",
        color_source: "fallback",
        queue_class: "fresh",
        retry_reason: null,
        next_attempt_at: null,
        history_request_count: 0,
        color_refresh_count: 0,
        has_archive_requested_at: 0,
      },
    ]
  );

  const tables = new Set(
    (await all(db, "SELECT name FROM sqlite_master WHERE type = 'table'")).map((row) => row.name)
  );
  assert.ok(tables.has("bga_replay_requests"));
  assert.ok(tables.has("bga_replay_account_state"));
  assert.ok(tables.has("bga_replay_budget_overrides"));

  const replayIndexes = new Set(
    (await all(db, "PRAGMA index_list(game_replays)")).map((row) => row.name)
  );
  assert.ok(replayIndexes.has("idx_game_replays_due"));
  const requestIndexes = new Set(
    (await all(db, "PRAGMA index_list(bga_replay_requests)")).map((row) => row.name)
  );
  assert.ok(requestIndexes.has("idx_bga_replay_requests_budget"));
  assert.ok(requestIndexes.has("idx_bga_replay_requests_class_budget"));
});

test("legacy color provenance requires two distinct supported BGA colors", () => {
  const events = JSON.stringify([
    { type: "playTile", player_id: "1" },
    { type: "playTile", player_id: "2" },
  ]);
  assert.equal(
    inferExistingReplayColorSource(
      JSON.stringify([
        { player_id: "1", color_hex: "#ff0000" },
        { player_id: "2", color_hex: "0000FF" },
      ]),
      events
    ),
    "bga"
  );
  assert.equal(
    inferExistingReplayColorSource(
      JSON.stringify([
        { player_id: "1", color_hex: "ff0000" },
        { player_id: "2", color_hex: "ff0000" },
      ]),
      events
    ),
    "fallback"
  );
});
