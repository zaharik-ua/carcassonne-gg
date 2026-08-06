import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import sqlite3 from "sqlite3";
import {
  SECRET_LINEUP_SIZE,
  ensureSecretLineupsSchema,
  publishDueSecretLineups,
  publishSecretLineupMatch,
} from "./secret-lineups.js";

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) reject(error);
      else resolve(this);
    });
  });
}

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

async function createDatabase(t) {
  const directory = await mkdtemp(path.join(os.tmpdir(), "secret-lineups-"));
  const db = new sqlite3.Database(path.join(directory, "test.sqlite"));
  db.configure("busyTimeout", 5000);
  t.after(async () => {
    await new Promise((resolve) => db.close(() => resolve()));
    await rm(directory, { recursive: true, force: true });
  });
  await exec(db, `
    CREATE TABLE profiles (
      id TEXT PRIMARY KEY,
      deleted_at TEXT
    );
    CREATE TABLE matches (
      id TEXT PRIMARY KEY,
      tournament_id TEXT,
      is_test INTEGER NOT NULL DEFAULT 0,
      time_utc TEXT,
      lineup_type TEXT,
      lineup_deadline_h INTEGER,
      lineup_deadline_utc TEXT,
      team_1 TEXT,
      team_2 TEXT,
      status TEXT,
      updated_by TEXT,
      deleted_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE duels (
      id TEXT PRIMARY KEY,
      tournament_id TEXT,
      match_id TEXT,
      is_test INTEGER NOT NULL DEFAULT 0,
      duel_number INTEGER,
      duel_format TEXT,
      time_utc TEXT,
      custom_time INTEGER,
      player_1_id TEXT,
      player_2_id TEXT,
      dw1 INTEGER,
      dw2 INTEGER,
      status TEXT,
      created_by TEXT,
      updated_by TEXT,
      deleted_by TEXT,
      deleted_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
  `);
  await ensureSecretLineupsSchema(db);
  return db;
}

async function seedMatch(db, matchId, deadlineUtc) {
  await run(
    db,
    `
      INSERT INTO matches (
        id, tournament_id, time_utc, lineup_type, lineup_deadline_utc,
        team_1, team_2, status
      )
      VALUES (?, 'TOURNAMENT', ?, 'Blind', ?, 'AAA', 'BBB', 'Planned')
    `,
    [matchId, new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(), deadlineUtc]
  );
}

async function seedLineup(db, matchId, teamId, playerPrefix) {
  await run(
    db,
    `
      INSERT INTO match_lineup_submissions (match_id, team_id, submitted_by)
      VALUES (?, ?, ?)
    `,
    [matchId, teamId, `${playerPrefix}-captain`]
  );
  for (let position = 1; position <= SECRET_LINEUP_SIZE; position += 1) {
    const playerId = `${playerPrefix}-${position}`;
    await run(db, "INSERT INTO profiles (id) VALUES (?)", [playerId]);
    await run(
      db,
      `
        INSERT INTO match_lineup_entries (match_id, team_id, position, player_id)
        VALUES (?, ?, ?, ?)
      `,
      [matchId, teamId, position, playerId]
    );
  }
}

test("normalizes legacy Secret lineup type to Blind", async (t) => {
  const db = await createDatabase(t);
  const matchId = "legacy-secret-match";
  await seedMatch(db, matchId, new Date(Date.now() + 60 * 60 * 1000).toISOString());
  await run(
    db,
    "UPDATE matches SET lineup_type = 'Secret', lineup_deadline_utc = NULL WHERE id = ?",
    [matchId]
  );

  await ensureSecretLineupsSchema(db);
  const match = await get(
    db,
    "SELECT lineup_type, lineup_deadline_h, lineup_deadline_utc, time_utc FROM matches WHERE id = ?",
    [matchId]
  );

  assert.equal(match.lineup_type, "Blind");
  assert.equal(match.lineup_deadline_h, 24);
  assert.equal(
    Math.round((Date.parse(match.time_utc) - Date.parse(match.lineup_deadline_utc)) / (60 * 60 * 1000)),
    24
  );
});

test("keeps both submitted lineups private before the deadline", async (t) => {
  const db = await createDatabase(t);
  const matchId = "future-match";
  await seedMatch(db, matchId, new Date(Date.now() + 60 * 60 * 1000).toISOString());
  await seedLineup(db, matchId, "AAA", "a");
  await seedLineup(db, matchId, "BBB", "b");

  const result = await publishSecretLineupMatch(db, matchId);
  const duelCount = await get(db, "SELECT COUNT(*) AS count FROM duels WHERE match_id = ?", [matchId]);
  const match = await get(db, "SELECT lineups_published_at FROM matches WHERE id = ?", [matchId]);

  assert.equal(result.reason, "deadline_not_reached");
  assert.equal(duelCount.count, 0);
  assert.equal(match.lineups_published_at, null);
});

test("does not run the private publisher for Open lineups", async (t) => {
  const db = await createDatabase(t);
  const matchId = "open-match";
  await seedMatch(db, matchId, new Date(Date.now() - 60 * 1000).toISOString());
  await run(db, "UPDATE matches SET lineup_type = 'Open' WHERE id = ?", [matchId]);
  await seedLineup(db, matchId, "AAA", "a");
  await seedLineup(db, matchId, "BBB", "b");

  const result = await publishSecretLineupMatch(db, matchId);
  const duelCount = await get(db, "SELECT COUNT(*) AS count FROM duels WHERE match_id = ?", [matchId]);

  assert.equal(result.reason, "not_blind");
  assert.equal(duelCount.count, 0);
});

test("publishes five paired duels once when both due lineups exist", async (t) => {
  const db = await createDatabase(t);
  const matchId = "due-match";
  await seedMatch(db, matchId, new Date(Date.now() - 60 * 1000).toISOString());
  await seedLineup(db, matchId, "AAA", "a");
  await seedLineup(db, matchId, "BBB", "b");

  const [result] = await publishDueSecretLineups(db);
  const duels = await all(
    db,
    "SELECT duel_number, player_1_id, player_2_id FROM duels WHERE match_id = ? ORDER BY duel_number",
    [matchId]
  );
  const match = await get(db, "SELECT lineups_published_at FROM matches WHERE id = ?", [matchId]);
  const secondResult = await publishSecretLineupMatch(db, matchId);
  const duelCount = await get(db, "SELECT COUNT(*) AS count FROM duels WHERE match_id = ?", [matchId]);

  assert.equal(result.published, true);
  assert.ok(match.lineups_published_at);
  assert.deepEqual(duels, Array.from({ length: SECRET_LINEUP_SIZE }, (_, index) => ({
    duel_number: index + 1,
    player_1_id: `a-${index + 1}`,
    player_2_id: `b-${index + 1}`,
  })));
  assert.equal(secondResult.reason, "already_published");
  assert.equal(duelCount.count, SECRET_LINEUP_SIZE);
});

test("waits for a missing lineup after the deadline and publishes as soon as it arrives", async (t) => {
  const db = await createDatabase(t);
  const matchId = "late-match";
  await seedMatch(db, matchId, new Date(Date.now() - 60 * 1000).toISOString());
  await seedLineup(db, matchId, "AAA", "a");

  const waitingResult = await publishSecretLineupMatch(db, matchId);
  const privateDuelCount = await get(db, "SELECT COUNT(*) AS count FROM duels WHERE match_id = ?", [matchId]);

  assert.equal(waitingResult.reason, "waiting_for_lineup");
  assert.equal(privateDuelCount.count, 0);

  await seedLineup(db, matchId, "BBB", "b");
  const publishedResult = await publishSecretLineupMatch(db, matchId);
  const publishedDuelCount = await get(db, "SELECT COUNT(*) AS count FROM duels WHERE match_id = ?", [matchId]);

  assert.equal(publishedResult.published, true);
  assert.equal(publishedDuelCount.count, SECRET_LINEUP_SIZE);
});
