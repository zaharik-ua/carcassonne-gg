import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import { ensureTournamentCasesSchema } from "./tournament-cases.js";

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

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    PRAGMA foreign_keys = ON;
    CREATE TABLE users (id INTEGER PRIMARY KEY);
    CREATE TABLE profiles (id TEXT PRIMARY KEY);
    CREATE TABLE matches (id TEXT PRIMARY KEY);
    CREATE TABLE duels (id TEXT PRIMARY KEY);
    CREATE TABLE tournaments (id TEXT PRIMARY KEY);
    CREATE TABLE challenge_periods (id TEXT PRIMARY KEY);
  `);
  await ensureTournamentCasesSchema(db);
  return db;
}

test("creates tournament_cases with workflow and relation fields", async (t) => {
  const db = await createDatabase(t);
  const columns = await all(db, "PRAGMA table_info(tournament_cases)");
  const names = new Set(columns.map((column) => column.name));

  [
    "case_type",
    "status",
    "priority",
    "subject",
    "details",
    "submitted_by_player_id",
    "responsible_user_id",
    "reported_player_id",
    "match_id",
    "duel_id",
    "tournament_id",
    "challenge_period_id",
    "related_entity_type",
    "related_entity_id",
    "resolution",
  ].forEach((name) => assert.equal(names.has(name), true, `missing ${name}`));
});

test("accepts a Challenge no-show complaint and enforces workflow values", async (t) => {
  const db = await createDatabase(t);
  await run(db, "INSERT INTO users (id) VALUES (1)");
  await run(db, "INSERT INTO profiles (id) VALUES ('reporter'), ('missing-player')");
  await run(db, "INSERT INTO duels (id) VALUES ('duel-1')");
  await run(db, "INSERT INTO tournaments (id) VALUES ('rivals-1')");
  await run(db, "INSERT INTO challenge_periods (id) VALUES ('period-1')");

  await run(
    db,
    `
      INSERT INTO tournament_cases (
        case_type, category, status, priority, subject, details,
        submitted_by_user_id, submitted_by_player_id, reported_player_id,
        duel_id, tournament_id, challenge_period_id
      )
      VALUES ('complaint', 'no_show', 'open', 'normal', ?, ?, 1, 'reporter', 'missing-player', 'duel-1', 'rivals-1', 'period-1')
    `,
    ["No-show report", "The player did not show up."]
  );

  const rows = await all(db, "SELECT * FROM tournament_cases");
  assert.equal(rows.length, 1);
  assert.equal(rows[0].status, "open");
  assert.equal(rows[0].reported_player_id, "missing-player");

  await assert.rejects(
    run(
      db,
      "INSERT INTO tournament_cases (case_type, status, priority, subject, submitted_by_player_id) VALUES ('complaint', 'waiting', 'normal', 'Invalid', 'reporter')"
    ),
    /CHECK constraint failed/
  );
});
