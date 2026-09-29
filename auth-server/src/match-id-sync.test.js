import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import { ensureMatchIdReferenceSync } from "./match-id-sync.js";

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => error ? reject(error) : resolve());
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows));
  });
}

test("renaming a match keeps every linked record attached", async (t) => {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve, reject) => {
    db.close((error) => error ? reject(error) : resolve());
  }));

  await exec(db, `
    PRAGMA foreign_keys = ON;

    CREATE TABLE matches (id TEXT PRIMARY KEY);
    CREATE TABLE duels (id TEXT PRIMARY KEY, match_id TEXT);
    CREATE TABLE match_lineup_submissions (
      match_id TEXT NOT NULL,
      team_id TEXT NOT NULL,
      PRIMARY KEY (match_id, team_id),
      FOREIGN KEY (match_id) REFERENCES matches(id) ON UPDATE CASCADE
    );
    CREATE TABLE match_lineup_entries (
      match_id TEXT NOT NULL,
      team_id TEXT NOT NULL,
      position INTEGER NOT NULL,
      PRIMARY KEY (match_id, team_id, position),
      FOREIGN KEY (match_id, team_id)
        REFERENCES match_lineup_submissions(match_id, team_id)
        ON UPDATE CASCADE
    );
    CREATE TABLE news (id INTEGER PRIMARY KEY, match_id TEXT);
    CREATE TABLE streams (id INTEGER PRIMARY KEY, entity_type TEXT, entity_id TEXT);
    CREATE TABLE tournament_cases (
      id INTEGER PRIMARY KEY,
      match_id TEXT REFERENCES matches(id),
      related_entity_type TEXT,
      related_entity_id TEXT
    );

    INSERT INTO matches (id) VALUES ('old-match-id');
    INSERT INTO duels (id, match_id) VALUES ('duel-1', 'old-match-id');
    INSERT INTO match_lineup_submissions (match_id, team_id) VALUES ('old-match-id', 'UKR');
    INSERT INTO match_lineup_entries (match_id, team_id, position) VALUES ('old-match-id', 'UKR', 1);
    INSERT INTO news (id, match_id) VALUES (1, 'old-match-id');
    INSERT INTO streams (id, entity_type, entity_id) VALUES (1, 'match', 'old-match-id');
    INSERT INTO tournament_cases (id, match_id, related_entity_type, related_entity_id)
      VALUES (1, 'old-match-id', 'match', 'old-match-id');
  `);

  await ensureMatchIdReferenceSync(db);
  await exec(db, "UPDATE matches SET id = 'new-match-id' WHERE id = 'old-match-id'");

  const checks = [
    ["duels", "match_id"],
    ["match_lineup_submissions", "match_id"],
    ["match_lineup_entries", "match_id"],
    ["news", "match_id"],
    ["streams", "entity_id"],
    ["tournament_cases", "match_id"],
    ["tournament_cases", "related_entity_id"],
  ];
  for (const [table, column] of checks) {
    const rows = await all(db, `SELECT ${column} AS match_id FROM ${table}`);
    assert.deepEqual(rows, [{ match_id: "new-match-id" }], `${table}.${column}`);
  }
  assert.deepEqual(await all(db, "PRAGMA foreign_key_check"), []);
});
