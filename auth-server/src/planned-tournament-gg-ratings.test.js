import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import { updatePlannedTournamentGgRatings } from "./planned-tournament-gg-ratings.js";

const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (err) => err ? reject(err) : resolve()));
const all = (db, sql) => new Promise((resolve, reject) => db.all(sql, (err, rows) => err ? reject(err) : resolve(rows)));

async function fixture(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    CREATE TABLE profiles (id TEXT PRIMARY KEY, gg_elo REAL, deleted_at TEXT);
    INSERT INTO profiles VALUES ('a',1800,NULL), ('b',1800,NULL);
    CREATE TABLE tournaments (id TEXT PRIMARY KEY, ranking INTEGER, tournament_type TEXT, deleted_at TEXT);
    INSERT INTO tournaments VALUES ('T',1,'Teams',NULL),('U',0,'Teams',NULL),('I',1,'Individuals',NULL),('X',1,'Teams','deleted');
    CREATE TABLE matches (id TEXT PRIMARY KEY, tournament_id TEXT, status TEXT, deleted_at TEXT, updated_at TEXT DEFAULT 'old', updated_by TEXT DEFAULT 'keeper');
    INSERT INTO matches (id,tournament_id,status,deleted_at) VALUES
      ('team','T','Planned',NULL), ('unranked','U','Planned',NULL), ('done','T','Done',NULL),
      ('empty','T','Planned',NULL), ('missing','T','Planned',NULL), ('individual','I','Planned',NULL),
      ('deleted','T','Planned','deleted'), ('deleted-tournament','X','Planned',NULL);
    CREATE TABLE duels (
      id TEXT PRIMARY KEY, tournament_id TEXT, match_id TEXT, status TEXT,
      player_1_id TEXT DEFAULT 'a', player_2_id TEXT DEFAULT 'b', deleted_at TEXT,
      gg_rating_full REAL DEFAULT 2, gg_rating INTEGER DEFAULT 2,
      updated_at TEXT DEFAULT 'old', updated_by TEXT DEFAULT 'keeper'
    );
    INSERT INTO duels (id,tournament_id,match_id,status,deleted_at) VALUES
      ('team','T','team','Planned',NULL), ('unranked','U','unranked','Planned',NULL),
      ('done','T','team','Done',NULL), ('individual','I',NULL,'Planned',NULL),
      ('deleted','T','team','Planned','deleted'), ('deleted-parent','T','deleted','Planned',NULL),
      ('orphan','T','absent','Planned',NULL), ('no-tournament',NULL,NULL,'Planned',NULL),
      ('deleted-tournament','X','deleted-tournament','Planned',NULL), ('in-progress','T',NULL,'In progress',NULL),
      ('inherited',NULL,'team','Planned',NULL), ('missing','T','missing','Planned',NULL);
    UPDATE duels SET player_2_id='absent' WHERE id='missing';
    CREATE TABLE audit_trail (id INTEGER PRIMARY KEY);
  `);
  return db;
}

test("backfill updates only Planned ranking duels and Team matches, preserving audit fields", async (t) => {
  const db = await fixture(t);
  const summary = await updatePlannedTournamentGgRatings(db);
  assert.equal(summary.selected_duels,4);
  assert.equal(summary.calculated_duels,3);
  assert.equal(summary.duels_without_player_gg_elo,1);
  assert.equal(summary.selected_matches,3);
  assert.equal(summary.calculated_matches,1);
  const duels = await all(db,"SELECT * FROM duels");
  for (const duel of duels) {
    const expected = ['team','individual','inherited'].includes(duel.id) ? 6 : duel.id === 'missing' ? null : 2;
    assert.equal(duel.gg_rating_full,expected,duel.id);
    assert.equal(duel.gg_rating,expected,duel.id);
    assert.equal(duel.updated_at,'old');
    assert.equal(duel.updated_by,'keeper');
  }
  const matches = await all(db,"SELECT * FROM matches");
  for (const match of matches) {
    // Includes the preserved completed duel snapshot: sqrt((36+36+4)/3) rounds to 5.
    assert.equal(match.gg_rating,match.id === 'team' ? 5 : null,match.id);
    assert.equal(match.updated_at,'old');
    assert.equal(match.updated_by,'keeper');
  }
  assert.deepEqual(await all(db,"SELECT * FROM audit_trail"),[]);
  assert.deepEqual(await updatePlannedTournamentGgRatings(db),summary);
});

test("dry-run reports the same results but rolls back data and the new match column", async (t) => {
  const db = await fixture(t);
  const before = await all(db,"SELECT * FROM duels");
  const preview = await updatePlannedTournamentGgRatings(db, { dryRun:true });
  assert.equal(preview.calculated_matches,1);
  assert.deepEqual(await all(db,"SELECT * FROM duels"),before);
  assert.ok(!(await all(db,"PRAGMA table_info(matches)")).some((row) => row.name === 'gg_rating'));
  const applied = await updatePlannedTournamentGgRatings(db);
  assert.deepEqual(applied,{ ...preview, dry_run:false });
});

test("a match update error rolls back all duel writes and schema changes", async (t) => {
  const db = await fixture(t);
  await exec(db, "CREATE TRIGGER fail_match BEFORE UPDATE ON matches BEGIN SELECT RAISE(ABORT, 'test failure'); END;");
  const before = await all(db,"SELECT * FROM duels");
  await assert.rejects(updatePlannedTournamentGgRatings(db), /test failure/);
  assert.deepEqual(await all(db,"SELECT * FROM duels"),before);
  assert.ok(!(await all(db,"PRAGMA table_info(matches)")).some((row) => row.name === 'gg_rating'));
});

test("an empty Elo population aborts without replacing existing ratings", async (t) => {
  const db = await fixture(t);
  await exec(db,"UPDATE profiles SET gg_elo=NULL");
  const before = await all(db,"SELECT * FROM duels");
  await assert.rejects(updatePlannedTournamentGgRatings(db), /No numeric/);
  assert.deepEqual(await all(db,"SELECT * FROM duels"),before);
});
