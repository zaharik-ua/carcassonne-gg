import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import sqlite3 from "sqlite3";
import {
  buildGgRatingContext,
  calculateGgDuelRating,
  calculateGgMatchRating,
  ensureGgRatingsSchema,
  loadGgRatingContext,
  updateMatchGgRating,
} from "./gg-ratings.js";

const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (err) => err ? reject(err) : resolve()));
const all = (db, sql) => new Promise((resolve, reject) => db.all(sql, (err, rows) => err ? reject(err) : resolve(rows)));
const run = (db, sql, params) => new Promise((resolve, reject) => db.run(sql, params, (err) => err ? reject(err) : resolve()));

async function database(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    CREATE TABLE profiles (id TEXT PRIMARY KEY, gg_elo REAL, deleted_at TEXT);
    CREATE TABLE tournaments (id TEXT PRIMARY KEY, ranking INTEGER, tournament_type TEXT);
    CREATE TABLE matches (id TEXT PRIMARY KEY, tournament_id TEXT, deleted_at TEXT, updated_at TEXT DEFAULT 'old');
    CREATE TABLE duels (
      id TEXT PRIMARY KEY, tournament_id TEXT, match_id TEXT, is_test INTEGER,
      duel_number INTEGER, duel_format TEXT, time_utc TEXT, custom_time INTEGER,
      player_1_id TEXT, player_2_id TEXT, dw1 INTEGER, dw2 INTEGER, ranking INTEGER,
      status TEXT, created_by TEXT, updated_by TEXT, deleted_by TEXT, deleted_at TEXT,
      created_at TEXT, updated_at TEXT
    );
    INSERT INTO profiles (id, gg_elo) VALUES ('p1',1000),('p2',1200),('p3',1400),('p4',1600),('p5',1800);
    INSERT INTO profiles VALUES ('deleted',9999,'deleted');
    INSERT INTO tournaments VALUES ('T',1,'Teams'),('U',0,'Teams'),('I',1,'Individuals');
    INSERT INTO matches (id,tournament_id) VALUES ('m','T'),('unranked','U'),('individual','I');
  `);
  await ensureGgRatingsSchema(db);
  return db;
}

test("GG formula matches reference values, rejects missing Elo and non-ranking duels", () => {
  const context = buildGgRatingContext([1000,1200,1400,1600,1800].map((gg_elo, id) => ({ id: `p${id+1}`, gg_elo })));
  assert.equal(context.lowAnchor, 1120);
  assert.equal(context.highAnchor, 1760);
  const result = calculateGgDuelRating(context, "p3", "p4");
  assert.ok(Math.abs(result.ggRatingFull - 3.6505809577542965) < 1e-12);
  assert.equal(result.ggRating, 4);
  assert.deepEqual(calculateGgDuelRating(context, "p5", "p5"), { ggRatingFull: 6, ggRating: 6 });
  for (const [ctx, p1, p2, ranked] of [[context,"p3","p4",0],[context,"p1","missing",1],[buildGgRatingContext([]),"p1","p2",1]]) {
    assert.deepEqual(calculateGgDuelRating(ctx,p1,p2,ranked), { ggRatingFull: null, ggRating: null });
  }
  assert.equal(calculateGgMatchRating([0,6]), 4);
  assert.equal(calculateGgMatchRating([2.4,4.4]), 4); // Use full values, not rounded duel ratings.
  assert.equal(calculateGgMatchRating([6,null]), null);
  assert.equal(calculateGgMatchRating([]), null);
});

test("schema migration is repeatable; aggregates exclude deleted duels and require all ratings", async (t) => {
  const db = await database(t);
  await ensureGgRatingsSchema(db);
  assert.equal((await loadGgRatingContext(db)).highAnchor, 1760);
  await exec(db, `
    INSERT INTO duels (id,match_id,gg_rating_full) VALUES ('a','m',0),('b','m',6),('c','unranked',6),('d','individual',6);
    INSERT INTO duels (id,match_id,gg_rating_full,deleted_at) VALUES ('deleted','m',NULL,'deleted');
  `);
  assert.equal(await updateMatchGgRating(db, 'm'), 4);
  assert.equal(await updateMatchGgRating(db, 'unranked'), null);
  assert.equal(await updateMatchGgRating(db, 'individual'), null);
  assert.deepEqual(await all(db,"SELECT gg_rating,updated_at FROM matches WHERE id='m'"), [{ gg_rating:4, updated_at:'old' }]);
  await exec(db, "UPDATE duels SET gg_rating_full=NULL WHERE id='a'");
  assert.equal(await updateMatchGgRating(db, 'm'), null);
  await exec(db, "DELETE FROM duels WHERE match_id='m'");
  assert.equal(await updateMatchGgRating(db, 'm'), null);
});

test("both bulk-upsert paths fill planned ratings and preserve completed snapshots", async (t) => {
  const db = await database(t);
  const source = readFileSync(new URL('./server.js', import.meta.url), 'utf8');
  const route = source.slice(source.indexOf('app.post("/duels/bulk-upsert"'));
  const statements = [...route.matchAll(/const stmt = db.prepare\(`([\s\S]*?)`\);/g)].slice(0,2).map((match) => match[1]);
  assert.equal(statements.length,2);
  for (const [index,sql] of statements.entries()) {
    const id = `upsert-${index}`;
    const params = (full, rounded, status='Planned', ranking=1) => [
      id,'T', ...(index ? ['m',0] : []), 1,'Bo3',null,0,'p3','p4',null,null,full,rounded,ranking,status,'actor','actor',
    ];
    await run(db,sql,params(null,null));
    await run(db,sql,params(3.65,4));
    assert.deepEqual(await all(db,`SELECT gg_rating_full,gg_rating FROM duels WHERE id='${id}'`), [{ gg_rating_full:3.65,gg_rating:4 }]);
    await run(db,sql,params(6,6,'Done'));
    assert.equal((await all(db,`SELECT gg_rating FROM duels WHERE id='${id}'`))[0].gg_rating,4);
    await run(db,sql,params(null,null,'Planned',0));
    assert.equal((await all(db,`SELECT gg_rating FROM duels WHERE id='${id}'`))[0].gg_rating,null);
  }
});
