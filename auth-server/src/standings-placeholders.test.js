import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import sqlite3 from "sqlite3";

const source = readFileSync(new URL("./server.js", import.meta.url), "utf8");
const route = source.slice(source.indexOf('app.put("/standings",'), source.indexOf('\napp.get(', source.indexOf('app.put("/standings",')));

test("standings placeholders survive saving, assignment and deletion without affecting players or other stages", async () => {
  const db = new sqlite3.Database(":memory:");
  const run = (sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, function (error) {
    if (error) reject(error); else resolve({ lastID: this.lastID });
  }));
  const all = (sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows)));
  const get = async (sql, params) => (await all(sql, params))[0];
  try {
    await run("CREATE TABLE tournaments (id TEXT PRIMARY KEY)");
    await run("CREATE TABLE tournament_teams (tournament_id TEXT, team_id TEXT)");
    const schema = source.match(/CREATE TABLE IF NOT EXISTS standings \([\s\S]*?\n    \)/)[0];
    await run(schema);
    await run("INSERT INTO tournaments VALUES ('T')");
    await run("INSERT INTO tournament_teams VALUES ('T', 'UA')");
    await run("INSERT INTO standings (tournament_id, stage, player_id) VALUES ('T', 'Stage 1', 'player')");
    await run("INSERT INTO standings (tournament_id, stage) VALUES ('T', 'Stage 2')");
    let handler;
    const load = (id, stage) => all("SELECT * FROM standings WHERE tournament_id = ? AND stage = ? ORDER BY id", [id, stage]);
    new Function("app", "requireTournamentAdmin", "normalizeNullableText", "STANDINGS_STAGES", "isValidStandingsStage", "normalizeStandingsStage", "dbGetAsync", "dbAllAsync", "dbRunAsync", "loadStandings", route)(
      { put: (_path, _auth, fn) => { handler = fn; } }, () => {},
      (value) => String(value ?? "").trim() || null, ["Stage 1", "Stage 2"],
      (stage) => ["Stage 1", "Stage 2"].includes(stage), (stage) => stage, get, all, run, load
    );
    const save = async (standings) => {
      let status = 200;
      let result;
      await handler({ body: { tournament_id: "T", stage: "Stage 1", standings } }, {
        status(value) { status = value; return this; },
        json(value) { result = value; return this; },
      });
      return { status, ...result };
    };
    let result = await save([{ team_id: null, group: "A" }, { team_id: "", group: "A" }, {}]);
    assert.equal(result.status, 200);
    let rows = result.standings.filter((row) => !row.player_id);
    assert.equal(rows.length, 3);
    assert.deepEqual(rows.map((row) => row.team_id), [null, null, null]);
    const ids = rows.map((row) => row.id);
    result = await save(rows);
    assert.equal(result.status, 200);
    assert.deepEqual(result.standings.filter((row) => !row.player_id).map((row) => row.id), ids);
    rows[0].team_id = "ua";
    result = await save(rows);
    assert.equal(result.status, 200);
    assert.equal(result.standings.find((row) => row.id === ids[0]).team_id, "UA");
    rows[0].team_id = null;
    result = await save(rows.slice(0, 2));
    assert.equal(result.status, 200);
    assert.equal(result.standings.filter((row) => !row.player_id).length, 2);
    assert.equal(result.standings.filter((row) => row.player_id).length, 1);
    assert.equal((await load("T", "Stage 2")).length, 1);
    assert.equal((await save([{ team_id: "UA" }, { team_id: "ua" }])).status, 400);
    assert.equal((await save([{ team_id: "UNKNOWN" }])).status, 400);
    assert.equal((await save([{ id: 999, team_id: null }])).status, 400);
  } finally {
    await new Promise((resolve, reject) => db.close((error) => error ? reject(error) : resolve()));
  }
});
