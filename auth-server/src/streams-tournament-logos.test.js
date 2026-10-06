import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import sqlite3 from "sqlite3";

const server = readFileSync(new URL("./server.js", import.meta.url), "utf8");
const routeStart = server.indexOf('app.get("/streams",');
const routeEnd = server.indexOf('app.post("/streams",', routeStart);
assert.ok(routeStart >= 0 && routeEnd > routeStart);

async function createStreamsApi(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await new Promise((resolve, reject) => db.exec(`
    CREATE TABLE streams (
      id INTEGER, entity_type TEXT, entity_id TEXT, streamer_id INTEGER,
      link TEXT, created_at TEXT, updated_at TEXT, deleted_at TEXT
    );
    CREATE TABLE streamers (
      id INTEGER, profile_id TEXT, name TEXT, short_name TEXT,
      avatar TEXT, scoreboard_style TEXT, deleted_at TEXT
    );
    CREATE TABLE tournaments (id TEXT, logo TEXT, player_hub_visibility TEXT);
    CREATE TABLE matches (
      id TEXT, tournament_id TEXT, time_utc TEXT, team_1 TEXT,
      team_2 TEXT, status TEXT, deleted_at TEXT
    );
    CREATE TABLE duels (
      id TEXT, challenge_period_id TEXT, time_utc TEXT, player_1_id TEXT,
      player_2_id TEXT, duel_format TEXT, deleted_at TEXT
    );
    CREATE TABLE challenge_periods (id TEXT, name TEXT, logo TEXT);
    CREATE TABLE profiles (id TEXT, bga_nickname TEXT, avatar TEXT, association TEXT);
    CREATE TABLE associations (code TEXT, name TEXT, flag TEXT);
    CREATE TABLE teams (id TEXT, name TEXT, flag TEXT, logo TEXT);

    INSERT INTO streamers (id, profile_id, name) VALUES
      (1, 'streamer-player', 'Streamer'), (2, 'other-player', 'Other streamer');
    INSERT INTO tournaments VALUES
      ('HIDDEN-CUP', 'https://carcassonne.gg/hidden-cup.png', 'hidden'),
      ('VISIBLE-CUP', 'https://carcassonne.gg/visible-cup.png', 'visible');
    INSERT INTO matches (id, tournament_id, time_utc) VALUES
      ('hidden-match', ' hidden-cup ', '2099-01-02T18:00:00Z'),
      ('finished-match', 'HIDDEN-CUP', '2098-12-31T18:00:00Z'),
      ('visible-match', 'VISIBLE-CUP', '2099-01-02T18:00:00Z');
    INSERT INTO challenge_periods VALUES
      ('challenge-week', 'Challenge week', 'https://carcassonne.gg/challenge.png');
    INSERT INTO duels (id, challenge_period_id, time_utc) VALUES
      ('challenge-duel', 'challenge-week', '2099-01-02T18:00:00Z');
    INSERT INTO streams (id, entity_type, entity_id, streamer_id) VALUES
      (1, 'match', 'hidden-match', 1),
      (2, 'match', 'finished-match', 1),
      (3, 'match', 'visible-match', 1),
      (4, 'match', 'hidden-match', 2),
      (5, 'duel', 'challenge-duel', 1);
  `, (error) => error ? reject(error) : resolve()));

  let handler;
  const context = vm.createContext({
    db,
    app: { get: (_path, callback) => { handler = callback; } },
    normalizePositiveInteger(value) {
      const number = Number(value);
      return Number.isInteger(number) && number > 0 ? number : null;
    },
    getUserStreamerByProfileId(profileId, done) {
      db.get("SELECT * FROM streamers WHERE profile_id = ? AND deleted_at IS NULL", [profileId], done);
    },
  });
  vm.runInContext(server.slice(routeStart, routeEnd), context);

  return (user, query = {}) => new Promise((resolve, reject) => {
    handler({ user, query }, {
      statusCode: 200,
      status(code) { this.statusCode = code; return this; },
      json(payload) { resolve({ statusCode: this.statusCode, ...payload }); },
    }, reject);
  });
}

const streamer = { admin: 0, player_id: "streamer-player" };
const admin = { admin: 1, player_id: "admin-player" };

test("streamers receive the same tournament logos as admins without tournament management access", async (t) => {
  const request = await createStreamsApi(t);
  for (const query of [
    {},
    { section: "ongoing", today_start: "2099-01-01T00:00:00Z" },
    { section: "finished", today_start: "2099-01-01T00:00:00Z" },
    { id: 1 },
  ]) {
    const own = await request(streamer, query);
    const all = await request(admin, query);
    assert.equal(own.statusCode, 200);
    assert.equal(all.statusCode, 200);
    assert.ok(own.streams.length > 0);
    for (const stream of own.streams) {
      assert.equal(stream.streamer_id, 1, "streamers still see only their own streams");
      if (stream.entity_type !== "match") continue;
      const expectedLogo = stream.entity_id === "visible-match"
        ? "https://carcassonne.gg/visible-cup.png"
        : "https://carcassonne.gg/hidden-cup.png";
      assert.equal(stream.tournament_logo, expectedLogo);
      assert.equal(stream.tournament_logo, all.streams.find((row) => row.id === stream.id).tournament_logo);
    }
    if (query.section === "finished") {
      assert.equal(own.pagination.finished.total, 1);
    }
  }

  const own = await request(streamer);
  const all = await request(admin);
  assert.equal(all.streams.length, own.streams.length + 1);
  assert.equal(own.streams.find((stream) => stream.id === 5).challenge_period_logo,
    "https://carcassonne.gg/challenge.png");
});

test("tournament logos do not grant access to another streamer's streams", async (t) => {
  const request = await createStreamsApi(t);
  assert.deepEqual((await request(streamer, { id: 4 })).streams, []);
  assert.equal((await request({ admin: 0, player_id: "unassigned-player" })).statusCode, 403);
  assert.equal((await request(null)).statusCode, 401);
});
