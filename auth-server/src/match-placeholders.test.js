import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import test from "node:test";
import sqlite3 from "sqlite3";

const source = readFileSync(new URL("./server.js", import.meta.url), "utf8");
const between = (start, end) => source.slice(source.indexOf(start), source.indexOf(end, source.indexOf(start)));
const routes = between('app.post("/matches",', 'app.post("/matches/:id/time-proposal",');
const publicHandler = between("function publicMainPageMatchesHandler(", 'app.get("/public/player-official-duels",');
const helpers = [
  between("function buildGeneratedMatchId(", "function ensureDuelsSchema("),
  between("function normalizePlannedMatchScores(", "function ratingValueToIntOrNull("),
  between("function normalizeTournamentAccessType(", "function normalizeTournamentAccessUserIds("),
].join("\n");

const globalAdmin = { admin: 1, player_id: "global-admin" };
const tournamentAdmin = { role: "admin", player_id: "tournament-admin" };
const captain = { role: "captain", team_captain: 1, association: "UKR", player_id: "captain" };
const matchPayload = {
  tournament_id: "T", team_1: "", team_2: "", time_utc: null,
  lineup_type: "Open", number_of_duels: 5, status: "Planned",
  stage: "Stage 2", round_name: "Final", knockout_id: 7,
};

async function createContext(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve, reject) => db.close((error) => error ? reject(error) : resolve())));
  const exec = (sql) => new Promise((resolve, reject) => db.exec(sql, (error) => error ? reject(error) : resolve()));
  const all = (sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows)));
  await exec(`
    ${source.match(/CREATE TABLE matches \([\s\S]*?\n        \);/)[0]}
    CREATE TABLE tournaments (id TEXT PRIMARY KEY, name TEXT, short_title TEXT, logo TEXT, link TEXT, tournament_type TEXT);
    CREATE TABLE teams (id TEXT PRIMARY KEY, name TEXT, flag TEXT, logo TEXT);
    CREATE TABLE news (id TEXT, match_id TEXT, title TEXT, short_title TEXT, time_utc TEXT, status TEXT, deleted_at TEXT, updated_at TEXT);
    CREATE TABLE job_runs (job_name TEXT, last_success_at TEXT);
    INSERT INTO tournaments (id, name, tournament_type) VALUES ('T', 'Test Cup', 'Teams'), ('Friendly-Matches', 'Friendly', 'Teams');
    INSERT INTO teams (id, name) VALUES ('UKR', 'Ukraine'), ('POL', 'Poland');
  `);
  const handlers = {};
  const auditEvents = [];
  const emptyRelatedRows = (_ids, done) => done(null, []);
  const dependencies = {
    app: {
      post: (path, handler) => { handlers[`POST ${path}`] = handler; },
      patch: (path, handler) => { handlers[`PATCH ${path}`] = handler; },
      get: (path, handler) => { handlers[`GET ${path}`] = handler; },
    },
    // No duels are needed for these matches; run the actual match/public SQL against SQLite.
    db: {
      get: db.get.bind(db), run: db.run.bind(db), serialize: db.serialize.bind(db),
      all(sql, params, done) {
        if (sql.includes("FROM duels d")) return done(null, []);
        return db.all(sql, params, done);
      },
    },
    randomUUID,
    loadTournamentAccessForUser(id, user, done) {
      done(null, {
        id, subtype: id === "T" ? "Official" : "Friendly", tournament_type: "Teams", lineup_type: "Open",
        has_access: !!(user.admin || user.role), access_role: user.admin ? "admin" : user.role,
      });
    },
    TOURNAMENT_ACCESS_TYPES: { OFFICIAL: "Official", FRIENDLY: "Friendly" },
    TOURNAMENT_ACCESS_ROLES: { ADMIN: "admin" },
    normalizeStatusText: (value) => String(value || "").trim().toLowerCase(),
    normalizeTournamentLineupType: (_type, lineup) => lineup,
    isBlindLineupType: (value) => ["blind", "secret", "closed"].includes(String(value).toLowerCase()),
    SECRET_LINEUP_SIZE: 5,
    MATCH_AUDIT_FIELDS: [],
    getAuditActor: () => ({}),
    buildAuditCreationChanges: (row) => row,
    buildAuditChanges: (_before, after) => after,
    logAuditEvent: (entry, done) => { auditEvents.push(entry); done(); },
    loadStreamsByMatchIds: emptyRelatedRows,
    loadStreamsByDuelIds: emptyRelatedRows,
    loadGamesByDuelIds: emptyRelatedRows,
    normalizeBooleanInt: (value) => Number(value) === 1 ? 1 : 0,
  };
  // Exercise the production handlers without starting the server or background jobs.
  new Function(...Object.keys(dependencies), `${helpers}\n${routes}\n${publicHandler}`)(...Object.values(dependencies));

  const request = (route, req) => new Promise((resolve, reject) => {
    let status = 200;
    handlers[route](req, {
      status(value) { status = value; return this; },
      json(data) { resolve({ status, ...data }); },
    }, reject);
  });
  return {
    create: (user, payload = {}) => request("POST /matches", { user, body: { ...matchPayload, ...payload } }),
    update: (user, id, payload = {}) => request("PATCH /matches/:id", { user, params: { id }, body: { ...matchPayload, ...payload } }),
    publicMatches: () => request("GET /public/main-page-matches", { query: { tournament_id: "T", include_bracket: "true" } }),
    savedMatches: () => all("SELECT * FROM matches ORDER BY id"),
    auditEvents,
  };
}

test("placeholder matches persist with unique IDs and appear in the public tournament feed", async (t) => {
  const ctx = await createContext(t);
  const first = await ctx.create(globalAdmin);
  const second = await ctx.create(tournamentAdmin);
  assert.equal(first.status, 201);
  assert.equal(second.status, 201);
  assert.ok(first.match.id);
  assert.ok(second.match.id);
  assert.notEqual(first.match.id, second.match.id);
  assert.equal((await ctx.savedMatches()).length, 2);
  assert.equal(ctx.auditEvents.length, 2);

  const feed = await ctx.publicMatches();
  assert.equal(feed.ok, true);
  assert.equal(feed.matches.length, 2);
  for (const match of feed.matches) {
    assert.equal(match.team_1, "");
    assert.equal(match.team_2, "");
    assert.equal(match.team1, "TBD");
    assert.equal(match.team2, "TBD");
    assert.equal(match.time_utc, null);
    assert.equal(match.round, "Final");
    assert.equal(match.knockout_id, 7);
  }
  assert.deepEqual(feed.teams, []);
});

test("tournament admins can edit placeholders and assign teams in stages", async (t) => {
  const ctx = await createContext(t);
  const created = await ctx.create(tournamentAdmin);
  const matchId = created.match.id;
  const edited = await ctx.update(tournamentAdmin, matchId, { round_name: "Semi-final" });
  assert.equal(edited.status, 200);
  assert.equal(edited.match.id, matchId);
  assert.equal(edited.match.round_name, "Semi-final");

  const partial = await ctx.update(tournamentAdmin, matchId, { team_1: "ukr" });
  assert.equal(partial.status, 200);
  assert.equal(partial.match.id, matchId);
  const partialFeed = await ctx.publicMatches();
  assert.equal(partialFeed.matches[0].team1, "Ukraine");
  assert.equal(partialFeed.matches[0].team2, "TBD");

  const assigned = await ctx.update(tournamentAdmin, matchId, {
    team_1: "UKR", team_2: "POL", time_utc: "2027-01-10T12:00:00Z",
  });
  assert.equal(assigned.status, 200);
  assert.equal(assigned.match.id, "20270110UKRPOL");
  assert.equal((await ctx.savedMatches()).length, 1);
  const feed = await ctx.publicMatches();
  assert.equal(feed.matches[0].team1, "Ukraine");
  assert.equal(feed.matches[0].team2, "Poland");
});

test("matches can be created with either team missing, with or without a date", async (t) => {
  const ctx = await createContext(t);
  for (const teams of [{ team_1: "UKR" }, { team_2: "POL" }]) {
    for (const time of [null, "2027-01-10T12:00:00Z"]) {
      const result = await ctx.create(tournamentAdmin, { ...teams, time_utc: time, lineup_type: "Blind" });
      assert.equal(result.status, 201);
      assert.ok(result.match.id);
      assert.equal(result.match.number_of_duels, 5);
    }
  }
  assert.equal((await ctx.publicMatches()).matches.length, 4);
});

test("placeholder support preserves match access and duplicate-team validation", async (t) => {
  const ctx = await createContext(t);
  assert.equal((await ctx.create(null)).status, 401);
  assert.equal((await ctx.create({})).status, 403);
  assert.equal((await ctx.create(captain)).status, 403);
  assert.equal((await ctx.create(captain, { tournament_id: "Friendly-Matches", team_1: "UKR" })).status, 400);
  assert.equal((await ctx.create(globalAdmin, { team_1: "ukr", team_2: "UKR" })).status, 400);

  const created = await ctx.create(tournamentAdmin);
  assert.equal((await ctx.update(captain, created.match.id)).status, 403);
  assert.equal((await ctx.update({}, created.match.id)).status, 403);
  assert.equal((await ctx.update(tournamentAdmin, created.match.id, { team_1: "UKR", team_2: "ukr" })).status, 400);

  const complete = await ctx.create(captain, {
    tournament_id: "Friendly-Matches", team_1: "UKR", team_2: "POL", time_utc: "2027-01-10T12:00:00Z",
  });
  assert.equal(complete.status, 201);
  assert.equal(complete.match.id, "20270110UKRPOL");
  assert.equal((await ctx.create(globalAdmin, {
    team_1: "UKR", team_2: "POL", time_utc: "2027-01-10T12:00:00Z",
  })).status, 409);
});
