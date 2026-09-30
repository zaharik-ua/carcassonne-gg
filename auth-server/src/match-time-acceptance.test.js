import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import sqlite3 from "sqlite3";

import { ensureMatchIdReferenceSync } from "./match-id-sync.js";
import {
  calculateLineupDeadlineUtc,
  softDeleteMatchLineupDataInTransaction,
} from "./secret-lineups.js";

const source = readFileSync(new URL("./server.js", import.meta.url), "utf8");
const routeStart = source.indexOf('app.patch("/matches/:id/time-proposal/accept"');
const routeEnd = source.indexOf('app.delete("/matches/:id"', routeStart);
const acceptRoute = source.slice(routeStart, routeEnd);
const adminRouteStart = source.indexOf('app.patch("/matches/:id",');
const adminRouteEnd = source.indexOf('app.post("/matches/:id/time-proposal"', adminRouteStart);
const adminUpdateRoute = source.slice(adminRouteStart, adminRouteEnd);
const generatedIdStart = source.indexOf("function buildGeneratedMatchId(");
const generatedIdEnd = source.indexOf("function ensureDuelsSchema(", generatedIdStart);
const generatedIdHelper = source.slice(generatedIdStart, generatedIdEnd);

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => error ? reject(error) : resolve());
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
    db.get(sql, params, (error, row) => error ? reject(error) : resolve(row || null));
  });
}

test("an admin match edit recalculates lineup_deadline_utc from the saved match time", () => {
  assert.match(
    adminUpdateRoute,
    /const lineupDeadlineUtc = lineupType === "Open"[\s\S]*?calculateLineupDeadlineUtc\(timeUtc, lineupDeadlineHours\)/
  );
  assert.match(adminUpdateRoute, /lineup_deadline_utc = \?/);
});

test("accepting a rescheduled match time resets the deadline and soft-deletes old lineups", async (t) => {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve, reject) => {
    db.close((error) => error ? reject(error) : resolve());
  }));

  await exec(db, `
    CREATE TABLE matches (
      id TEXT PRIMARY KEY,
      tournament_id TEXT,
      time_utc TEXT,
      proposed_time_utc TEXT,
      proposed_time_by_team_id TEXT,
      proposed_time_status TEXT,
      lineup_type TEXT,
      lineup_deadline_h INTEGER,
      lineup_deadline_utc TEXT,
      lineups_published_at TEXT,
      team_1_lineup_added INTEGER DEFAULT 0,
      team_2_lineup_added INTEGER DEFAULT 0,
      team_1 TEXT,
      team_2 TEXT,
      status TEXT,
      dw1 INTEGER,
      dw2 INTEGER,
      gw1 INTEGER,
      gw2 INTEGER,
      rating INTEGER,
      gg_rating INTEGER,
      updated_by TEXT,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
      deleted_at TEXT
    );
    CREATE TABLE duels (
      id TEXT PRIMARY KEY,
      match_id TEXT,
      time_utc TEXT,
      custom_time INTEGER DEFAULT 0,
      status TEXT,
      updated_by TEXT,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
      deleted_by TEXT,
      deleted_at TEXT
    );
    CREATE TABLE match_lineup_submissions (match_id TEXT, team_id TEXT, PRIMARY KEY (match_id, team_id));
    CREATE TABLE match_lineup_entries (match_id TEXT, team_id TEXT, position INTEGER, player_id TEXT, PRIMARY KEY (match_id, team_id, position));
    CREATE TABLE games (id TEXT PRIMARY KEY, duel_id TEXT, deleted_at TEXT);
    CREATE TABLE news (id INTEGER PRIMARY KEY, match_id TEXT);
    CREATE TABLE streams (
      id INTEGER PRIMARY KEY,
      entity_type TEXT,
      entity_id TEXT,
      deleted_at TEXT,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE tournament_cases (id INTEGER PRIMARY KEY, match_id TEXT, related_entity_type TEXT, related_entity_id TEXT);

    INSERT INTO matches (
      id, tournament_id, time_utc, proposed_time_utc, proposed_time_by_team_id,
      proposed_time_status, lineup_type, lineup_deadline_h, lineup_deadline_utc,
      lineups_published_at, team_1_lineup_added, team_2_lineup_added,
      team_1, team_2, status, dw1, dw2, gw1, gw2, rating, gg_rating
    ) VALUES (
      'temporary-match-id', 'CUP', '2099-01-01T18:00:00.000Z', '2099-01-02T18:00:00.000Z', 'UKR',
      'change_pending', 'Blind', 24, '2098-12-31T18:00:00.000Z',
      '2098-12-31T18:00:00.000Z', 1, 1,
      'UKR', 'POL', 'Planned', 1, 2, 3, 4, 5, 6
    );
    INSERT INTO duels (id, match_id, status) VALUES ('lineup-1', 'temporary-match-id', 'Planned');
    INSERT INTO games (id, duel_id) VALUES ('game-1', 'lineup-1');
    INSERT INTO streams (id, entity_type, entity_id) VALUES (1, 'duel', 'lineup-1');
    INSERT INTO match_lineup_submissions (match_id, team_id) VALUES ('temporary-match-id', 'UKR');
    INSERT INTO match_lineup_entries (match_id, team_id, position, player_id)
      VALUES ('temporary-match-id', 'UKR', 1, 'player-1');
  `);
  await exec(db, `
    ALTER TABLE match_lineup_submissions ADD COLUMN deleted_by TEXT;
    ALTER TABLE match_lineup_submissions ADD COLUMN deleted_at TEXT;
    ALTER TABLE match_lineup_submissions ADD COLUMN updated_at TEXT;
    ALTER TABLE match_lineup_entries ADD COLUMN deleted_by TEXT;
    ALTER TABLE match_lineup_entries ADD COLUMN deleted_at TEXT;
    ALTER TABLE match_lineup_entries ADD COLUMN updated_at TEXT;
  `);
  await ensureMatchIdReferenceSync(db);

  const handlers = {};
  const auditEvents = [];
  const dependencies = {
    db,
    app: {
      patch(path, ...routeHandlers) {
        handlers[path] = routeHandlers.at(-1);
      },
    },
    requireAuthenticated: (_req, _res, next) => next(),
    dbRunAsync: (sql, params = []) => run(db, sql, params),
    dbGetAsync: (sql, params = []) => get(db, sql, params),
    loadMatchTimeProposalContext: async (matchId) => ({
      match: await get(db, "SELECT * FROM matches WHERE id = ?", [matchId]),
      captainTeamId: "POL",
    }),
    matchTimeProposalSnapshotMatches: () => true,
    MATCH_TIME_PROPOSAL_STATUSES: {
      PENDING: "pending",
      CHANGE_PENDING: "change_pending",
      ACCEPTED: "accepted",
    },
    normalizeUtcTimestamp: (value) => {
      const timestamp = Date.parse(String(value || ""));
      return Number.isFinite(timestamp) ? new Date(timestamp).toISOString() : null;
    },
    normalizeNullableText: (value) => String(value ?? "").trim() || null,
    isBlindLineupType: (value) => String(value || "").trim().toLowerCase() === "blind",
    calculateLineupDeadlineUtc,
    softDeleteMatchLineupDataInTransaction,
    getAuditActor: () => ({}),
    buildAuditChanges: () => ({}),
    MATCH_AUDIT_FIELDS: [],
    logAuditEvent: (event) => auditEvents.push(event),
    console,
  };
  new Function(
    ...Object.keys(dependencies),
    `${generatedIdHelper}\n${acceptRoute}`
  )(...Object.values(dependencies));

  const result = await new Promise((resolve, reject) => {
    handlers["/matches/:id/time-proposal/accept"](
      {
        params: { id: "temporary-match-id" },
        body: {},
        user: { player_id: "captain-pol" },
      },
      {
        status(statusCode) {
          this.statusCode = statusCode;
          return this;
        },
        json(body) {
          resolve({ status: this.statusCode || 200, ...body });
        },
      },
      reject
    );
  });

  assert.equal(result.status, 200);
  assert.equal(result.match.id, "20990102UKRPOL");
  assert.equal(result.match.time_utc, "2099-01-02T18:00:00.000Z");
  assert.equal(result.match.lineup_deadline_utc, "2099-01-01T18:00:00.000Z");
  assert.equal(result.match.lineups_published_at, null);
  assert.equal(result.match.team_1_lineup_added, 0);
  assert.equal(result.match.team_2_lineup_added, 0);
  assert.equal(result.match.dw1, null);
  assert.equal(result.match.dw2, null);
  assert.equal(result.match.gw1, null);
  assert.equal(result.match.gw2, null);
  assert.equal(result.match.rating, null);
  assert.equal(result.match.gg_rating, null);
  const deletedDuel = await get(db, "SELECT match_id, deleted_at, deleted_by FROM duels WHERE id = 'lineup-1'");
  assert.equal(deletedDuel.match_id, "20990102UKRPOL");
  assert.ok(deletedDuel.deleted_at);
  assert.equal(deletedDuel.deleted_by, "captain-pol");
  assert.ok((await get(db, "SELECT deleted_at FROM games WHERE id = 'game-1'")).deleted_at);
  assert.ok((await get(db, "SELECT deleted_at FROM streams WHERE id = 1")).deleted_at);
  assert.ok((await get(db, "SELECT deleted_at FROM match_lineup_submissions WHERE team_id = 'UKR'")).deleted_at);
  assert.ok((await get(db, "SELECT deleted_at FROM match_lineup_entries WHERE player_id = 'player-1'")).deleted_at);
  assert.equal(auditEvents[0].record_id, "20990102UKRPOL");
  assert.equal(auditEvents[0].metadata.previous_record_id, "temporary-match-id");
  assert.deepEqual(auditEvents[0].metadata.lineup_reset, {
    duels: 1,
    entries: 1,
    submissions: 1,
    games: 1,
    streams: 1,
  });
});
