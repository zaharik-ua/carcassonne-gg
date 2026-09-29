import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import sqlite3 from "sqlite3";

import { ensureMatchIdReferenceSync } from "./match-id-sync.js";

const source = readFileSync(new URL("./server.js", import.meta.url), "utf8");
const routeStart = source.indexOf('app.patch("/matches/:id/time-proposal/accept"');
const routeEnd = source.indexOf('app.delete("/matches/:id"', routeStart);
const acceptRoute = source.slice(routeStart, routeEnd);
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

test("accepting a match time immediately updates its generated id and lineup links", async (t) => {
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
      team_1 TEXT,
      team_2 TEXT,
      status TEXT,
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
      deleted_at TEXT
    );
    CREATE TABLE match_lineup_submissions (match_id TEXT, team_id TEXT, PRIMARY KEY (match_id, team_id));
    CREATE TABLE match_lineup_entries (match_id TEXT, team_id TEXT, position INTEGER, PRIMARY KEY (match_id, team_id, position));
    CREATE TABLE news (id INTEGER PRIMARY KEY, match_id TEXT);
    CREATE TABLE streams (id INTEGER PRIMARY KEY, entity_type TEXT, entity_id TEXT);
    CREATE TABLE tournament_cases (id INTEGER PRIMARY KEY, match_id TEXT, related_entity_type TEXT, related_entity_id TEXT);

    INSERT INTO matches (
      id, tournament_id, proposed_time_utc, proposed_time_by_team_id,
      proposed_time_status, lineup_type, lineup_deadline_h,
      team_1, team_2, status
    ) VALUES (
      'temporary-match-id', 'CUP', '2099-01-02T18:00:00.000Z', 'UKR',
      'pending', 'Open', NULL, 'UKR', 'POL', 'Planned'
    );
    INSERT INTO duels (id, match_id, status) VALUES ('lineup-1', 'temporary-match-id', 'Planned');
  `);
  await ensureMatchIdReferenceSync(db);

  const handlers = {};
  const auditEvents = [];
  const dependencies = {
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
  assert.equal((await get(db, "SELECT match_id FROM duels WHERE id = 'lineup-1'")).match_id, "20990102UKRPOL");
  assert.equal(auditEvents[0].record_id, "20990102UKRPOL");
  assert.equal(auditEvents[0].metadata.previous_record_id, "temporary-match-id");
});
