import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import {
  MATCH_CASE_CHOICES,
  NON_MATCH_CASE_CHOICES,
  createOrAppendTournamentCase,
  ensureTournamentCasesSchema,
  normalizeCaseChoice,
  normalizeCaseDetails,
  registerTournamentCaseRoutes,
} from "./tournament-cases.js";

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

const BASE_TABLES_SQL = `
  PRAGMA foreign_keys = ON;
  CREATE TABLE users (id INTEGER PRIMARY KEY);
  CREATE TABLE profiles (id TEXT PRIMARY KEY);
  CREATE TABLE matches (id TEXT PRIMARY KEY);
  CREATE TABLE duels (id TEXT PRIMARY KEY);
  CREATE TABLE tournaments (id TEXT PRIMARY KEY);
  CREATE TABLE challenge_periods (id TEXT PRIMARY KEY);
  INSERT INTO users (id) VALUES (1);
  INSERT INTO profiles (id) VALUES ('reporter'), ('other');
  INSERT INTO duels (id) VALUES ('duel-1');
  INSERT INTO tournaments (id) VALUES ('CCL-2027');
`;

async function openDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, BASE_TABLES_SQL);
  return db;
}

async function createDatabase(t) {
  const db = await openDatabase(t);
  await ensureTournamentCasesSchema(db);
  return db;
}

const columnNames = async (db) => new Set((await all(db, "PRAGMA table_info(tournament_cases)")).map((column) => column.name));

test("creates tournament_cases with details and without subject or reported player", async (t) => {
  const db = await createDatabase(t);
  const names = await columnNames(db);
  [
    "case_type", "category", "status", "priority", "details",
    "submitted_by_user_id", "submitted_by_player_id", "responsible_user_id",
    "match_id", "duel_id", "tournament_id", "challenge_period_id",
    "related_entity_type", "related_entity_id", "resolution", "resolved_at", "deleted_at",
  ].forEach((name) => assert.equal(names.has(name), true, `missing ${name}`));
  assert.equal(names.has("subject"), false);
  assert.equal(names.has("reported_player_id"), false);

  await run(db, "INSERT INTO tournament_cases (case_type, details, submitted_by_player_id) VALUES ('feedback', 'Nice', 'reporter')");
  await assert.rejects(
    run(db, "INSERT INTO tournament_cases (case_type, status, submitted_by_player_id) VALUES ('problem', 'waiting', 'reporter')"),
    /CHECK constraint failed/
  );
  await assert.rejects(
    run(db, "INSERT INTO tournament_cases (case_type, submitted_by_player_id) VALUES ('praise', 'reporter')"),
    /CHECK constraint failed/
  );
});

test("rebuilds the first version of the table, dropping subject and reported_player_id", async (t) => {
  const db = await openDatabase(t);
  await exec(db, `
    CREATE TABLE tournament_cases (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      case_type TEXT NOT NULL DEFAULT 'problem',
      category TEXT,
      status TEXT NOT NULL DEFAULT 'open',
      priority TEXT NOT NULL DEFAULT 'normal',
      subject TEXT NOT NULL,
      details TEXT NOT NULL DEFAULT '',
      submitted_by_user_id INTEGER,
      submitted_by_player_id TEXT NOT NULL,
      responsible_user_id INTEGER,
      reported_player_id TEXT,
      match_id TEXT,
      duel_id TEXT,
      tournament_id TEXT,
      challenge_period_id TEXT,
      related_entity_type TEXT,
      related_entity_id TEXT,
      resolution TEXT,
      resolved_at TEXT,
      deleted_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (reported_player_id) REFERENCES profiles(id) ON DELETE SET NULL,
      CHECK (case_type IN ('complaint', 'problem', 'request', 'other'))
    );
    INSERT INTO tournament_cases (id, case_type, category, subject, details, submitted_by_player_id, reported_player_id, duel_id, created_at)
      VALUES (7, 'complaint', 'no_show', 'No-show report: other vs reporter', 'Did not show up', 'reporter', 'other', 'duel-1', '2026-11-01 10:00:00');
    CREATE TRIGGER trg_matches_sync_references_after_id_update AFTER UPDATE OF id ON matches
    BEGIN UPDATE tournament_cases SET match_id = NEW.id WHERE match_id = OLD.id; END;
  `);

  await ensureTournamentCasesSchema(db);
  await ensureTournamentCasesSchema(db); // a second start changes nothing

  const names = await columnNames(db);
  assert.equal(names.has("subject"), false);
  assert.equal(names.has("reported_player_id"), false);
  const rows = await all(db, "SELECT * FROM tournament_cases");
  assert.equal(rows.length, 1);
  assert.equal(rows[0].id, 7);
  assert.equal(rows[0].details, "Did not show up");
  assert.equal(rows[0].duel_id, "duel-1");
  assert.equal(rows[0].created_at, "2026-11-01 10:00:00");
  // The match id sync trigger is re-created by ensureMatchIdReferenceSync after the schema.
  assert.deepEqual(await all(db, "SELECT name FROM sqlite_master WHERE type = 'trigger'"), []);
  const indexes = (await all(db, "PRAGMA index_list(tournament_cases)")).map((index) => index.name);
  assert.ok(indexes.includes("idx_tournament_cases_duel"));
  // New ids continue after the copied ones.
  const insert = await run(db, "INSERT INTO tournament_cases (case_type, submitted_by_player_id) VALUES ('feedback', 'reporter')");
  assert.equal(insert.lastID, 8);
});

test("a player's open case on a duel takes new details of the same category", async (t) => {
  const db = await createDatabase(t);
  const base = {
    case_type: "problem", category: "scheduling", submitted_by_user_id: 1,
    submitted_by_player_id: "reporter", duel_id: "duel-1", tournament_id: "CCL-2027",
  };
  const first = await createOrAppendTournamentCase(db, { ...base, details: "No answer" });
  assert.equal(first.appended, false);
  assert.equal(first.case.status, "open");

  const second = await createOrAppendTournamentCase(db, { ...base, details: "Still no answer" });
  assert.equal(second.appended, true);
  assert.equal(second.case.id, first.case.id);
  assert.equal(second.case.details, "No answer\n--\nStill no answer");

  // Another category, or a closed case, starts a new case.
  const other = await createOrAppendTournamentCase(db, { ...base, category: "result_dispute", details: "Wrong" });
  assert.notEqual(other.case.id, first.case.id);
  await run(db, "UPDATE tournament_cases SET status = 'resolved' WHERE id = ?", [first.case.id]);
  const third = await createOrAppendTournamentCase(db, { ...base, details: "Again" });
  assert.equal(third.appended, false);

  // Cases without a duel are never merged.
  const rules = { case_type: "request", category: "general", submitted_by_player_id: "reporter", tournament_id: "CCL-2027" };
  const a = await createOrAppendTournamentCase(db, { ...rules, details: "Q1" });
  const b = await createOrAppendTournamentCase(db, { ...rules, details: "Q2" });
  assert.notEqual(a.case.id, b.case.id);
});

test("case choices and details are validated", () => {
  assert.deepEqual(normalizeCaseChoice({ case_type: "Feedback" }, NON_MATCH_CASE_CHOICES), { case_type: "feedback", category: null });
  assert.deepEqual(
    normalizeCaseChoice({ case_type: "complaint", category: "conduct" }, NON_MATCH_CASE_CHOICES),
    { case_type: "complaint", category: "conduct" }
  );
  assert.ok(normalizeCaseChoice({ case_type: "problem", category: "scheduling" }, NON_MATCH_CASE_CHOICES).error);
  assert.ok(normalizeCaseChoice({ case_type: "problem", category: "scheduling" }, MATCH_CASE_CHOICES).case_type);
  assert.ok(normalizeCaseChoice({ case_type: "complaint", category: "no_show" }, MATCH_CASE_CHOICES).error);
  assert.ok(normalizeCaseChoice({}, NON_MATCH_CASE_CHOICES).error);
  assert.equal(normalizeCaseDetails("  text ").value, "text");
  assert.ok(normalizeCaseDetails(" ").error);
  assert.ok(normalizeCaseDetails("x".repeat(5001)).error);
});

function createApi(db) {
  const routes = new Map();
  const app = {
    get: (routePath, ...handlers) => routes.set(`GET ${routePath}`, handlers.at(-1)),
    post: (routePath, ...handlers) => routes.set(`POST ${routePath}`, handlers.at(-1)),
  };
  const audit = [];
  registerTournamentCaseRoutes(app, {
    db,
    requireAuthenticated: () => {},
    logAuditEvent: (entry, done) => {
      audit.push(entry);
      done();
    },
    logger: { error() {} },
  });
  const call = async (route, { user = null, params = {}, body = {} } = {}) => {
    const result = { status: 200, body: null };
    const res = {
      status(code) {
        result.status = code;
        return this;
      },
      json(value) {
        result.body = value;
        return this;
      },
    };
    await routes.get(route)({ user, params, body }, res);
    return result;
  };
  return { call, audit };
}

test("POST /tournaments/:tournamentId/cases sends a case from the Rules page", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const user = { id: 1, player_id: "reporter" };
  const route = "POST /tournaments/:tournamentId/cases";

  const sent = await api.call(route, {
    user,
    params: { tournamentId: "ccl-2027" },
    body: { case_type: "request", category: "general", details: "When is the draw?" },
  });
  assert.equal(sent.status, 201, JSON.stringify(sent.body));
  assert.equal(sent.body.case.tournament_id, "CCL-2027");
  assert.equal(sent.body.case.duel_id, null);
  assert.equal(sent.body.case.type_label, "Request");
  assert.equal(api.audit.at(-1).event_type, "tournament_case.created");
  const [row] = await all(db, "SELECT * FROM tournament_cases");
  assert.equal(row.submitted_by_user_id, 1);
  assert.equal(row.details, "When is the draw?");

  const feedback = await api.call(route, { user, params: { tournamentId: "CCL-2027" }, body: { case_type: "feedback", details: "Great" } });
  assert.equal(feedback.status, 201);
  assert.equal(feedback.body.case.category, null);

  const matchCategory = await api.call(route, {
    user, params: { tournamentId: "CCL-2027" }, body: { case_type: "problem", category: "scheduling", details: "x" },
  });
  assert.equal(matchCategory.status, 400);
  const empty = await api.call(route, { user, params: { tournamentId: "CCL-2027" }, body: { case_type: "feedback", details: " " } });
  assert.equal(empty.status, 400);
  const missing = await api.call(route, { user, params: { tournamentId: "NOPE" }, body: { case_type: "feedback", details: "x" } });
  assert.equal(missing.status, 404);
  const noProfile = await api.call(route, { user: { id: 1 }, params: { tournamentId: "CCL-2027" }, body: { case_type: "feedback", details: "x" } });
  assert.equal(noProfile.status, 403);
});
