import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import { mkdtemp, readdir, rm, stat, readFile } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { promisify } from "node:util";
import test from "node:test";
import {
  applyTournamentClassificationPlan, buildTournamentClassificationPlan, openClassificationDatabase,
} from "./tournament-classification-migration.js";
import { ensureCategoriesSchema } from "./categories.js";

const source = readFileSync(new URL("./server.js", import.meta.url), "utf8");
const cli = new URL("./migrate-tournament-classification.js", import.meta.url).pathname;
const execute = promisify(execFile);
const fields = ["type", "tournament_type", "team_type", "category", "subtype", "access_type"];
const fixtures = [
  ["actual-id-a", "Asian Cup 2026", "Teams", "Asian Cup", "Official", "Hidden"],
  ["actual-id-b", "Carcassonne Champions League 2027 Qualifications", "Individuals", "CCL", null, "Visible"],
  ["actual-id-c", "Copa America 2026", "Teams", "Copa America", "Official", "Hidden"],
  ["actual-id-d", "European Team Carcassonne Online Championship 2026", "Teams", "ETCOC", "Official", "Visible"],
  ["Friendly-Matches", "Team Friendly Matches", "Teams", null, "Friendly", "Visible"],
  ["actual-id-f", "GG Rivals: Season 1", "Individuals", "Rivals", null, "Hidden"],
  ["actual-id-g", "King of Carcassonne Tournaments", "Individuals", "King of Carcassonne", null, "Hidden"],
  ["actual-id-h", "Mind Sport Olympiad Grand Prix 2026", "Individuals", "MSO", null, "Hidden"],
  ["actual-id-i", "World Team Carcassonne Online Championship 2025", "Teams", "WTCOC", "Official", "Hidden"],
  ["actual-id-j", "World Team Carcassonne Online Championship 2026", "Teams", "WTCOC", "Official", "Hidden"],
];
const all = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows)));
const run = (db, sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, (error) => error ? reject(error) : resolve()));
const close = (db) => new Promise((resolve) => db.close(resolve));
const classification = (row) => Object.fromEntries(fields.map((field) => [field, row[field] ?? null]));

async function fixture(t) {
  const folder = await mkdtemp(path.join(tmpdir(), "tournament migration "));
  const filename = path.join(folder, "auth.sqlite");
  // Create an existing file explicitly; the migration never creates a database.
  const sqlite3 = (await import("sqlite3")).default;
  const initial = new sqlite3.Database(filename);
  await close(initial);
  const db = await openClassificationDatabase(filename, { readOnly: false });
  t.after(async () => { await close(db); await rm(folder, { recursive: true, force: true }); });
  await run(db, "PRAGMA journal_mode = WAL");
  await run(db, "PRAGMA wal_autocheckpoint = 0");
  await run(db, "PRAGMA foreign_keys = ON");
  const schema = source.match(/function ensureTournamentsSchema\(\) \{\s*db.run\(`([\s\S]*?)`/)[1]
    .replace("      type TEXT CHECK (type IN ('Teams', 'Individuals')),\n", "");
  await run(db, schema);
  await ensureCategoriesSchema(db);
  for (const [id, name, type, category, subtype, visibility] of fixtures) {
    await run(db, `INSERT INTO tournaments (id, name, tournament_type, category, subtype, access_type,
      player_hub_visibility, standings_scoring, tpr_target_games, tpr_smoothing, about, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, 17, 0.8, 'Keep metadata', '2000-01-01 00:00:00')`,
    [id, name, type, category, subtype, subtype || "Friendly", visibility, category === "Rivals" ? "bounty_tpr" : "standard"]);
  }
  const statements = [
    `CREATE TABLE tournament_access_users (tournament_entity_type TEXT, tournament_id TEXT, user_id INTEGER, role TEXT,
      PRIMARY KEY (tournament_entity_type, tournament_id, user_id)) WITHOUT ROWID`,
    "INSERT INTO tournament_access_users VALUES ('tournament','actual-id-a',7,'admin'), ('tournament','actual-id-a',8,'captain'), ('in_person_tournament','separate',9,'admin')",
    "CREATE TABLE tournament_teams (tournament_id TEXT, team_id TEXT, captain_id TEXT)",
    "INSERT INTO tournament_teams VALUES ('actual-id-a','UA','captain-1')",
    "CREATE TABLE tournament_players (tournament_id TEXT, team_id TEXT, player_id TEXT, captain INTEGER)",
    "INSERT INTO tournament_players VALUES ('actual-id-a','UA','captain-1',1)",
    "CREATE TABLE matches (id TEXT PRIMARY KEY, tournament_id TEXT REFERENCES tournaments(id), status TEXT, score INTEGER, gg_rating INTEGER)",
    "INSERT INTO matches VALUES ('match','actual-id-a','Done',3,5)",
    "CREATE TABLE duels (id TEXT PRIMARY KEY, match_id TEXT REFERENCES matches(id), tournament_id TEXT, score INTEGER)",
    "INSERT INTO duels VALUES ('duel','match','actual-id-a',2)",
    "CREATE TABLE games (id TEXT PRIMARY KEY, duel_id TEXT REFERENCES duels(id), score INTEGER, replay BLOB)",
    "INSERT INTO games VALUES ('game','duel',123,x'01020304')",
    "CREATE TABLE standings (tournament_id TEXT, team_id TEXT, points REAL)",
    "INSERT INTO standings VALUES ('actual-id-a','UA',9.5)",
    "CREATE TABLE challenge_periods (id TEXT PRIMARY KEY, rivals_tournament_id TEXT REFERENCES tournaments(id), status TEXT)",
    "INSERT INTO challenge_periods VALUES ('period','actual-id-f','active')",
    "CREATE TABLE in_person_tournaments (id TEXT PRIMARY KEY, name TEXT)",
    "INSERT INTO in_person_tournaments VALUES ('separate','Untouched')",
    "CREATE TABLE icons (id INTEGER PRIMARY KEY, category TEXT, updated_at TEXT)",
    "INSERT INTO icons VALUES (1,'King of Carcassonne','2001-01-01')",
    "CREATE TABLE news (id INTEGER PRIMARY KEY, category TEXT, title TEXT)",
    "INSERT INTO news VALUES (1,'King of Carcassonne','Keep news')",
  ];
  for (const sql of statements) await run(db, sql);
  return { db, filename, folder, backupDir: path.join(folder, "backups") };
}

test("dry-run uses actual IDs, covers all ten supplied tournaments, and does not change schema or data", async (t) => {
  const { db } = await fixture(t);
  const before = await all(db, "SELECT * FROM tournaments ORDER BY id");
  const plan = await buildTournamentClassificationPlan(db);
  assert.deepEqual(plan.issues, []);
  assert.equal(plan.summary.blocked, 0);
  assert.equal(plan.summary.changed, 10);
  assert.equal(plan.type_column_exists, false);
  assert.deepEqual(plan.rows.map((row) => row.id).sort(), fixtures.map((row) => row[0]).sort());
  assert.equal(plan.rows.find((row) => row.id === "Friendly-Matches").after.category, "Friendly");
  assert.equal(plan.source_snapshot.tables.tournament_access_users.count, 3);
  assert.deepEqual(await all(db, "SELECT * FROM tournaments ORDER BY id"), before);
  assert.equal((await all(db, "PRAGMA table_info(tournaments)")).some((column) => column.name === "type"), false);
  assert.equal((await all(db, "SELECT name FROM sqlite_master WHERE name LIKE 'tournament_classification_migration%'")).length, 0);
});

test("apply creates a verified WAL-aware backup, preserves metadata and related tables, and is repeatable", async (t) => {
  const { db, filename, backupDir } = await fixture(t);
  const before = await all(db, "SELECT * FROM tournaments ORDER BY id");
  assert.ok((await stat(`${filename}-wal`)).size > 0);
  const plan = await buildTournamentClassificationPlan(db);
  const result = await applyTournamentClassificationPlan(db, plan, { backupDir });
  assert.equal(result.changed, 10);
  assert.equal((await stat(result.backup_path)).mode & 0o777, 0o600);
  const backup = await openClassificationDatabase(result.backup_path);
  try {
    assert.deepEqual(await all(backup, "SELECT * FROM tournaments ORDER BY id"), before);
    assert.deepEqual(await all(backup, "PRAGMA integrity_check"), [{ integrity_check: "ok" }]);
  } finally { await close(backup); }
  for (const old of before) {
    const [stored] = await all(db, "SELECT * FROM tournaments WHERE id = ?", [old.id]);
    const target = plan.rows.find((row) => row.id === old.id).after;
    assert.deepEqual(stored, { ...old, ...target });
  }
  const repeated = await applyTournamentClassificationPlan(db, plan, { backupDir });
  assert.equal(repeated.already_applied, true);
  const fresh = await buildTournamentClassificationPlan(db);
  assert.equal(fresh.summary.already_applied, true);
  assert.equal(fresh.summary.changed, 0);
  assert.equal((await all(db, "SELECT * FROM tournament_classification_migration_rows")).length, 10);
  assert.equal((await readdir(backupDir)).filter((name) => name.endsWith(".sqlite")).length, 1);
});

test("legacy National and KoC map without changing access, and the old fields remain available", async (t) => {
  const { db, backupDir } = await fixture(t);
  await run(db, "INSERT INTO tournaments (id, name, tournament_type) VALUES ('national-real-id','Country Cup','National')");
  await run(db, "UPDATE tournaments SET category = 'KoC' WHERE id = 'actual-id-g'");
  const plan = await buildTournamentClassificationPlan(db);
  assert.equal(plan.summary.blocked, 0);
  assert.equal(plan.rows.find((row) => row.id === "national-real-id").after.type, "Individuals");
  await applyTournamentClassificationPlan(db, plan, { backupDir });
  const [national] = await all(db, "SELECT * FROM tournaments WHERE id = 'national-real-id'");
  assert.equal(national.type, "Individuals");
  assert.equal(national.category, "National");
  assert.equal(national.tournament_type, "Individuals");
  assert.equal((await all(db, "SELECT category FROM tournaments WHERE id = 'actual-id-g'"))[0].category, "King of Carcassonne");
});

test("startup backfill preserves migrated classification and leaves rollback ready", async (t) => {
  const { db, backupDir } = await fixture(t);
  await applyTournamentClassificationPlan(db, await buildTournamentClassificationPlan(db), { backupDir });
  const before = (await all(db, "SELECT * FROM tournaments ORDER BY id")).map(classification);
  const section = source.slice(source.indexOf("function ensureTournamentsSchema()"), source.indexOf("function ensureTournamentTeamIndexes()"));
  const backfill = section.match(/db.run\(\s*`(\s*UPDATE tournaments[\s\S]*?)`/)[1];
  await run(db, backfill);
  assert.deepEqual((await all(db, "SELECT * FROM tournaments ORDER BY id")).map(classification), before);
  assert.equal((await buildTournamentClassificationPlan(db, { rollback: true })).summary.blocked, 0);
});

test("changed data and edited reports cannot be applied", async (t) => {
  const { db, backupDir } = await fixture(t);
  const plan = await buildTournamentClassificationPlan(db);
  const edited = structuredClone(plan);
  edited.rows[0].after.category = "Clubs";
  await assert.rejects(applyTournamentClassificationPlan(db, edited, { backupDir }), { code: "INVALID_PLAN" });
  await run(db, "INSERT INTO matches (id, tournament_id, status) VALUES ('new-match','actual-id-a','Planned')");
  await assert.rejects(applyTournamentClassificationPlan(db, plan, { backupDir }), { code: "STALE_PLAN" });
  assert.equal((await all(db, "SELECT * FROM matches WHERE id='new-match'")).length, 1);
  await assert.rejects(stat(backupDir), { code: "ENOENT" });
});

test("unknown categories, ambiguous IDs and permission changes block migration without guessing", async (t) => {
  const { db, backupDir } = await fixture(t);
  await run(db, "UPDATE tournaments SET subtype = 'Friendly', access_type = 'Friendly' WHERE id = 'actual-id-a'");
  await run(db, "UPDATE tournaments SET category = 'Custom Old Category' WHERE id = 'actual-id-b'");
  await run(db, "INSERT INTO tournaments (id, name, category) VALUES ('ACTUAL-ID-A','Ambiguous','Friendly')");
  const plan = await buildTournamentClassificationPlan(db);
  assert.ok(plan.issues.includes("AMBIGUOUS_ID:actual-id-a") || plan.issues.includes("AMBIGUOUS_ID:ACTUAL-ID-A"));
  assert.ok(plan.rows.find((row) => row.id === "actual-id-a").blockers.includes("ACCESS_BEHAVIOR_CHANGE"));
  assert.ok(plan.rows.find((row) => row.id === "actual-id-b").blockers.includes("UNMAPPED_CATEGORY"));
  await assert.rejects(applyTournamentClassificationPlan(db, plan, { backupDir }), { code: "MIGRATION_BLOCKED" });
  await assert.rejects(stat(backupDir), { code: "ENOENT" });
});

test("an unavailable backup destination prevents all database changes", async (t) => {
  const { db, filename } = await fixture(t);
  const before = await all(db, "SELECT * FROM tournaments ORDER BY id");
  await assert.rejects(applyTournamentClassificationPlan(db, await buildTournamentClassificationPlan(db), {
    backupDir: path.join(filename, "cannot-be-a-directory"),
  }));
  assert.deepEqual(await all(db, "SELECT * FROM tournaments ORDER BY id"), before);
  assert.equal((await all(db, "PRAGMA table_info(tournaments)")).some((column) => column.name === "type"), false);
});

test("a concurrent write after backup is caught again under the migration write lock", async (t) => {
  const { db, filename, backupDir } = await fixture(t);
  const other = await openClassificationDatabase(filename, { readOnly: false });
  t.after(() => close(other));
  const nativeBackup = db.backup.bind(db);
  db.backup = (...args) => {
    const backup = nativeBackup(...args);
    const finish = backup.finish.bind(backup);
    backup.finish = (done) => finish(async (error) => {
      await run(other, "INSERT INTO matches (id, tournament_id) VALUES ('racing-match','actual-id-a')");
      done(error);
    });
    return backup;
  };
  const plan = await buildTournamentClassificationPlan(db);
  await assert.rejects(applyTournamentClassificationPlan(db, plan, { backupDir }), { code: "STALE_PLAN" });
  assert.equal((await all(db, "SELECT * FROM matches WHERE id='racing-match'")).length, 1);
  assert.equal((await all(db, "PRAGMA table_info(tournaments)")).some((column) => column.name === "type"), false);
});

test("rollback preserves later matches, new tournaments and metadata edits", async (t) => {
  const { db, backupDir } = await fixture(t);
  const old = await all(db, "SELECT * FROM tournaments ORDER BY id");
  await applyTournamentClassificationPlan(db, await buildTournamentClassificationPlan(db), { backupDir });
  await run(db, "UPDATE tournaments SET about = 'Edited later' WHERE id = 'actual-id-f'");
  await run(db, "INSERT INTO matches (id, tournament_id, status) VALUES ('later-match','actual-id-a','Planned')");
  await run(db, "INSERT INTO tournaments (id, name, type, tournament_type, category) VALUES ('later-cup','Later','Individuals','Individuals','National')");
  const plan = await buildTournamentClassificationPlan(db, { rollback: true });
  assert.equal(plan.summary.blocked, 0);
  const result = await applyTournamentClassificationPlan(db, plan, { backupDir });
  assert.equal(result.direction, "down");
  for (const previous of old) {
    const [stored] = await all(db, "SELECT * FROM tournaments WHERE id = ?", [previous.id]);
    assert.deepEqual(classification(stored), classification(previous));
  }
  assert.equal((await all(db, "SELECT about FROM tournaments WHERE id='actual-id-f'"))[0].about, "Edited later");
  assert.equal((await all(db, "SELECT * FROM matches WHERE id='later-match'")).length, 1);
  assert.equal((await all(db, "SELECT type FROM tournaments WHERE id='later-cup'"))[0].type, "Individuals");
  assert.equal((await buildTournamentClassificationPlan(db, { rollback: true })).summary.already_rolled_back, true);
  const reapplied = await applyTournamentClassificationPlan(db, await buildTournamentClassificationPlan(db), { backupDir });
  assert.equal(reapplied.applied, true);
  assert.equal((await all(db, "SELECT COUNT(*) AS count FROM tournament_classification_migrations"))[0].count, 2);
});

test("rollback refuses to overwrite a classification edited since migration", async (t) => {
  const { db, backupDir } = await fixture(t);
  await applyTournamentClassificationPlan(db, await buildTournamentClassificationPlan(db), { backupDir });
  await run(db, "UPDATE tournaments SET category = 'ETCOC' WHERE id = 'actual-id-a'");
  const plan = await buildTournamentClassificationPlan(db, { rollback: true });
  assert.ok(plan.rows.find((row) => row.id === "actual-id-a").blockers.includes("CLASSIFICATION_CHANGED_SINCE_MIGRATION"));
  await assert.rejects(applyTournamentClassificationPlan(db, plan, { backupDir }), { code: "MIGRATION_BLOCKED" });
});

test("trigger side effects roll back classification, journal and schema changes", async (t) => {
  const { db, backupDir } = await fixture(t);
  await run(db, "CREATE TRIGGER corrupt_related AFTER UPDATE OF category ON tournaments BEGIN UPDATE games SET score=999; END");
  const before = await all(db, "SELECT * FROM tournaments ORDER BY id");
  const plan = await buildTournamentClassificationPlan(db);
  await assert.rejects(applyTournamentClassificationPlan(db, plan, { backupDir }), { code: "RELATED_DATA_CHANGED" });
  assert.deepEqual(await all(db, "SELECT * FROM tournaments ORDER BY id"), before);
  assert.equal((await all(db, "SELECT score FROM games"))[0].score, 123);
  assert.equal((await all(db, "PRAGMA table_info(tournaments)")).some((column) => column.name === "type"), false);
  assert.equal((await all(db, "SELECT name FROM sqlite_master WHERE name LIKE 'tournament_classification_migration%'")).length, 0);
});

test("a mid-migration SQLite error restores every earlier update", async (t) => {
  const { db, backupDir } = await fixture(t);
  await run(db, `CREATE TRIGGER reject_update BEFORE UPDATE OF category ON tournaments
    WHEN OLD.id = 'actual-id-b' BEGIN SELECT RAISE(ABORT, 'stop migration'); END`);
  const before = await all(db, "SELECT * FROM tournaments ORDER BY id");
  await assert.rejects(applyTournamentClassificationPlan(db, await buildTournamentClassificationPlan(db), { backupDir }), /stop migration/);
  assert.deepEqual(await all(db, "SELECT * FROM tournaments ORDER BY id"), before);
});

test("CLI defaults to read-only preview and applies only its reviewed report", async (t) => {
  const { db, filename, folder, backupDir } = await fixture(t);
  const report = path.join(folder, "reviewed plan.json");
  const args = [cli, "--db-path", filename];
  const preview = JSON.parse((await execute(process.execPath, [...args, "--report", report])).stdout);
  assert.equal(preview.summary.changed, 10);
  assert.equal((await all(db, "PRAGMA table_info(tournaments)")).some((column) => column.name === "type"), false);
  assert.equal(JSON.parse(await readFile(report, "utf8")).plan_hash, preview.plan_hash);
  const applied = JSON.parse((await execute(process.execPath, [...args, "--apply", "--plan", report, "--backup-dir", backupDir])).stdout);
  assert.equal(applied.ok, true);
  assert.equal(applied.changed, 10);
  assert.equal((await all(db, "SELECT COUNT(*) AS count FROM tournaments WHERE type IS NOT NULL"))[0].count, 10);
  const rollbackReport = path.join(folder, "rollback plan.json");
  await execute(process.execPath, [...args, "--rollback", "--report", rollbackReport]);
  await assert.rejects(execute(process.execPath, [...args, "--rollback", "--apply", "--plan", report]),
    (error) => /direction does not match/.test(error.stderr));
  const rolledBack = JSON.parse((await execute(process.execPath, [...args, "--rollback", "--apply", "--plan", rollbackReport, "--backup-dir", backupDir])).stdout);
  assert.equal(rolledBack.direction, "down");
  assert.equal((await all(db, "SELECT COUNT(*) AS count FROM tournaments WHERE type IS NOT NULL"))[0].count, 0);
  await assert.rejects(execute(process.execPath, [...args, "--apply"]), (error) => /requires a reviewed --plan/.test(error.stderr));
  const missing = path.join(folder, "does-not-exist.sqlite");
  await assert.rejects(execute(process.execPath, [cli, "--db-path", missing]));
  await assert.rejects(stat(missing), { code: "ENOENT" });
});
