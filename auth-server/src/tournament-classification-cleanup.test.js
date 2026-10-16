import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import { mkdtemp, readdir, rm, stat, readFile } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { promisify } from "node:util";
import test from "node:test";
import sqlite3 from "sqlite3";
import { ensureCategoriesSchema } from "./categories.js";
import { TOURNAMENT_CATEGORIES } from "./tournament-classification.js";
import { applyTournamentClassificationPlan, buildTournamentClassificationPlan, openClassificationDatabase } from "./tournament-classification-migration.js";
import { applyTournamentCleanupPlan, buildTournamentCleanupPlan, LEGACY_TOURNAMENT_COLUMNS } from "./tournament-classification-cleanup.js";

const all = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows)));
const run = (db, sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, (error) => error ? reject(error) : resolve()));
const close = (db) => new Promise((resolve) => db.close(resolve));
const retained = (row) => Object.fromEntries(Object.entries(row).filter(([field]) => !LEGACY_TOURNAMENT_COLUMNS.includes(field)));
const cli = new URL("./cleanup-tournament-classification.js", import.meta.url).pathname;
const execute = promisify(execFile);

async function fixture(t) {
  const folder = await mkdtemp(path.join(tmpdir(), "tournament cleanup "));
  const filename = path.join(folder, "auth.sqlite");
  const initial = new sqlite3.Database(filename);
  await close(initial);
  const db = await openClassificationDatabase(filename, { readOnly: false });
  t.after(async () => { await close(db); await rm(folder, { recursive: true, force: true }); });
  await run(db, "PRAGMA journal_mode = WAL");
  await run(db, "PRAGMA wal_autocheckpoint = 0");
  await run(db, "PRAGMA foreign_keys = ON");
  await run(db, readFileSync(new URL("./fixtures/tournaments-legacy.sql", import.meta.url), "utf8"));
  await ensureCategoriesSchema(db);
  for (const rule of TOURNAMENT_CATEGORIES) {
    await run(db, `INSERT INTO tournaments (id, name, tournament_type, category, subtype, access_type,
      about, rules, player_hub_visibility, standings_scoring, tpr_target_games, tpr_smoothing, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, 'Keep about', 'Keep rules', 'Hidden', ?, 17, 0.8, '2000-01-01')`,
    [rule.name, rule.name, rule.type, rule.name, rule.type === "Teams" ? (rule.mode === "official" ? "Official" : "Friendly") : null,
      rule.mode === "official" ? "Official" : "Friendly", rule.name === "Rivals" ? "bounty_tpr" : "standard"]);
  }
  await run(db, "UPDATE tournaments SET team_type = 'Club' WHERE id = 'Clubs'");
  await run(db, "CREATE TABLE matches (id TEXT PRIMARY KEY, tournament_id TEXT REFERENCES tournaments(id), status TEXT)");
  await run(db, "INSERT INTO matches VALUES ('match', 'ETCOC', 'Planned')");
  await run(db, "CREATE TABLE games (id TEXT PRIMARY KEY, replay BLOB)");
  await run(db, "INSERT INTO games VALUES ('game', x'01020304')");
  await run(db, "CREATE TABLE tournament_access_users (tournament_id TEXT, user_id INTEGER, role TEXT)");
  await run(db, "INSERT INTO tournament_access_users VALUES ('ETCOC', 7, 'admin'), ('ETCOC', 8, 'captain')");
  await run(db, "CREATE TABLE tournament_teams (tournament_id TEXT, team_id TEXT, captain_id TEXT)");
  await run(db, "INSERT INTO tournament_teams VALUES ('ETCOC', 'UA', 'player')");
  await run(db, "CREATE TABLE challenge_periods (id TEXT PRIMARY KEY, rivals_tournament_id TEXT REFERENCES tournaments(id))");
  await run(db, "INSERT INTO challenge_periods VALUES ('period', 'Rivals')");
  await run(db, "CREATE TABLE news_editors (tournament_id TEXT REFERENCES tournaments(id), access_type TEXT)");
  await run(db, "INSERT INTO news_editors VALUES ('ETCOC', 'tournament')");
  await run(db, "CREATE INDEX idx_tournament_category ON tournaments(category)");
  await run(db, "CREATE VIEW tournament_names AS SELECT id, name FROM tournaments");
  const backupDir = path.join(folder, "backups");
  await applyTournamentClassificationPlan(db, await buildTournamentClassificationPlan(db), { backupDir });
  return { db, filename, folder, backupDir };
}

test("cleanup previews all eleven categories without changing schema, rows or migration history", async (t) => {
  const { db, filename } = await fixture(t);
  const before = await all(db, "SELECT * FROM tournaments ORDER BY id");
  const reader = await openClassificationDatabase(filename);
  try {
    const plan = await buildTournamentCleanupPlan(reader);
    assert.deepEqual(plan.issues, []);
    assert.equal(plan.summary.blocked, 0);
    assert.equal(plan.summary.columns_to_remove, 4);
    assert.equal(plan.summary.tournaments, 11);
    assert.equal(plan.source_snapshot.tables.tournament_classification_migration_rows.count, 11);
    assert.deepEqual(await all(db, "SELECT * FROM tournaments ORDER BY id"), before);
    assert.equal((await buildTournamentCleanupPlan(db)).plan_hash, plan.plan_hash);
  } finally { await close(reader); }
});

test("cleanup removes only four columns, verifies a WAL backup, preserves all data and is repeatable", async (t) => {
  const { db, backupDir, filename } = await fixture(t);
  const before = await all(db, "SELECT * FROM tournaments ORDER BY id");
  const plan = await buildTournamentCleanupPlan(db);
  assert.ok((await stat(`${filename}-wal`)).size > 0);
  const result = await applyTournamentCleanupPlan(db, plan, { backupDir });
  assert.deepEqual(result.removed_columns, LEGACY_TOURNAMENT_COLUMNS);
  assert.equal((await stat(result.backup_path)).mode & 0o777, 0o600);
  const copy = await openClassificationDatabase(result.backup_path);
  try { assert.deepEqual(await all(copy, "SELECT * FROM tournaments ORDER BY id"), before); }
  finally { await close(copy); }
  assert.deepEqual(await all(db, "SELECT * FROM tournaments ORDER BY id"), before.map(retained));
  const after = await buildTournamentCleanupPlan(db);
  assert.equal(after.summary.already_applied, true);
  for (const [table, summary] of Object.entries(plan.source_snapshot.tables)) {
    if (table !== "tournaments") assert.deepEqual(after.source_snapshot.tables[table], summary, table);
  }
  assert.deepEqual(await all(db, "PRAGMA foreign_key_check"), []);
  assert.deepEqual(await all(db, "PRAGMA integrity_check"), [{ integrity_check: "ok" }]);
  assert.equal((await applyTournamentCleanupPlan(db, plan, { backupDir })).already_applied, true);
  assert.equal((await readdir(backupDir)).filter((file) => file.startsWith("tournament-classification-cleanup") && file.endsWith(".sqlite")).length, 1);
  await assert.rejects(buildTournamentClassificationPlan(db, { rollback: true }), { code: "LEGACY_COLUMNS_REMOVED" });
});

test("unmigrated or invalid pairs block removal; obsolete column drift is not used as configuration", async (t) => {
  const { db, backupDir } = await fixture(t);
  for (const update of ["type = NULL", "category = 'Unknown'", "type = 'Individuals'"]) {
    await run(db, `UPDATE tournaments SET ${update} WHERE id = 'ETCOC'`);
    const plan = await buildTournamentCleanupPlan(db);
    assert.equal(plan.summary.blocked, 1);
    await assert.rejects(applyTournamentCleanupPlan(db, plan, { backupDir }), { code: "CLEANUP_BLOCKED" });
    await run(db, "UPDATE tournaments SET type = 'Teams', category = 'ETCOC' WHERE id = 'ETCOC'");
  }
  // The new backend no longer synchronizes obsolete columns during the rollout.
  await run(db, "UPDATE tournaments SET subtype = 'Friendly', access_type = 'Friendly' WHERE id = 'ETCOC'");
  const plan = await buildTournamentCleanupPlan(db);
  assert.equal(plan.summary.blocked, 0);
  assert.equal(plan.rows.find((row) => row.id === "ETCOC").access, "Official");
});

test("edited and stale reports and unavailable backup destinations cannot remove columns", async (t) => {
  const { db, backupDir, filename } = await fixture(t);
  const plan = await buildTournamentCleanupPlan(db);
  await assert.rejects(applyTournamentCleanupPlan(db, { ...plan, removed_columns: [] }, { backupDir }), { code: "INVALID_PLAN" });
  await run(db, "UPDATE tournaments SET about = 'New text' WHERE id = 'Rivals'");
  await assert.rejects(applyTournamentCleanupPlan(db, plan, { backupDir }), { code: "STALE_PLAN" });
  await assert.rejects(applyTournamentCleanupPlan(db, await buildTournamentCleanupPlan(db), { backupDir: path.join(filename, "invalid") }));
  assert.equal((await all(db, "PRAGMA table_info(tournaments)")).some((column) => column.name === "subtype"), true);
});

test("custom indexes and views referencing removed fields block cleanup without dropping dependencies", async (t) => {
  const { db, backupDir } = await fixture(t);
  await run(db, "CREATE INDEX legacy_type_idx ON tournaments(tournament_type)");
  await run(db, "CREATE VIEW legacy_type_view AS SELECT subtype FROM tournaments");
  const plan = await buildTournamentCleanupPlan(db);
  assert.ok(plan.issues.includes("DEPENDENT_SCHEMA_OBJECT:index:legacy_type_idx"));
  assert.ok(plan.issues.includes("DEPENDENT_SCHEMA_OBJECT:view:legacy_type_view"));
  await assert.rejects(applyTournamentCleanupPlan(db, plan, { backupDir }), { code: "CLEANUP_BLOCKED" });
  assert.equal((await all(db, "SELECT * FROM legacy_type_view")).length, 11);
});

test("a later SQL failure rolls back every column already dropped", async (t) => {
  const { db, backupDir } = await fixture(t);
  await run(db, "ALTER TABLE tournaments ADD COLUMN custom TEXT CHECK (team_type <> 'Invalid')");
  const before = await all(db, "SELECT * FROM tournaments ORDER BY id");
  const plan = await buildTournamentCleanupPlan(db);
  // First DROP succeeds, second fails because of the retained CHECK constraint.
  await assert.rejects(applyTournamentCleanupPlan(db, plan, { backupDir }), /team_type/);
  assert.deepEqual(await all(db, "SELECT * FROM tournaments ORDER BY id"), before);
  assert.equal((await buildTournamentCleanupPlan(db)).plan_hash, plan.plan_hash);
});

test("a write during backup is caught under the write lock and preserved", async (t) => {
  const { db, filename, backupDir } = await fixture(t);
  const writer = await openClassificationDatabase(filename, { readOnly: false });
  t.after(() => close(writer));
  const original = db.backup.bind(db);
  db.backup = (...args) => {
    const backup = original(...args);
    const finish = backup.finish.bind(backup);
    backup.finish = (done) => finish(async (error) => {
      await run(writer, "INSERT INTO matches VALUES ('new', 'ETCOC', 'Planned')");
      done(error);
    });
    return backup;
  };
  await assert.rejects(applyTournamentCleanupPlan(db, await buildTournamentCleanupPlan(db), { backupDir }), { code: "STALE_PLAN" });
  assert.equal((await all(db, "SELECT * FROM matches WHERE id = 'new'")).length, 1);
  assert.equal((await all(db, "PRAGMA table_info(tournaments)")).some((column) => column.name === "subtype"), true);
});

test("CLI is read-only by default, protects its report and requires a plan for removal", async (t) => {
  const { filename, folder, backupDir } = await fixture(t);
  const report = path.join(folder, "cleanup.json");
  const preview = JSON.parse((await execute(process.execPath, [cli, "--db-path", filename, "--report", report])).stdout);
  assert.equal(preview.summary.columns_to_remove, 4);
  assert.equal((await stat(report)).mode & 0o777, 0o600);
  await assert.rejects(execute(process.execPath, [cli, "--db-path", filename, "--apply"]));
  const applied = JSON.parse((await execute(process.execPath, [cli, "--db-path", filename, "--apply", "--plan", report, "--backup-dir", backupDir])).stdout);
  assert.equal(applied.applied, true);
  const reader = await openClassificationDatabase(filename);
  try { assert.equal((await buildTournamentCleanupPlan(reader)).summary.already_applied, true); }
  finally { await close(reader); }
  assert.ok(JSON.parse(await readFile(report, "utf8")).plan_hash);
  const absent = path.join(folder, "absent.sqlite");
  await assert.rejects(execute(process.execPath, [cli, "--db-path", absent]));
  await assert.rejects(stat(absent), { code: "ENOENT" });
});
