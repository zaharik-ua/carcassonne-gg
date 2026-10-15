import { createHash, randomUUID } from "node:crypto";
import { mkdir, open, unlink } from "node:fs/promises";
import path from "node:path";
import sqlite3 from "sqlite3";
import { getTournamentCategory, readTournamentClassification, TOURNAMENT_CATEGORIES } from "./tournament-classification.js";

const VERSION = "tournament-classification-v1";
const RUNS = "tournament_classification_migrations";
const ROWS = "tournament_classification_migration_rows";
const FIELDS = ["type", "tournament_type", "team_type", "category", "subtype", "access_type"];
const key = (value) => String(value ?? "").trim().replace(/\s+/g, " ").toLowerCase();
const quote = (value) => `"${value.replaceAll('"', '""')}"`;
const pick = (row) => Object.fromEntries(FIELDS.map((field) => [field, row[field] ?? null]));
function canonical(value) {
  if (Buffer.isBuffer(value)) return { blob: value.toString("hex") };
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") return Object.fromEntries(Object.keys(value).sort().map((name) => [name, canonical(value[name])]));
  return value;
}
const json = (value) => JSON.stringify(canonical(value));
const digest = (value) => createHash("sha256").update(json(value)).digest("hex");
const equal = (left, right) => json(left) === json(right);
const failure = (code, message) => Object.assign(new Error(message), { code });
const all = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows)));
const run = (db, sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, function done(error) {
  if (error) reject(error); else resolve(this);
}));
const close = (db) => new Promise((resolve, reject) => db.close((error) => error ? reject(error) : resolve()));

export function openClassificationDatabase(filename, { readOnly = true } = {}) {
  return new Promise((resolve, reject) => {
    // Never create a database when the path is wrong.
    const db = new sqlite3.Database(filename, readOnly ? sqlite3.OPEN_READONLY : sqlite3.OPEN_READWRITE, (error) => {
      if (error) reject(error);
      else { db.configure("busyTimeout", 5000); resolve(db); }
    });
  });
}

async function tableColumns(db, table) {
  return all(db, `PRAGMA table_info(${quote(table)})`);
}
async function activeRun(db) {
  if (!(await tableColumns(db, RUNS)).length) return null;
  return (await all(db, `SELECT * FROM ${RUNS} WHERE version = ? AND status = 'applied' ORDER BY rowid DESC LIMIT 1`, [VERSION]))[0] || null;
}

// Stream table contents rather than keeping games/replays in memory. Primary keys
// give a stable ordering, including WITHOUT ROWID tables with composite keys.
async function snapshot(db, { omitClassification = false } = {}) {
  const schema = (await all(db, `SELECT type, name, tbl_name, sql FROM sqlite_master
    WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name`))
    .filter((item) => ![RUNS, ROWS].includes(item.tbl_name));
  const tables = {};
  for (const table of schema.filter((item) => item.type === "table")) {
    const columns = await tableColumns(db, table.name);
    const primary = columns.filter((column) => column.pk).sort((a, b) => a.pk - b.pk);
    const order = (primary.length ? primary : columns).map((column) => quote(column.name)).join(", ");
    const hash = createHash("sha256");
    let count = 0;
    await new Promise((resolve, reject) => db.each(`SELECT * FROM ${quote(table.name)} ORDER BY ${order}`, (error, row) => {
      if (error) return reject(error);
      const data = omitClassification && table.name === "tournaments"
        ? Object.fromEntries(Object.entries(row).filter(([field]) => !FIELDS.includes(field))) : row;
      hash.update(json(data)); hash.update("\n"); count += 1;
    }, (error) => error ? reject(error) : resolve()));
    tables[table.name] = { count, sha256: hash.digest("hex") };
  }
  return { schema_sha256: digest(schema), tables };
}

async function foreignKeys(db) {
  try { return { violations: await all(db, "PRAGMA foreign_key_check") }; }
  catch (error) { return { diagnostic_error: error.message }; }
}

function legacyProfile(row) {
  const raw = key(row.tournament_type);
  const type = ["individual", "individuals", "national", "nationals"].includes(raw) ? "Individuals"
    : ["team", "teams", "club", "clubs"].includes(raw) ? "Teams" : null;
  const mode = key(row.subtype) || key(row.access_type);
  return {
    type,
    team_type: ["club", "clubs"].includes(raw) || key(row.team_type) === "club" ? "Club" : "National",
    access_type: ["official", "closed", "2"].includes(mode) ? "Official" : "Friendly",
  };
}

function planRow(row) {
  const target = readTournamentClassification(row);
  const after = { type: target.type, tournament_type: target.type, team_type: target.team_type,
    category: target.category, subtype: target.subtype, access_type: target.access_type };
  const before = pick(row);
  const legacy = legacyProfile(row);
  const blockers = [];
  if (!String(row.id || "").trim()) blockers.push("EMPTY_ID");
  if (!getTournamentCategory(target.category)) blockers.push("UNMAPPED_CATEGORY");
  if (key(row.category) && !getTournamentCategory(row.category)) blockers.push("UNKNOWN_LEGACY_CATEGORY");
  if (!legacy.type) blockers.push("UNKNOWN_LEGACY_TYPE");
  else if (legacy.type !== target.type) blockers.push("TYPE_BEHAVIOR_CHANGE");
  if (target.type === "Teams" && legacy.team_type !== target.team_type) blockers.push("TEAM_KIND_CHANGE");
  if (legacy.access_type !== target.access_type) blockers.push("ACCESS_BEHAVIOR_CHANGE");
  if (row.type != null && row.type !== target.type) blockers.push("STORED_TYPE_CONFLICT");
  for (const field of ["subtype", "access_type"]) {
    if (key(row[field]) && !["official", "closed", "2", "friendly", "open", "1"].includes(key(row[field]))) blockers.push(`INVALID_${field.toUpperCase()}`);
  }
  if (key(row.team_type) && !["national", "club"].includes(key(row.team_type))) blockers.push("INVALID_TEAM_TYPE");
  return { id: row.id, name: row.name, short_title: row.short_title ?? null,
    before, after, access_before: legacy.access_type, access_after: target.access_type,
    changed: !equal(before, after), blockers };
}

function planHash(plan) {
  const { plan_hash, generated_at, ...content } = plan;
  return digest(content);
}

async function inspectPlan(db, { rollback = false } = {}) {
  const columns = await tableColumns(db, "tournaments");
  for (const field of ["id", "name", ...FIELDS.filter((field) => field !== "type")]) {
    if (!columns.some((column) => column.name === field)) throw failure("MISSING_SCHEMA", `Missing tournaments.${field}; use the actual auth-server database`);
  }
  const active = await activeRun(db);
  const issues = [];
  const integrity = await all(db, "PRAGMA integrity_check");
  if (!equal(integrity, [{ integrity_check: "ok" }])) issues.push("DATABASE_INTEGRITY_CHECK_FAILED");
  const catalog = (await tableColumns(db, "categories")).some((column) => column.name === "is_tournament")
    ? await all(db, "SELECT id, name, is_tournament FROM categories") : [];
  for (const category of TOURNAMENT_CATEGORIES) {
    if (!catalog.some((row) => key(row.name) === key(category.name) && Number(row.is_tournament) === 1)) {
      issues.push(`CATALOG_NOT_READY:${category.name}`);
    }
  }
  const tournaments = await all(db, "SELECT * FROM tournaments ORDER BY id");
  const ids = new Set();
  for (const row of tournaments) {
    if (ids.has(key(row.id))) issues.push(`AMBIGUOUS_ID:${row.id}`);
    ids.add(key(row.id));
  }
  let rows;
  if (rollback) {
    rows = [];
    if (active) {
      for (const saved of await all(db, `SELECT * FROM ${ROWS} WHERE migration_id = ? ORDER BY tournament_id`, [active.id])) {
        const row = tournaments.find((item) => item.id === saved.tournament_id);
        const expected = JSON.parse(saved.after_json);
        const target = JSON.parse(saved.before_json);
        rows.push({ id: saved.tournament_id, name: row?.name ?? null,
          before: row ? pick(row) : null, after: target, changed: true,
          blockers: !row ? ["TOURNAMENT_DELETED"] : !equal(pick(row), expected) ? ["CLASSIFICATION_CHANGED_SINCE_MIGRATION"] : [] });
      }
    }
  } else rows = tournaments.map(planRow);
  const changed = rows.filter((row) => row.changed).length;
  if (!rollback && active && changed) issues.push("MIGRATION_ALREADY_APPLIED_WITH_CLASSIFICATION_DRIFT");
  const plan = {
    format_version: 1, version: VERSION, direction: rollback ? "down" : "up", generated_at: new Date().toISOString(),
    database_path: (await all(db, "PRAGMA database_list")).find((item) => item.name === "main")?.file || "",
    type_column_exists: columns.some((column) => column.name === "type"),
    active_run: active, source_snapshot: await snapshot(db), foreign_keys: await foreignKeys(db), issues, rows,
    summary: { tournaments: tournaments.length, changed, unchanged: rows.length - changed,
      blocked: rows.filter((row) => row.blockers.length).length,
      already_applied: !rollback && !!active && changed === 0,
      already_rolled_back: rollback && !active },
  };
  plan.plan_hash = planHash(plan);
  return plan;
}

export async function buildTournamentClassificationPlan(db, options = {}) {
  await run(db, "BEGIN");
  try {
    const plan = await inspectPlan(db, options);
    await run(db, "COMMIT");
    return plan;
  } catch (error) { await run(db, "ROLLBACK").catch(() => {}); throw error; }
}

async function onlineBackup(db, filename) {
  await mkdir(path.dirname(filename), { recursive: true });
  const handle = await open(filename, "wx", 0o600);
  await handle.close();
  try {
    await new Promise((resolve, reject) => {
      const backup = db.backup(filename);
      backup.step(-1, (error) => backup.finish((finishError) => error || finishError ? reject(error || finishError) : resolve()));
    });
  } catch (error) { await unlink(filename).catch(() => {}); throw error; }
}

function assertReady(plan) {
  if (plan.issues.length || plan.summary.blocked) throw failure("MIGRATION_BLOCKED", "Migration has unresolved issues; inspect the dry-run report");
}
function assertSamePlan(expected, actual) {
  if (expected.plan_hash !== actual.plan_hash) throw failure("STALE_PLAN", "Database or migration rules changed after dry-run; generate a new report");
}

// This function owns the connection's transaction. Stop the backend and writers
// before apply. It also rechecks the plan under a write lock after the backup.
export async function applyTournamentClassificationPlan(db, plan, { backupDir } = {}) {
  if (plan?.format_version !== 1 || plan.version !== VERSION || !["up", "down"].includes(plan.direction)
      || plan.plan_hash !== planHash(plan)) throw failure("INVALID_PLAN", "Invalid or edited migration report");
  const active = await activeRun(db);
  if (plan.direction === "up" && active?.plan_hash === plan.plan_hash) return { already_applied: true, migration_id: active.id };
  const options = { rollback: plan.direction === "down" };
  const current = await buildTournamentClassificationPlan(db, options);
  assertSamePlan(plan, current);
  if (current.summary.already_applied || current.summary.already_rolled_back) return { ...current.summary, changed: 0 };
  assertReady(current);
  const backupPath = path.join(backupDir || path.join(path.dirname(plan.database_path), "backups"),
    `${VERSION}-${plan.direction}-${new Date().toISOString().replaceAll(":", "-")}-${randomUUID()}.sqlite`);
  await onlineBackup(db, backupPath);
  const backup = await openClassificationDatabase(backupPath);
  try {
    const copied = await buildTournamentClassificationPlan(backup, options);
    if (!equal(copied.source_snapshot, plan.source_snapshot) || !equal(copied.active_run, plan.active_run)) {
      throw failure("BACKUP_MISMATCH", "Backup does not match the reviewed database snapshot");
    }
  } finally { await close(backup); }

  await run(db, "BEGIN IMMEDIATE TRANSACTION");
  try {
    const locked = await inspectPlan(db, options);
    assertSamePlan(plan, locked);
    assertReady(locked);
    const protectedBefore = (await snapshot(db, { omitClassification: true })).tables;
    if (!locked.type_column_exists) await run(db, "ALTER TABLE tournaments ADD COLUMN type TEXT CHECK (type IN ('Teams', 'Individuals'))");
    await run(db, `CREATE TABLE IF NOT EXISTS ${RUNS} (
      id TEXT PRIMARY KEY, version TEXT NOT NULL, status TEXT NOT NULL,
      plan_hash TEXT NOT NULL, backup_path TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      rolled_back_at TEXT, rollback_backup_path TEXT
    )`);
    await run(db, `CREATE TABLE IF NOT EXISTS ${ROWS} (
      migration_id TEXT NOT NULL, tournament_id TEXT NOT NULL, before_json TEXT NOT NULL, after_json TEXT NOT NULL,
      PRIMARY KEY (migration_id, tournament_id)
    )`);
    const migrationId = options.rollback ? locked.active_run.id : randomUUID();
    if (!options.rollback) await run(db, `INSERT INTO ${RUNS} (id, version, status, plan_hash, backup_path) VALUES (?, ?, 'applied', ?, ?)`,
      [migrationId, VERSION, plan.plan_hash, backupPath]);
    for (const row of locked.rows.filter((row) => row.changed)) {
      await run(db, `UPDATE tournaments SET ${FIELDS.map((field) => `${quote(field)} = ?`).join(", ")} WHERE id = ?`,
        [...FIELDS.map((field) => row.after[field]), row.id]);
      if (!options.rollback) await run(db, `INSERT INTO ${ROWS} (migration_id, tournament_id, before_json, after_json) VALUES (?, ?, ?, ?)`,
        [migrationId, row.id, json(row.before), json(row.after)]);
    }
    if (options.rollback) await run(db, `UPDATE ${RUNS} SET status = 'rolled_back', rolled_back_at = CURRENT_TIMESTAMP, rollback_backup_path = ? WHERE id = ?`, [backupPath, migrationId]);
    for (const row of locked.rows) {
      const [stored] = await all(db, "SELECT * FROM tournaments WHERE id = ?", [row.id]);
      if (!stored || !equal(pick(stored), row.after)) throw failure("CLASSIFICATION_VERIFICATION_FAILED", `Classification verification failed for ${row.id}`);
    }
    if (!equal(protectedBefore, (await snapshot(db, { omitClassification: true })).tables)) {
      throw failure("RELATED_DATA_CHANGED", "Migration changed tournament metadata or related data; all changes were rolled back");
    }
    if (!equal(locked.foreign_keys, await foreignKeys(db))) throw failure("FOREIGN_KEYS_CHANGED", "Foreign-key diagnostics changed during migration");
    if (!equal(await all(db, "PRAGMA integrity_check"), [{ integrity_check: "ok" }])) throw failure("INTEGRITY_CHECK_FAILED", "Integrity check failed after migration");
    await run(db, "COMMIT");
    return { applied: true, direction: plan.direction, migration_id: migrationId, changed: locked.summary.changed, backup_path: backupPath };
  } catch (error) { await run(db, "ROLLBACK").catch(() => {}); throw error; }
}
