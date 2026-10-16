import { createHash, randomUUID } from "node:crypto";
import path from "node:path";
import { getTournamentCategory, readTournamentClassification } from "./tournament-classification.js";
import { createClassificationBackup, openClassificationDatabase } from "./tournament-classification-migration.js";

const VERSION = "tournament-classification-cleanup-v1";
export const LEGACY_TOURNAMENT_COLUMNS = Object.freeze(["tournament_type", "team_type", "subtype", "access_type"]);
const quote = (name) => `"${name.replaceAll('"', '""')}"`;
const all = (db, sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows)));
const run = (db, sql) => new Promise((resolve, reject) => db.run(sql, (error) => error ? reject(error) : resolve()));
const close = (db) => new Promise((resolve, reject) => db.close((error) => error ? reject(error) : resolve()));
const fail = (code, message) => Object.assign(new Error(message), { code });
function canonical(value) {
  if (Buffer.isBuffer(value)) return { blob: value.toString("hex") };
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonical(value[key])]));
  return value;
}
const json = (value) => JSON.stringify(canonical(value));
const hash = (value) => createHash("sha256").update(json(value)).digest("hex");
const equal = (a, b) => json(a) === json(b);
const columns = (db) => all(db, "PRAGMA table_info(tournaments)");
const schema = (db) => all(db, "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name");

// Includes the original migration journal, metadata, replays and every related table.
// Stream large tables to keep memory usage bounded on the production server.
async function snapshot(db, { omitLegacy = false } = {}) {
  const objects = await schema(db);
  const tables = {};
  for (const table of objects.filter((item) => item.type === "table")) {
    const info = await all(db, `PRAGMA table_info(${quote(table.name)})`);
    const primary = info.filter((column) => column.pk).sort((a, b) => a.pk - b.pk);
    const order = (primary.length ? primary : info).map((column) => quote(column.name)).join(", ");
    const digest = createHash("sha256");
    let count = 0;
    await new Promise((resolve, reject) => db.each(`SELECT * FROM ${quote(table.name)} ORDER BY ${order}`, (error, row) => {
      if (error) return reject(error);
      const data = omitLegacy && table.name === "tournaments"
        ? Object.fromEntries(Object.entries(row).filter(([field]) => !LEGACY_TOURNAMENT_COLUMNS.includes(field))) : row;
      digest.update(json(data)); digest.update("\n"); count += 1;
    }, (error) => error ? reject(error) : resolve()));
    tables[table.name] = { count, sha256: digest.digest("hex") };
  }
  return { schema_sha256: hash(objects), tables };
}

async function foreignKeys(db) {
  try { return { violations: await all(db, "PRAGMA foreign_key_check") }; }
  catch (error) { return { diagnostic_error: error.message }; }
}
function planHash({ generated_at, plan_hash, ...content }) { return hash(content); }
function assertReady(plan) {
  if (plan.issues.length || plan.summary.blocked) throw fail("CLEANUP_BLOCKED", "Cleanup has unresolved issues; inspect the report");
}

async function inspect(db) {
  const info = await columns(db);
  for (const field of ["id", "type", "category"]) {
    if (!info.some((column) => column.name === field)) throw fail("CLASSIFICATION_NOT_MIGRATED", `Missing tournaments.${field}; complete the type/category migration first`);
  }
  const removed = LEGACY_TOURNAMENT_COLUMNS.filter((field) => info.some((column) => column.name === field));
  const issues = [];
  const [engine] = await all(db, "SELECT sqlite_version() AS version");
  const [major, minor, patch] = engine.version.split(".").map(Number);
  if (major < 3 || (major === 3 && (minor < 35 || (minor === 35 && patch < 5)))) issues.push("SQLITE_3_35_5_OR_NEWER_REQUIRED");
  if (!equal(await all(db, "PRAGMA integrity_check"), [{ integrity_check: "ok" }])) issues.push("DATABASE_INTEGRITY_CHECK_FAILED");
  // Custom dependent objects need an explicit decision; never delete them implicitly.
  for (const object of await schema(db)) {
    if (object.type === "table") {
      // Other tables may legitimately have their own access_type/team_type.
      // Only a foreign key targeting a removed tournament column is a dependency.
      for (const reference of await all(db, `PRAGMA foreign_key_list(${quote(object.name)})`)) {
        if (reference.table.toLowerCase() === "tournaments" && removed.includes(reference.to?.toLowerCase())) {
          issues.push(`DEPENDENT_SCHEMA_OBJECT:table:${object.name}`);
        }
      }
      continue;
    }
    if (object.tbl_name !== "tournaments" && !/\btournaments\b/i.test(object.sql || "")) continue;
    if (removed.some((field) => new RegExp(`\\b${field}\\b`, "i").test(object.sql || ""))) issues.push(`DEPENDENT_SCHEMA_OBJECT:${object.type}:${object.name}`);
  }
  const ids = new Set();
  const rows = (await all(db, "SELECT * FROM tournaments ORDER BY id")).map((row) => {
    const category = getTournamentCategory(row.category);
    const blockers = [];
    if (!["Teams", "Individuals"].includes(row.type) || !category || category.name !== row.category || category.type !== row.type) blockers.push("INVALID_CANONICAL_PAIR");
    const id = String(row.id || "").trim().toLowerCase();
    if (!id || ids.has(id)) issues.push(`INVALID_OR_AMBIGUOUS_ID:${row.id}`);
    ids.add(id);
    return { id: row.id, type: row.type, category: row.category,
      access: readTournamentClassification(row).access_type,
      removed: Object.fromEntries(removed.map((field) => [field, row[field]])), blockers };
  });
  const plan = {
    version: VERSION, generated_at: new Date().toISOString(),
    database_path: (await all(db, "PRAGMA database_list")).find((item) => item.name === "main")?.file || "",
    removed_columns: removed, issues, rows, source_snapshot: await snapshot(db), foreign_keys: await foreignKeys(db),
    summary: { tournaments: rows.length, columns_to_remove: removed.length, blocked: rows.filter((row) => row.blockers.length).length, already_applied: removed.length === 0 },
  };
  plan.plan_hash = planHash(plan);
  return plan;
}

export async function buildTournamentCleanupPlan(db) {
  await run(db, "BEGIN");
  try {
    const plan = await inspect(db);
    await run(db, "COMMIT");
    return plan;
  } catch (error) { await run(db, "ROLLBACK").catch(() => {}); throw error; }
}

export async function applyTournamentCleanupPlan(db, plan, { backupDir } = {}) {
  if (plan?.version !== VERSION || plan.plan_hash !== planHash(plan)) throw fail("INVALID_PLAN", "Invalid or edited cleanup report");
  const current = await buildTournamentCleanupPlan(db);
  assertReady(current);
  if (current.database_path !== plan.database_path) throw fail("STALE_PLAN", "Report belongs to a different database");
  if (current.summary.already_applied) return { already_applied: true, removed_columns: [] };
  if (plan.plan_hash !== current.plan_hash) throw fail("STALE_PLAN", "Database changed after dry-run; generate a new report");
  const backupPath = path.join(backupDir || path.join(path.dirname(plan.database_path), "backups"),
    `${VERSION}-${new Date().toISOString().replaceAll(":", "-")}-${randomUUID()}.sqlite`);
  await createClassificationBackup(db, backupPath);
  const backup = await openClassificationDatabase(backupPath);
  try {
    if (!equal(await snapshot(backup), plan.source_snapshot)
        || !equal(await all(backup, "PRAGMA integrity_check"), [{ integrity_check: "ok" }])) throw fail("BACKUP_MISMATCH", "Backup does not match the reviewed database");
  } finally { await close(backup); }

  await run(db, "BEGIN IMMEDIATE");
  try {
    const locked = await inspect(db);
    if (locked.plan_hash !== plan.plan_hash) throw fail("STALE_PLAN", "Database changed during backup; generate a new report");
    assertReady(locked);
    const protectedBefore = (await snapshot(db, { omitLegacy: true })).tables;
    const columnsBefore = (await columns(db)).filter((column) => !plan.removed_columns.includes(column.name)).map(({ cid, ...column }) => column);
    const schemaBefore = (await schema(db)).filter((item) => !(item.type === "table" && item.name === "tournaments"));
    for (const field of plan.removed_columns) await run(db, `ALTER TABLE tournaments DROP COLUMN ${quote(field)}`);
    if (!equal(protectedBefore, (await snapshot(db)).tables)) throw fail("RELATED_DATA_CHANGED", "Cleanup changed retained data; all changes were rolled back");
    const columnsAfter = (await columns(db)).map(({ cid, ...column }) => column);
    const schemaAfter = (await schema(db)).filter((item) => !(item.type === "table" && item.name === "tournaments"));
    if (!equal(columnsBefore, columnsAfter) || !equal(schemaBefore, schemaAfter)) throw fail("SCHEMA_CHANGED", "Cleanup changed unrelated schema; all changes were rolled back");
    if (!equal(locked.foreign_keys, await foreignKeys(db))) throw fail("FOREIGN_KEYS_CHANGED", "Foreign-key diagnostics changed; all changes were rolled back");
    if (!equal(await all(db, "PRAGMA integrity_check"), [{ integrity_check: "ok" }])) throw fail("INTEGRITY_CHECK_FAILED", "Integrity check failed; all changes were rolled back");
    await run(db, "COMMIT");
    return { applied: true, removed_columns: plan.removed_columns, tournaments: plan.summary.tournaments, backup_path: backupPath };
  } catch (error) { await run(db, "ROLLBACK").catch(() => {}); throw error; }
}
