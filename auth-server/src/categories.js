import { getTournamentCategory, TOURNAMENT_CATEGORIES } from "./tournament-classification.js";

export const SYSTEM_TOURNAMENT_CATEGORIES = TOURNAMENT_CATEGORIES;

const DEFAULT_CATEGORIES = [
  { name: "Players", sort_order: 10 },
  { name: "GG", sort_order: 20 },
  { name: "Local", sort_order: 30 },
  { name: "Local Website", sort_order: 35 },
  ...SYSTEM_TOURNAMENT_CATEGORIES,
  { name: "World Championship", sort_order: 110 },
  { name: "YouTube", sort_order: 120 },
  { name: "Arena", sort_order: 130 },
];

function categoryKey(value) {
  return String(value || "").trim().replace(/\s+/g, " ").toLowerCase();
}

const systemCategoryKeys = new Set(SYSTEM_TOURNAMENT_CATEGORIES.map(({ name }) => categoryKey(name)));

export function isSystemTournamentCategory(name) {
  // Reserve the retired name too, so old clients cannot recreate KoC.
  const key = categoryKey(name);
  return key === "koc" || systemCategoryKeys.has(key);
}

export function serializeCategory(row) {
  if (!row) return null;
  const tournamentCategory = getTournamentCategory(row.name);
  return {
    ...row,
    is_read_only: isSystemTournamentCategory(row.name),
    tournament_type: tournamentCategory?.type || null,
  };
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
    db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows || []));
  });
}

// Run before accepting HTTP requests, after the referencing schemas are ready.
export async function ensureCategoriesSchema(db) {
  await run(db, "BEGIN IMMEDIATE TRANSACTION");
  try {
    await run(db, `
      CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        sort_order INTEGER NOT NULL DEFAULT 0,
        is_tournament INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      )
    `);
    const columns = new Set((await all(db, "PRAGMA table_info(categories)")).map(({ name }) => name));
    for (const [name, definition] of [
      ["name", "TEXT"],
      ["sort_order", "INTEGER NOT NULL DEFAULT 0"],
      ["is_tournament", "INTEGER NOT NULL DEFAULT 0"],
      // SQLite cannot add a CURRENT_TIMESTAMP default to a populated table.
      ["created_at", "TEXT"],
      ["updated_at", "TEXT"],
    ]) {
      if (!columns.has(name)) await run(db, `ALTER TABLE categories ADD COLUMN ${name} ${definition}`);
    }
    await run(db, "CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_name_unique ON categories(lower(trim(name)))");
    await run(db, "CREATE INDEX IF NOT EXISTS idx_categories_order ON categories(sort_order ASC, name COLLATE NOCASE ASC)");

    const kingCategories = await all(db, `
      SELECT id, name FROM categories
      WHERE lower(trim(name)) IN ('koc', 'king of carcassonne')
    `);
    if (kingCategories.length > 1) {
      throw new Error("Both KoC and King of Carcassonne categories exist; resolve the duplicate before migration to preserve category IDs");
    }
    await run(db, `
      UPDATE categories SET name = 'King of Carcassonne', updated_at = CURRENT_TIMESTAMP
      WHERE lower(trim(name)) = 'koc'
    `);

    const referenceTables = [];
    for (const table of ["icons", "news", "tournaments"]) {
      const referenceColumns = new Set((await all(db, `PRAGMA table_info(${table})`)).map(({ name }) => name));
      if (referenceColumns.size === 0) continue;
      if (!referenceColumns.has("category")) throw new Error(`Missing ${table}.category during category migration`);
      referenceTables.push(table);
      await run(db, `
        UPDATE ${table}
        SET category = 'King of Carcassonne'${referenceColumns.has("updated_at") ? ", updated_at = CURRENT_TIMESTAMP" : ""}
        WHERE lower(trim(COALESCE(category, ''))) = 'koc'
      `);
    }

    for (const { name, sort_order } of DEFAULT_CATEGORIES) {
      await run(db, `
        INSERT OR IGNORE INTO categories (name, sort_order, is_tournament, created_at, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      `, [name, sort_order, isSystemTournamentCategory(name) ? 1 : 0]);
    }
    for (const table of referenceTables) {
      await run(db, `
        INSERT OR IGNORE INTO categories (name, sort_order, created_at, updated_at)
        SELECT DISTINCT trim(category), 1000, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM ${table}
        WHERE trim(COALESCE(category, '')) <> ''
      `);
    }
    for (const { name } of SYSTEM_TOURNAMENT_CATEGORIES) {
      // Keep existing IDs, sort orders and creation dates; do not touch unchanged rows on restart.
      await run(db, `
        UPDATE categories SET name = ?, is_tournament = 1, updated_at = CURRENT_TIMESTAMP
        WHERE lower(trim(name)) = lower(trim(?))
          AND (name <> ? OR COALESCE(is_tournament, 0) <> 1)
      `, [name, name, name]);
    }
    await run(db, `
      UPDATE categories
      SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP), updated_at = COALESCE(updated_at, CURRENT_TIMESTAMP)
      WHERE created_at IS NULL OR updated_at IS NULL
    `);
    await run(db, "COMMIT");
  } catch (error) {
    await run(db, "ROLLBACK").catch(() => {});
    throw error;
  }
}
