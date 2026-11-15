// Tournament structure: a tree of stages (a stage may hold child stages, e.g. Qualifiers → Q1),
// groups inside a stage, and the bracket columns of duels that belong to a group.
// For CCL the tree is generated from a season format (ccl-formats.js).

export const STAGE_FORMATS = Object.freeze([
  "double_elimination",
  "single_elimination",
  "round_robin",
  "swiss",
  "league",
]);
export const STAGE_STATUSES = Object.freeze(["structure", "drawn", "in_progress", "done"]);
export const BRACKET_SIDES = Object.freeze(["upper", "lower", "final"]);

const sqlList = (values) => values.map((value) => `'${value}'`).join(", ");

const TOURNAMENT_STAGES_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS tournament_stages (
    id TEXT PRIMARY KEY COLLATE NOCASE,
    tournament_id TEXT NOT NULL COLLATE NOCASE,
    parent_id TEXT COLLATE NOCASE,
    stage_key TEXT NOT NULL COLLATE NOCASE CHECK (trim(stage_key) <> ''),
    name TEXT NOT NULL CHECK (trim(name) <> ''),
    sort_order INTEGER NOT NULL DEFAULT 0,
    format TEXT CHECK (format IS NULL OR format IN (${sqlList(STAGE_FORMATS)})),
    settings_json TEXT,
    -- Monday of the first week; match deadlines are counted from it.
    start_date TEXT,
    status TEXT NOT NULL DEFAULT 'structure' CHECK (status IN (${sqlList(STAGE_STATUSES)})),
    created_by TEXT,
    updated_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tournament_id, stage_key),
    CHECK (parent_id IS NULL OR parent_id <> id),
    FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE,
    FOREIGN KEY (parent_id) REFERENCES tournament_stages(id) ON DELETE CASCADE
  );
  CREATE INDEX IF NOT EXISTS idx_tournament_stages_parent
    ON tournament_stages(parent_id);
`;

const TOURNAMENT_GROUPS_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS tournament_groups (
    id TEXT PRIMARY KEY COLLATE NOCASE,
    stage_id TEXT NOT NULL COLLATE NOCASE,
    group_key TEXT NOT NULL COLLATE NOCASE CHECK (trim(group_key) <> ''),
    name TEXT,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (stage_id, group_key),
    FOREIGN KEY (stage_id) REFERENCES tournament_stages(id) ON DELETE CASCADE
  );
`;

// Bracket columns of duels. Added with ALTER TABLE, so group_id carries no foreign key:
// structure generation deletes and recreates a group's duels itself.
export const DUEL_BRACKET_COLUMNS = Object.freeze([
  ["group_id", "TEXT"],
  // Match number inside the group, 1-15 for an 8-player Double Elimination group.
  ["bracket_no", "INTEGER CHECK (bracket_no IS NULL OR bracket_no > 0)"],
  ["bracket_side", `TEXT CHECK (bracket_side IS NULL OR bracket_side IN (${sqlList(BRACKET_SIDES)}))`],
  // Where a player comes from: draw:<position>, winner:<bracket_no>, loser:<bracket_no>.
  ["p1_source", "TEXT"],
  ["p2_source", "TEXT"],
  ["bracket_label", "TEXT"],
  // 1 = played only if needed (Group Final Reset).
  ["is_conditional", "BOOLEAN NOT NULL DEFAULT 0 CHECK (is_conditional IN (0, 1))"],
]);

// One bracket slot per group; soft-deleted duels free their slot.
export const DUEL_BRACKET_INDEX_SQL = `
  CREATE UNIQUE INDEX IF NOT EXISTS idx_duels_group_bracket_no
    ON duels(group_id COLLATE NOCASE, bracket_no)
    WHERE group_id IS NOT NULL AND deleted_at IS NULL;
`;

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function dbExec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

// Repeatable: creates tournament_stages and tournament_groups.
export async function ensureTournamentStructureSchema(db) {
  await dbExec(db, TOURNAMENT_STAGES_TABLE_SQL);
  await dbExec(db, TOURNAMENT_GROUPS_TABLE_SQL);
}

// Repeatable. The server calls it from its duels startup step, after the other duels columns,
// so two startup steps never add the same column at once. Does nothing without a duels table.
export async function ensureDuelBracketColumns(db) {
  const columns = await dbAll(db, "PRAGMA table_info(duels)");
  if (!columns.length) return;
  const existing = new Set(columns.map((column) => column.name));
  for (const [name, definition] of DUEL_BRACKET_COLUMNS) {
    if (!existing.has(name)) await dbExec(db, `ALTER TABLE duels ADD COLUMN ${name} ${definition}`);
  }
  await dbExec(db, DUEL_BRACKET_INDEX_SQL);
}
