// CCL slots are tournament_players rows with entry_stage set; team rosters keep entry_stage NULL.
const TOURNAMENT_PLAYERS_TABLE_SQL = (tableName) => `
  CREATE TABLE ${tableName} (
    id TEXT PRIMARY KEY COLLATE NOCASE,
    tournament_id TEXT NOT NULL COLLATE NOCASE,
    team_id TEXT COLLATE NOCASE,
    player_id TEXT,
    captain BOOLEAN NOT NULL DEFAULT 0 CHECK (captain IN (0, 1)),
    association_id TEXT COLLATE NOCASE,
    entry_stage TEXT CHECK (entry_stage IS NULL OR entry_stage IN ('Main', 'Q1', 'Q2', 'Q3')),
    league_status TEXT CHECK (league_status IS NULL OR league_status IN ('qualified', 'eliminated')),
    created_by TEXT,
    updated_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tournament_id, team_id, player_id),
    CHECK (
      (entry_stage IS NULL AND team_id IS NOT NULL AND player_id IS NOT NULL)
      OR (entry_stage IS NOT NULL AND team_id IS NULL)
    ),
    CHECK (entry_stage IS NULL OR player_id IS NULL OR association_id IS NOT NULL),
    CHECK (league_status IS NULL OR entry_stage IN ('Q1', 'Q2', 'Q3')),
    FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE,
    FOREIGN KEY (team_id) REFERENCES teams(id),
    FOREIGN KEY (player_id) REFERENCES profiles(id),
    FOREIGN KEY (tournament_id, team_id)
      REFERENCES tournament_teams(tournament_id, team_id)
      ON DELETE CASCADE
  )
`;

const CCL_STAGE_ENTRIES_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS ccl_stage_entries (
    tournament_id TEXT NOT NULL COLLATE NOCASE,
    stage TEXT NOT NULL CHECK (stage IN ('Main', 'Q1', 'Q2', 'Q3')),
    slot_id TEXT NOT NULL COLLATE NOCASE,
    qualified_by_name TEXT NOT NULL CHECK (trim(qualified_by_name) <> ''),
    qualified_by_link TEXT,
    player_id TEXT,
    elo REAL,
    seed TEXT CHECK (
      seed IS NULL
      OR (stage = 'Main' AND seed IN ('Pot 1', 'Pot 2', 'Pot 3', 'Pot 4', 'Pot 5'))
      OR (stage <> 'Main' AND seed IN ('Seeded', 'Unseeded'))
    ),
    elo_source TEXT CHECK (elo_source IS NULL OR elo_source IN ('gg_elo', 'manual')),
    fixed_at TEXT,
    fixed_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tournament_id, stage, slot_id),
    -- Elo fields are written together when the admin sets Elo; pots only after that.
    CHECK (
      (fixed_at IS NULL AND player_id IS NULL AND elo IS NULL AND elo_source IS NULL AND seed IS NULL)
      OR (fixed_at IS NOT NULL AND player_id IS NOT NULL AND elo_source IS NOT NULL)
    ),
    CHECK (elo_source IS NOT 'manual' OR elo IS NOT NULL),
    FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE,
    FOREIGN KEY (slot_id) REFERENCES tournament_players(id) ON DELETE CASCADE
  );
  CREATE INDEX IF NOT EXISTS idx_ccl_stage_entries_slot
    ON ccl_stage_entries(slot_id);
`;

const TOURNAMENT_PLAYERS_INDEXES_SQL = `
  CREATE UNIQUE INDEX IF NOT EXISTS idx_tournament_players_tournament_team_player
    ON tournament_players(tournament_id COLLATE NOCASE, team_id COLLATE NOCASE, player_id);
  CREATE INDEX IF NOT EXISTS idx_tournament_players_player_id
    ON tournament_players(player_id);
  CREATE INDEX IF NOT EXISTS idx_tournament_players_ccl_stage
    ON tournament_players(tournament_id COLLATE NOCASE, entry_stage)
    WHERE entry_stage IS NOT NULL;
  -- One CCL slot per player in a tournament.
  CREATE UNIQUE INDEX IF NOT EXISTS idx_tournament_players_ccl_player
    ON tournament_players(tournament_id COLLATE NOCASE, player_id)
    WHERE entry_stage IS NOT NULL AND player_id IS NOT NULL;
`;

const LEGACY_COLUMNS = ["id", "tournament_id", "team_id", "player_id", "captain", "created_at", "updated_at"];

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
  });
}

function dbExec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

function needsTournamentPlayersRebuild(columns) {
  const byName = new Map(columns.map((column) => [column.name, column]));
  return !byName.has("entry_stage")
    || Number(byName.get("team_id")?.notnull) === 1
    || Number(byName.get("player_id")?.notnull) === 1;
}

async function rebuildTournamentPlayers(db, columns) {
  const existing = new Set(columns.map((column) => column.name));
  const copied = LEGACY_COLUMNS.filter((name) => existing.has(name));
  const columnList = copied.join(", ");
  const foreignKeysRow = await dbGet(db, "PRAGMA foreign_keys");
  const foreignKeysWereEnabled = Number(foreignKeysRow?.foreign_keys) === 1;
  // foreign_keys cannot change inside a transaction; turn it off around the table swap.
  if (foreignKeysWereEnabled) await dbExec(db, "PRAGMA foreign_keys = OFF");
  try {
    await dbExec(db, `
      BEGIN IMMEDIATE TRANSACTION;
      DROP TABLE IF EXISTS tournament_players_ccl_migration;
      ${TOURNAMENT_PLAYERS_TABLE_SQL("tournament_players_ccl_migration")};
      INSERT INTO tournament_players_ccl_migration (${columnList})
      SELECT ${columnList} FROM tournament_players;
      DROP TABLE tournament_players;
      ALTER TABLE tournament_players_ccl_migration RENAME TO tournament_players;
      COMMIT;
    `);
  } catch (error) {
    await dbExec(db, "ROLLBACK").catch(() => {});
    throw error;
  } finally {
    if (foreignKeysWereEnabled) await dbExec(db, "PRAGMA foreign_keys = ON");
  }
}

// Repeatable: creates or upgrades tournament_players for CCL slots, then ccl_stage_entries.
export async function ensureCclSchema(db) {
  const columns = await dbAll(db, "PRAGMA table_info(tournament_players)");
  if (!columns.length) {
    await dbExec(db, TOURNAMENT_PLAYERS_TABLE_SQL("IF NOT EXISTS tournament_players"));
  } else if (needsTournamentPlayersRebuild(columns)) {
    await rebuildTournamentPlayers(db, columns);
  }
  await dbExec(db, TOURNAMENT_PLAYERS_INDEXES_SQL);
  await dbExec(db, CCL_STAGE_ENTRIES_TABLE_SQL);
}
