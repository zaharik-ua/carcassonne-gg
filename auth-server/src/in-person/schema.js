export const TOURNAMENT_ENTITY_TYPES = Object.freeze({
  TOURNAMENT: "tournament",
  IN_PERSON_TOURNAMENT: "in_person_tournament",
});

const ACCESS_TABLE = "tournament_access_users";
const ACCESS_MIGRATION_TABLE = "tournament_access_users_in_person_migration";

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => {
      if (error) reject(error);
      else resolve(rows || []);
    });
  });
}

function dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => {
      if (error) reject(error);
      else resolve(row || null);
    });
  });
}

function dbExec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

function accessTableSql(tableName) {
  return `
    CREATE TABLE ${tableName} (
      tournament_entity_type TEXT NOT NULL DEFAULT 'tournament'
        CHECK (tournament_entity_type IN ('tournament', 'in_person_tournament')),
      tournament_id TEXT NOT NULL,
      user_id INTEGER NOT NULL,
      role TEXT NOT NULL DEFAULT 'captain',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (tournament_entity_type, tournament_id, user_id)
    )
  `;
}

function hasExpectedAccessPrimaryKey(columns) {
  const primaryKeyColumns = (columns || [])
    .filter((column) => Number(column?.pk) > 0)
    .sort((left, right) => Number(left.pk) - Number(right.pk))
    .map((column) => String(column?.name || ""));
  return primaryKeyColumns.join("|") === "tournament_entity_type|tournament_id|user_id";
}

async function rollbackQuietly(db) {
  try {
    await dbExec(db, "ROLLBACK");
  } catch {
    // The failing batch may have rolled back before this fallback runs.
  }
}

async function migrateTournamentAccessUsers(db) {
  const columns = await dbAll(db, `PRAGMA table_info(${ACCESS_TABLE})`);
  if (!columns.length) {
    await dbExec(db, `${accessTableSql(ACCESS_TABLE)};`);
    return { migrated: false, created: true, rowsBefore: 0, rowsAfter: 0 };
  }

  const columnNames = new Set(columns.map((column) => String(column?.name || "")));
  if (!columnNames.has("tournament_id") || !columnNames.has("user_id")) {
    throw new Error("Cannot migrate tournament_access_users without tournament_id and user_id");
  }

  const hasAllColumns = [
    "tournament_entity_type",
    "tournament_id",
    "user_id",
    "role",
    "created_at",
    "updated_at",
  ].every((columnName) => columnNames.has(columnName));
  if (hasAllColumns && hasExpectedAccessPrimaryKey(columns)) {
    const row = await dbGet(db, `SELECT COUNT(*) AS count FROM ${ACCESS_TABLE}`);
    const count = Number(row?.count || 0);
    return { migrated: false, created: false, rowsBefore: count, rowsAfter: count };
  }

  const countBeforeRow = await dbGet(db, `SELECT COUNT(*) AS count FROM ${ACCESS_TABLE}`);
  const rowsBefore = Number(countBeforeRow?.count || 0);
  const entityTypeExpression = columnNames.has("tournament_entity_type")
    ? `CASE
        WHEN lower(trim(COALESCE(tournament_entity_type, ''))) = 'in_person_tournament'
          THEN 'in_person_tournament'
        ELSE 'tournament'
      END`
    : `'tournament'`;
  const roleExpression = columnNames.has("role")
    ? "COALESCE(NULLIF(lower(trim(role)), ''), 'captain')"
    : "'captain'";
  const createdAtExpression = columnNames.has("created_at")
    ? "COALESCE(created_at, CURRENT_TIMESTAMP)"
    : "CURRENT_TIMESTAMP";
  const updatedAtExpression = columnNames.has("updated_at")
    ? "COALESCE(updated_at, CURRENT_TIMESTAMP)"
    : "CURRENT_TIMESTAMP";

  try {
    await dbExec(db, `
      BEGIN IMMEDIATE TRANSACTION;
      DROP TABLE IF EXISTS ${ACCESS_MIGRATION_TABLE};
      ${accessTableSql(ACCESS_MIGRATION_TABLE)};
      INSERT INTO ${ACCESS_MIGRATION_TABLE} (
        tournament_entity_type,
        tournament_id,
        user_id,
        role,
        created_at,
        updated_at
      )
      SELECT
        ${entityTypeExpression},
        tournament_id,
        user_id,
        ${roleExpression},
        ${createdAtExpression},
        ${updatedAtExpression}
      FROM ${ACCESS_TABLE};
      DROP TABLE ${ACCESS_TABLE};
      ALTER TABLE ${ACCESS_MIGRATION_TABLE} RENAME TO ${ACCESS_TABLE};
      COMMIT;
    `);
  } catch (error) {
    await rollbackQuietly(db);
    throw error;
  }

  const countAfterRow = await dbGet(db, `SELECT COUNT(*) AS count FROM ${ACCESS_TABLE}`);
  const rowsAfter = Number(countAfterRow?.count || 0);
  if (rowsAfter !== rowsBefore) {
    throw new Error(
      `tournament_access_users row count mismatch after migration: ${rowsBefore} -> ${rowsAfter}`
    );
  }
  return { migrated: true, created: false, rowsBefore, rowsAfter };
}

async function ensureInPersonTables(db) {
  try {
    await dbExec(db, `
      BEGIN IMMEDIATE TRANSACTION;

      CREATE TABLE IF NOT EXISTS in_person_schema_migrations (
        version INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS cities (
        id TEXT PRIMARY KEY,
        association_id TEXT NOT NULL COLLATE NOCASE,
        name_en TEXT NOT NULL CHECK (length(trim(name_en)) > 0),
        name_local TEXT,
        icon_url TEXT,
        archived_at TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (association_id) REFERENCES associations(code)
          ON UPDATE CASCADE ON DELETE RESTRICT
      );

      CREATE UNIQUE INDEX IF NOT EXISTS idx_cities_active_association_name_en
        ON cities(association_id COLLATE NOCASE, lower(trim(name_en)))
        WHERE archived_at IS NULL;
      CREATE INDEX IF NOT EXISTS idx_cities_association_archived
        ON cities(association_id COLLATE NOCASE, archived_at);

      CREATE TABLE IF NOT EXISTS in_person_tournaments (
        id TEXT PRIMARY KEY,
        slug TEXT NOT NULL COLLATE NOCASE,
        name_en TEXT NOT NULL CHECK (length(trim(name_en)) > 0),
        name_local TEXT,
        scope TEXT NOT NULL CHECK (scope IN ('international', 'local')),
        association_id TEXT COLLATE NOCASE,
        local_subtype TEXT CHECK (local_subtype IN ('final', 'qualifier')),
        qualifier_city_id TEXT,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        organizer_name TEXT NOT NULL CHECK (length(trim(organizer_name)) > 0),
        organizer_url TEXT,
        rules_url TEXT,
        swiss_rounds_count INTEGER NOT NULL CHECK (swiss_rounds_count > 0),
        playoff_first_round TEXT NOT NULL
          CHECK (playoff_first_round IN ('round_of_32', 'round_of_16', 'quarter_final', 'semi_final')),
        draw_mode TEXT NOT NULL DEFAULT 'manual_draw_numbers'
          CHECK (draw_mode = 'manual_draw_numbers'),
        swiss_tiebreak_profile TEXT NOT NULL DEFAULT 'swiss_standard_v1'
          CHECK (swiss_tiebreak_profile = 'swiss_standard_v1'),
        status TEXT NOT NULL DEFAULT 'draft'
          CHECK (status IN ('draft', 'registration', 'check_in', 'swiss', 'playoff', 'completed', 'cancelled')),
        revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
        published_at TEXT,
        completed_at TEXT,
        cancelled_at TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CHECK (start_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
        CHECK (end_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
        CHECK (end_date >= start_date),
        CHECK (
          (scope = 'international'
            AND association_id IS NULL
            AND local_subtype IS NULL
            AND qualifier_city_id IS NULL)
          OR
          (scope = 'local'
            AND association_id IS NOT NULL
            AND local_subtype = 'final'
            AND qualifier_city_id IS NULL)
          OR
          (scope = 'local'
            AND association_id IS NOT NULL
            AND local_subtype = 'qualifier'
            AND qualifier_city_id IS NOT NULL)
        ),
        FOREIGN KEY (association_id) REFERENCES associations(code)
          ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (qualifier_city_id) REFERENCES cities(id)
          ON UPDATE CASCADE ON DELETE RESTRICT
      );

      CREATE UNIQUE INDEX IF NOT EXISTS idx_in_person_tournaments_slug
        ON in_person_tournaments(slug COLLATE NOCASE);
      CREATE INDEX IF NOT EXISTS idx_in_person_tournaments_public
        ON in_person_tournaments(status, start_date, end_date);

      CREATE TABLE IF NOT EXISTS in_person_participants (
        id TEXT PRIMARY KEY,
        tournament_id TEXT NOT NULL,
        name_en TEXT NOT NULL CHECK (length(trim(name_en)) > 0),
        name_local TEXT,
        bga_nickname TEXT,
        association_id TEXT COLLATE NOCASE,
        city_id TEXT,
        status TEXT NOT NULL DEFAULT 'registered'
          CHECK (status IN ('registered', 'checked_in', 'withdrawn', 'disqualified')),
        draw_number INTEGER CHECK (draw_number IS NULL OR draw_number > 0),
        checked_in_at TEXT,
        withdrawn_at TEXT,
        disqualified_at TEXT,
        status_reason TEXT,
        is_late_entry INTEGER NOT NULL DEFAULT 0 CHECK (is_late_entry IN (0, 1)),
        late_entry_mode TEXT CHECK (late_entry_mode IN ('late_bye', 'pair_with_bye')),
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CHECK (
          (association_id IS NOT NULL AND city_id IS NULL)
          OR (association_id IS NULL AND city_id IS NOT NULL)
        ),
        CHECK (status = 'checked_in' OR draw_number IS NULL),
        CHECK (is_late_entry = 1 OR late_entry_mode IS NULL),
        FOREIGN KEY (tournament_id) REFERENCES in_person_tournaments(id)
          ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY (association_id) REFERENCES associations(code)
          ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (city_id) REFERENCES cities(id)
          ON UPDATE CASCADE ON DELETE RESTRICT
      );

      CREATE UNIQUE INDEX IF NOT EXISTS idx_in_person_participants_draw_number
        ON in_person_participants(tournament_id, draw_number)
        WHERE draw_number IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_in_person_participants_tournament_status
        ON in_person_participants(tournament_id, status);

      CREATE TABLE IF NOT EXISTS in_person_rounds (
        id TEXT PRIMARY KEY,
        tournament_id TEXT NOT NULL,
        stage TEXT NOT NULL CHECK (stage IN ('swiss', 'playoff')),
        round_number INTEGER CHECK (round_number IS NULL OR round_number > 0),
        round_key TEXT,
        round_order INTEGER CHECK (round_order IS NULL OR round_order > 0),
        status TEXT NOT NULL DEFAULT 'draft'
          CHECK (status IN ('draft', 'published', 'completed', 'cancelled')),
        revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
        published_at TEXT,
        completed_at TEXT,
        cancelled_at TEXT,
        cancelled_by_user_id INTEGER,
        cancellation_reason TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CHECK (
          (stage = 'swiss'
            AND round_number IS NOT NULL
            AND round_key IS NULL
            AND round_order IS NULL)
          OR
          (stage = 'playoff'
            AND round_number IS NULL
            AND round_key IS NOT NULL
            AND length(trim(round_key)) > 0
            AND round_order IS NOT NULL)
        ),
        CHECK (status = 'cancelled' OR cancelled_at IS NULL),
        FOREIGN KEY (tournament_id) REFERENCES in_person_tournaments(id)
          ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY (cancelled_by_user_id) REFERENCES users(id)
          ON UPDATE CASCADE ON DELETE SET NULL
      );

      CREATE UNIQUE INDEX IF NOT EXISTS idx_in_person_rounds_active_swiss_number
        ON in_person_rounds(tournament_id, round_number)
        WHERE stage = 'swiss' AND status <> 'cancelled';
      CREATE UNIQUE INDEX IF NOT EXISTS idx_in_person_rounds_active_playoff_key
        ON in_person_rounds(tournament_id, round_key)
        WHERE stage = 'playoff' AND status <> 'cancelled';
      CREATE INDEX IF NOT EXISTS idx_in_person_rounds_tournament_stage_status
        ON in_person_rounds(tournament_id, stage, status);

      CREATE TABLE IF NOT EXISTS in_person_matches (
        id TEXT PRIMARY KEY,
        round_id TEXT NOT NULL,
        bracket_position INTEGER CHECK (bracket_position IS NULL OR bracket_position > 0),
        table_number INTEGER CHECK (table_number IS NULL OR table_number > 0),
        participant_a_id TEXT,
        participant_b_id TEXT,
        starting_participant_id TEXT,
        status TEXT NOT NULL DEFAULT 'scheduled'
          CHECK (status IN ('scheduled', 'completed', 'cancelled')),
        is_bye INTEGER NOT NULL DEFAULT 0 CHECK (is_bye IN (0, 1)),
        result_type TEXT
          CHECK (result_type IN ('points', 'simple', 'time_forfeit', 'technical', 'bye')),
        points_a INTEGER CHECK (points_a IS NULL OR points_a >= 0),
        points_b INTEGER CHECK (points_b IS NULL OR points_b >= 0),
        winner_participant_id TEXT,
        loser_participant_id TEXT,
        finish_reason TEXT
          CHECK (finish_reason IN ('time_forfeit', 'withdrawal', 'disqualification', 'no_show', 'admin_decision')),
        admin_note TEXT,
        revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
        next_match_for_winner_id TEXT,
        next_match_for_winner_slot TEXT
          CHECK (next_match_for_winner_slot IN ('participant_a', 'participant_b')),
        next_match_for_loser_id TEXT,
        next_match_for_loser_slot TEXT
          CHECK (next_match_for_loser_slot IN ('participant_a', 'participant_b')),
        cancelled_at TEXT,
        cancelled_by_user_id INTEGER,
        cancellation_reason TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CHECK (participant_a_id IS NULL OR participant_b_id IS NULL OR participant_a_id <> participant_b_id),
        CHECK (
          starting_participant_id IS NULL
          OR starting_participant_id = participant_a_id
          OR starting_participant_id = participant_b_id
        ),
        CHECK (
          winner_participant_id IS NULL
          OR winner_participant_id = participant_a_id
          OR winner_participant_id = participant_b_id
        ),
        CHECK (
          loser_participant_id IS NULL
          OR loser_participant_id = participant_a_id
          OR loser_participant_id = participant_b_id
        ),
        CHECK (
          winner_participant_id IS NULL
          OR loser_participant_id IS NULL
          OR winner_participant_id <> loser_participant_id
        ),
        CHECK (
          is_bye = 0
          OR (
            participant_a_id IS NOT NULL
            AND participant_b_id IS NULL
            AND starting_participant_id IS NULL
            AND status = 'completed'
            AND result_type = 'bye'
            AND winner_participant_id = participant_a_id
            AND loser_participant_id IS NULL
          )
        ),
        CHECK (status = 'cancelled' OR cancelled_at IS NULL),
        FOREIGN KEY (round_id) REFERENCES in_person_rounds(id)
          ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY (participant_a_id) REFERENCES in_person_participants(id)
          ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (participant_b_id) REFERENCES in_person_participants(id)
          ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (starting_participant_id) REFERENCES in_person_participants(id)
          ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (winner_participant_id) REFERENCES in_person_participants(id)
          ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (loser_participant_id) REFERENCES in_person_participants(id)
          ON UPDATE CASCADE ON DELETE RESTRICT,
        FOREIGN KEY (next_match_for_winner_id) REFERENCES in_person_matches(id)
          ON UPDATE CASCADE ON DELETE SET NULL,
        FOREIGN KEY (next_match_for_loser_id) REFERENCES in_person_matches(id)
          ON UPDATE CASCADE ON DELETE SET NULL,
        FOREIGN KEY (cancelled_by_user_id) REFERENCES users(id)
          ON UPDATE CASCADE ON DELETE SET NULL
      );

      CREATE UNIQUE INDEX IF NOT EXISTS idx_in_person_matches_active_table
        ON in_person_matches(round_id, table_number)
        WHERE status <> 'cancelled' AND table_number IS NOT NULL;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_in_person_matches_active_bracket_position
        ON in_person_matches(round_id, bracket_position)
        WHERE status <> 'cancelled' AND bracket_position IS NOT NULL;
      CREATE INDEX IF NOT EXISTS idx_in_person_matches_round_status
        ON in_person_matches(round_id, status);

      CREATE TABLE IF NOT EXISTS in_person_standings (
        tournament_id TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK (revision > 0),
        source_completed_round_id TEXT,
        participant_id TEXT NOT NULL,
        position INTEGER NOT NULL CHECK (position > 0),
        wins INTEGER NOT NULL DEFAULT 0 CHECK (wins >= 0),
        buchholz INTEGER NOT NULL DEFAULT 0,
        solkoff1 INTEGER NOT NULL DEFAULT 0,
        solkoff2 INTEGER NOT NULL DEFAULT 0,
        vp_difference INTEGER NOT NULL DEFAULT 0,
        sonneborn_berger INTEGER NOT NULL DEFAULT 0,
        bye_count INTEGER NOT NULL DEFAULT 0 CHECK (bye_count >= 0),
        calculated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (tournament_id, revision, participant_id),
        UNIQUE (tournament_id, revision, position),
        FOREIGN KEY (tournament_id) REFERENCES in_person_tournaments(id)
          ON UPDATE CASCADE ON DELETE CASCADE,
        FOREIGN KEY (source_completed_round_id) REFERENCES in_person_rounds(id)
          ON UPDATE CASCADE ON DELETE SET NULL,
        FOREIGN KEY (participant_id) REFERENCES in_person_participants(id)
          ON UPDATE CASCADE ON DELETE CASCADE
      );

      CREATE INDEX IF NOT EXISTS idx_in_person_standings_latest
        ON in_person_standings(tournament_id, revision DESC, position);

      INSERT OR IGNORE INTO in_person_schema_migrations (version, name)
      VALUES (1, 'schema_foundation');

      COMMIT;
    `);
  } catch (error) {
    await rollbackQuietly(db);
    throw error;
  }
}

async function ensureCityExtensions(db) {
  try {
    await dbExec(db, "BEGIN IMMEDIATE TRANSACTION");
    const columns = await dbAll(db, "PRAGMA table_info(cities)");
    if (!columns.some((column) => String(column?.name || "") === "icon_url")) {
      await dbExec(db, "ALTER TABLE cities ADD COLUMN icon_url TEXT");
    }
    await dbExec(db, `
      INSERT OR IGNORE INTO in_person_schema_migrations (version, name)
      VALUES (2, 'city_icon_url');
      COMMIT;
    `);
  } catch (error) {
    await rollbackQuietly(db);
    throw error;
  }
}

async function ensureInPersonTriggersAndAccessIndexes(db) {
  try {
    await dbExec(db, `
      BEGIN IMMEDIATE TRANSACTION;

      CREATE INDEX IF NOT EXISTS idx_tournament_access_users_user_type
      ON tournament_access_users(user_id, tournament_entity_type);
    CREATE INDEX IF NOT EXISTS idx_tournament_access_users_tournament_type
      ON tournament_access_users(tournament_id, tournament_entity_type);

    DROP TRIGGER IF EXISTS trg_tournament_access_users_validate_insert;
    CREATE TRIGGER trg_tournament_access_users_validate_insert
    BEFORE INSERT ON tournament_access_users
    BEGIN
      SELECT CASE
        WHEN NOT EXISTS (SELECT 1 FROM users WHERE id = NEW.user_id)
          THEN RAISE(ABORT, 'Unknown tournament access user')
      END;
      SELECT CASE
        WHEN NEW.tournament_entity_type = 'tournament'
          AND NOT EXISTS (
            SELECT 1 FROM tournaments
            WHERE upper(trim(id)) = upper(trim(NEW.tournament_id))
          )
          THEN RAISE(ABORT, 'Unknown tournament')
      END;
      SELECT CASE
        WHEN NEW.tournament_entity_type = 'in_person_tournament'
          AND NOT EXISTS (
            SELECT 1 FROM in_person_tournaments
            WHERE upper(trim(id)) = upper(trim(NEW.tournament_id))
          )
          THEN RAISE(ABORT, 'Unknown in-person tournament')
      END;
      SELECT CASE
        WHEN NEW.tournament_entity_type = 'in_person_tournament'
          AND lower(trim(NEW.role)) <> 'admin'
          THEN RAISE(ABORT, 'In-person tournament access role must be admin')
      END;
      SELECT CASE
        WHEN NEW.tournament_entity_type = 'tournament'
          AND lower(trim(NEW.role)) NOT IN ('admin', 'captain')
          THEN RAISE(ABORT, 'Invalid tournament access role')
      END;
    END;

    DROP TRIGGER IF EXISTS trg_tournament_access_users_validate_update;
    CREATE TRIGGER trg_tournament_access_users_validate_update
    BEFORE UPDATE OF tournament_entity_type, tournament_id, user_id, role
    ON tournament_access_users
    BEGIN
      SELECT CASE
        WHEN NOT EXISTS (SELECT 1 FROM users WHERE id = NEW.user_id)
          THEN RAISE(ABORT, 'Unknown tournament access user')
      END;
      SELECT CASE
        WHEN NEW.tournament_entity_type = 'tournament'
          AND NOT EXISTS (
            SELECT 1 FROM tournaments
            WHERE upper(trim(id)) = upper(trim(NEW.tournament_id))
          )
          THEN RAISE(ABORT, 'Unknown tournament')
      END;
      SELECT CASE
        WHEN NEW.tournament_entity_type = 'in_person_tournament'
          AND NOT EXISTS (
            SELECT 1 FROM in_person_tournaments
            WHERE upper(trim(id)) = upper(trim(NEW.tournament_id))
          )
          THEN RAISE(ABORT, 'Unknown in-person tournament')
      END;
      SELECT CASE
        WHEN NEW.tournament_entity_type = 'in_person_tournament'
          AND lower(trim(NEW.role)) <> 'admin'
          THEN RAISE(ABORT, 'In-person tournament access role must be admin')
      END;
      SELECT CASE
        WHEN NEW.tournament_entity_type = 'tournament'
          AND lower(trim(NEW.role)) NOT IN ('admin', 'captain')
          THEN RAISE(ABORT, 'Invalid tournament access role')
      END;
    END;

    DROP TRIGGER IF EXISTS trg_tournaments_delete_access_users;
    CREATE TRIGGER trg_tournaments_delete_access_users
    AFTER DELETE ON tournaments
    BEGIN
      DELETE FROM tournament_access_users
      WHERE tournament_entity_type = 'tournament'
        AND upper(trim(tournament_id)) = upper(trim(OLD.id));
    END;

    DROP TRIGGER IF EXISTS trg_in_person_tournaments_delete_access_users;
    CREATE TRIGGER trg_in_person_tournaments_delete_access_users
    AFTER DELETE ON in_person_tournaments
    BEGIN
      DELETE FROM tournament_access_users
      WHERE tournament_entity_type = 'in_person_tournament'
        AND upper(trim(tournament_id)) = upper(trim(OLD.id));
    END;

    DROP TRIGGER IF EXISTS trg_in_person_tournaments_validate_city_insert;
    CREATE TRIGGER trg_in_person_tournaments_validate_city_insert
    BEFORE INSERT ON in_person_tournaments
    WHEN NEW.qualifier_city_id IS NOT NULL
    BEGIN
      SELECT CASE
        WHEN NOT EXISTS (
          SELECT 1
          FROM cities c
          WHERE c.id = NEW.qualifier_city_id
            AND upper(trim(c.association_id)) = upper(trim(NEW.association_id))
        )
          THEN RAISE(ABORT, 'Qualifier city must belong to tournament association')
      END;
    END;

    DROP TRIGGER IF EXISTS trg_in_person_tournaments_validate_city_update;
    CREATE TRIGGER trg_in_person_tournaments_validate_city_update
    BEFORE UPDATE OF qualifier_city_id, association_id ON in_person_tournaments
    WHEN NEW.qualifier_city_id IS NOT NULL
    BEGIN
      SELECT CASE
        WHEN NOT EXISTS (
          SELECT 1
          FROM cities c
          WHERE c.id = NEW.qualifier_city_id
            AND upper(trim(c.association_id)) = upper(trim(NEW.association_id))
        )
          THEN RAISE(ABORT, 'Qualifier city must belong to tournament association')
      END;
    END;

    DROP TRIGGER IF EXISTS trg_in_person_participants_validate_location_insert;
    CREATE TRIGGER trg_in_person_participants_validate_location_insert
    BEFORE INSERT ON in_person_participants
    BEGIN
      SELECT CASE
        WHEN NOT EXISTS (
          SELECT 1
          FROM in_person_tournaments t
          WHERE t.id = NEW.tournament_id
            AND (
              (t.scope = 'international'
                AND NEW.association_id IS NOT NULL
                AND NEW.city_id IS NULL)
              OR
              (t.scope = 'local'
                AND NEW.association_id IS NULL
                AND NEW.city_id IS NOT NULL
                AND EXISTS (
                  SELECT 1
                  FROM cities c
                  WHERE c.id = NEW.city_id
                    AND upper(trim(c.association_id)) = upper(trim(t.association_id))
                ))
            )
        )
          THEN RAISE(ABORT, 'Participant location does not match tournament scope')
      END;
    END;

    DROP TRIGGER IF EXISTS trg_in_person_participants_validate_location_update;
    CREATE TRIGGER trg_in_person_participants_validate_location_update
    BEFORE UPDATE OF tournament_id, association_id, city_id ON in_person_participants
    BEGIN
      SELECT CASE
        WHEN NOT EXISTS (
          SELECT 1
          FROM in_person_tournaments t
          WHERE t.id = NEW.tournament_id
            AND (
              (t.scope = 'international'
                AND NEW.association_id IS NOT NULL
                AND NEW.city_id IS NULL)
              OR
              (t.scope = 'local'
                AND NEW.association_id IS NULL
                AND NEW.city_id IS NOT NULL
                AND EXISTS (
                  SELECT 1
                  FROM cities c
                  WHERE c.id = NEW.city_id
                    AND upper(trim(c.association_id)) = upper(trim(t.association_id))
                ))
            )
        )
          THEN RAISE(ABORT, 'Participant location does not match tournament scope')
      END;
    END;

    DROP TRIGGER IF EXISTS trg_in_person_matches_unique_participant_insert;
    CREATE TRIGGER trg_in_person_matches_unique_participant_insert
    BEFORE INSERT ON in_person_matches
    WHEN NEW.status <> 'cancelled'
    BEGIN
      SELECT CASE
        WHEN EXISTS (
          SELECT 1
          FROM in_person_matches m
          WHERE m.round_id = NEW.round_id
            AND m.status <> 'cancelled'
            AND (
              (NEW.participant_a_id IS NOT NULL
                AND NEW.participant_a_id IN (m.participant_a_id, m.participant_b_id))
              OR
              (NEW.participant_b_id IS NOT NULL
                AND NEW.participant_b_id IN (m.participant_a_id, m.participant_b_id))
            )
        )
          THEN RAISE(ABORT, 'Participant already has an active match in this round')
      END;
    END;

    DROP TRIGGER IF EXISTS trg_in_person_matches_unique_participant_update;
    CREATE TRIGGER trg_in_person_matches_unique_participant_update
    BEFORE UPDATE OF round_id, participant_a_id, participant_b_id, status
    ON in_person_matches
    WHEN NEW.status <> 'cancelled'
    BEGIN
      SELECT CASE
        WHEN EXISTS (
          SELECT 1
          FROM in_person_matches m
          WHERE m.id <> OLD.id
            AND m.round_id = NEW.round_id
            AND m.status <> 'cancelled'
            AND (
              (NEW.participant_a_id IS NOT NULL
                AND NEW.participant_a_id IN (m.participant_a_id, m.participant_b_id))
              OR
              (NEW.participant_b_id IS NOT NULL
                AND NEW.participant_b_id IN (m.participant_a_id, m.participant_b_id))
            )
        )
          THEN RAISE(ABORT, 'Participant already has an active match in this round')
      END;
      END;

      COMMIT;
    `);
  } catch (error) {
    await rollbackQuietly(db);
    throw error;
  }
}

export async function ensureInPersonSchema(db, { logger = console } = {}) {
  const accessMigration = await migrateTournamentAccessUsers(db);
  await ensureInPersonTables(db);
  await ensureCityExtensions(db);
  await ensureInPersonTriggersAndAccessIndexes(db);
  logger?.info?.("[in-person] Schema foundation ready", {
    accessTableMigrated: accessMigration.migrated,
    accessTableCreated: accessMigration.created,
    accessRows: accessMigration.rowsAfter,
  });
  return { accessMigration };
}
