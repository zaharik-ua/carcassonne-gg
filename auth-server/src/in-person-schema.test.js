import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import { ensureInPersonSchema } from "./in-person/schema.js";

const silentLogger = { info() {} };

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

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

async function createDatabase(t, { legacyAccessTable = false } = {}) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    PRAGMA foreign_keys = ON;
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      admin INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE associations (
      code TEXT UNIQUE COLLATE NOCASE,
      name TEXT NOT NULL UNIQUE COLLATE NOCASE
    );
    CREATE TABLE tournaments (
      id TEXT PRIMARY KEY
    );
  `);
  if (legacyAccessTable) {
    await exec(db, `
      CREATE TABLE tournament_access_users (
        tournament_id TEXT NOT NULL,
        user_id INTEGER NOT NULL,
        role TEXT NOT NULL DEFAULT 'captain',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (tournament_id, user_id)
      );
    `);
  }
  return db;
}

async function insertInternationalTournament(db, id, slug = id) {
  await run(
    db,
    `
      INSERT INTO in_person_tournaments (
        id, slug, name_en, scope, start_date, end_date,
        organizer_name, swiss_rounds_count, playoff_first_round
      )
      VALUES (?, ?, 'Test tournament', 'international', '2026-09-05', '2026-09-05',
        'Test organizer', 5, 'round_of_16')
    `,
    [id, slug]
  );
}

test("migrates legacy tournament access rows without changing their values", async (t) => {
  const db = await createDatabase(t, { legacyAccessTable: true });
  await exec(db, `
    INSERT INTO users (id) VALUES (1), (2);
    INSERT INTO tournaments (id) VALUES ('legacy-cup');
    INSERT INTO tournament_access_users (
      tournament_id, user_id, role, created_at, updated_at
    ) VALUES
      ('legacy-cup', 1, 'admin', '2025-01-01 10:00:00', '2025-01-02 10:00:00'),
      ('legacy-cup', 2, 'captain', '2025-02-01 10:00:00', '2025-02-02 10:00:00');
  `);

  const firstResult = await ensureInPersonSchema(db, { logger: silentLogger });
  assert.equal(firstResult.accessMigration.migrated, true);
  assert.equal(firstResult.accessMigration.rowsBefore, 2);
  assert.equal(firstResult.accessMigration.rowsAfter, 2);

  const rows = await all(
    db,
    `
      SELECT tournament_entity_type, tournament_id, user_id, role, created_at, updated_at
      FROM tournament_access_users
      ORDER BY user_id
    `
  );
  assert.deepEqual(rows, [
    {
      tournament_entity_type: "tournament",
      tournament_id: "legacy-cup",
      user_id: 1,
      role: "admin",
      created_at: "2025-01-01 10:00:00",
      updated_at: "2025-01-02 10:00:00",
    },
    {
      tournament_entity_type: "tournament",
      tournament_id: "legacy-cup",
      user_id: 2,
      role: "captain",
      created_at: "2025-02-01 10:00:00",
      updated_at: "2025-02-02 10:00:00",
    },
  ]);

  const columns = await all(db, "PRAGMA table_info(tournament_access_users)");
  const primaryKey = columns
    .filter((column) => Number(column.pk) > 0)
    .sort((left, right) => Number(left.pk) - Number(right.pk))
    .map((column) => column.name);
  assert.deepEqual(primaryKey, ["tournament_entity_type", "tournament_id", "user_id"]);

  const secondResult = await ensureInPersonSchema(db, { logger: silentLogger });
  assert.equal(secondResult.accessMigration.migrated, false);
  assert.equal(secondResult.accessMigration.rowsAfter, 2);
});

test("creates the in-person foundation without versioning or idempotency tables", async (t) => {
  const db = await createDatabase(t);
  await ensureInPersonSchema(db, { logger: silentLogger });

  const expectedTables = [
    "cities",
    "in_person_matches",
    "in_person_participants",
    "in_person_rounds",
    "in_person_schema_migrations",
    "in_person_standings",
    "in_person_tournaments",
    "tournament_access_users",
  ];
  const rows = await all(
    db,
    `
      SELECT name
      FROM sqlite_master
      WHERE type = 'table'
        AND (name LIKE 'in_person_%' OR name IN ('cities', 'tournament_access_users'))
      ORDER BY name
    `
  );
  assert.deepEqual(rows.map((row) => row.name), expectedTables);

  const roundColumns = new Set(
    (await all(db, "PRAGMA table_info(in_person_rounds)")).map((column) => column.name)
  );
  ["version", "replaces_round_id", "pairing_algorithm_version", "seed", "input_standings_revision"]
    .forEach((columnName) => assert.equal(roundColumns.has(columnName), false));

  const idempotencyTable = await get(
    db,
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'in_person_idempotency_keys'"
  );
  assert.equal(idempotencyTable, null);

  const cityColumns = new Set(
    (await all(db, "PRAGMA table_info(cities)")).map((column) => column.name)
  );
  assert.equal(cityColumns.has("icon_url"), true);
});

test("adds icon_url to an existing cities table without losing city data", async (t) => {
  const db = await createDatabase(t);
  await exec(db, `
    INSERT INTO associations (code, name) VALUES ('UKR', 'Ukraine');
    CREATE TABLE cities (
      id TEXT PRIMARY KEY,
      association_id TEXT NOT NULL,
      name_en TEXT NOT NULL,
      name_local TEXT,
      archived_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO cities (id, association_id, name_en, name_local)
    VALUES ('legacy-kyiv', 'UKR', 'Kyiv', 'Київ');
  `);

  await ensureInPersonSchema(db, { logger: silentLogger });
  const city = await get(
    db,
    "SELECT id, name_en, name_local, icon_url FROM cities WHERE id = 'legacy-kyiv'"
  );
  assert.deepEqual(city, {
    id: "legacy-kyiv",
    name_en: "Kyiv",
    name_local: "Київ",
    icon_url: null,
  });
});

test("repairs legacy Final and Bronze medal match table assignments", async (t) => {
  const db = await createDatabase(t);
  await ensureInPersonSchema(db, { logger: silentLogger });
  await insertInternationalTournament(db, "legacy-medal-tables");
  await exec(db, `
    INSERT INTO in_person_rounds (
      id, tournament_id, stage, round_key, round_order, status
    ) VALUES
      ('legacy-final', 'legacy-medal-tables', 'playoff', 'final', 4, 'draft'),
      ('legacy-bronze', 'legacy-medal-tables', 'playoff', 'bronze_medal_match', 4, 'draft');

    INSERT INTO in_person_matches (
      id, round_id, bracket_position, table_number
    ) VALUES
      ('legacy-final-match', 'legacy-final', 1, 7),
      ('legacy-bronze-match', 'legacy-bronze', 1, 1);

    DELETE FROM in_person_schema_migrations WHERE version = 3;
  `);

  const result = await ensureInPersonSchema(db, { logger: silentLogger });
  assert.equal(result.playoffMedalTableMigration.migrated, true);
  assert.equal(result.playoffMedalTableMigration.updatedRows, 2);
  const matches = await all(
    db,
    `SELECT id, table_number FROM in_person_matches
     WHERE id IN ('legacy-final-match', 'legacy-bronze-match')
     ORDER BY id`
  );
  assert.deepEqual(matches, [
    { id: "legacy-bronze-match", table_number: 2 },
    { id: "legacy-final-match", table_number: 1 },
  ]);
});

test("keeps access isolated when both tournament domains use the same id", async (t) => {
  const db = await createDatabase(t);
  await exec(db, `
    INSERT INTO users (id) VALUES (1);
    INSERT INTO tournaments (id) VALUES ('same-id');
  `);
  await ensureInPersonSchema(db, { logger: silentLogger });
  await insertInternationalTournament(db, "same-id");

  await run(
    db,
    `INSERT INTO tournament_access_users
      (tournament_entity_type, tournament_id, user_id, role)
     VALUES ('tournament', 'same-id', 1, 'captain')`
  );
  await run(
    db,
    `INSERT INTO tournament_access_users
      (tournament_entity_type, tournament_id, user_id, role)
     VALUES ('in_person_tournament', 'same-id', 1, 'admin')`
  );

  const beforeDelete = await all(
    db,
    `SELECT tournament_entity_type, role
     FROM tournament_access_users
     WHERE tournament_id = 'same-id'
     ORDER BY tournament_entity_type`
  );
  assert.deepEqual(beforeDelete, [
    { tournament_entity_type: "in_person_tournament", role: "admin" },
    { tournament_entity_type: "tournament", role: "captain" },
  ]);

  await run(db, "DELETE FROM tournaments WHERE id = 'same-id'");
  const afterDelete = await all(
    db,
    "SELECT tournament_entity_type, role FROM tournament_access_users WHERE tournament_id = 'same-id'"
  );
  assert.deepEqual(afterDelete, [
    { tournament_entity_type: "in_person_tournament", role: "admin" },
  ]);
});

test("rejects invalid in-person access targets and roles", async (t) => {
  const db = await createDatabase(t);
  await run(db, "INSERT INTO users (id) VALUES (1)");
  await ensureInPersonSchema(db, { logger: silentLogger });
  await insertInternationalTournament(db, "ipt-1");

  await assert.rejects(
    run(
      db,
      `INSERT INTO tournament_access_users
        (tournament_entity_type, tournament_id, user_id, role)
       VALUES ('in_person_tournament', 'missing', 1, 'admin')`
    ),
    /Unknown in-person tournament/
  );
  await assert.rejects(
    run(
      db,
      `INSERT INTO tournament_access_users
        (tournament_entity_type, tournament_id, user_id, role)
       VALUES ('in_person_tournament', 'ipt-1', 1, 'captain')`
    ),
    /must be admin/
  );
});

test("enforces active round, table, draw number and participant invariants", async (t) => {
  const db = await createDatabase(t);
  await exec(db, `
    INSERT INTO users (id) VALUES (1);
    INSERT INTO associations (code, name) VALUES ('UKR', 'Ukraine');
  `);
  await ensureInPersonSchema(db, { logger: silentLogger });
  await insertInternationalTournament(db, "ipt-1");

  await run(
    db,
    `INSERT INTO in_person_participants
      (id, tournament_id, name_en, association_id, status, draw_number)
     VALUES ('p1', 'ipt-1', 'Player One', 'UKR', 'checked_in', 2)`
  );
  await assert.rejects(
    run(
      db,
      `INSERT INTO in_person_participants
        (id, tournament_id, name_en, association_id, status, draw_number)
       VALUES ('p2-duplicate', 'ipt-1', 'Duplicate Draw', 'UKR', 'checked_in', 2)`
    ),
    /UNIQUE constraint failed/
  );
  await run(
    db,
    `INSERT INTO in_person_participants
      (id, tournament_id, name_en, association_id, status, draw_number)
     VALUES ('p2', 'ipt-1', 'Player Two', 'UKR', 'checked_in', 3)`
  );
  await run(
    db,
    `INSERT INTO in_person_participants
      (id, tournament_id, name_en, association_id, status, draw_number)
     VALUES ('p3', 'ipt-1', 'Player Three', 'UKR', 'checked_in', 5)`
  );
  await run(
    db,
    `INSERT INTO in_person_rounds (id, tournament_id, stage, round_number)
     VALUES ('round-1', 'ipt-1', 'swiss', 1)`
  );
  await assert.rejects(
    run(
      db,
      `INSERT INTO in_person_rounds (id, tournament_id, stage, round_number)
       VALUES ('round-1-duplicate', 'ipt-1', 'swiss', 1)`
    ),
    /UNIQUE constraint failed/
  );
  await run(
    db,
    `INSERT INTO in_person_matches
      (id, round_id, table_number, participant_a_id, participant_b_id, starting_participant_id)
     VALUES ('match-1', 'round-1', 1, 'p1', 'p2', 'p1')`
  );
  await assert.rejects(
    run(
      db,
      `INSERT INTO in_person_matches
        (id, round_id, table_number, participant_a_id, participant_b_id, starting_participant_id)
       VALUES ('match-2', 'round-1', 2, 'p1', 'p3', 'p3')`
    ),
    /already has an active match/
  );

  await run(
    db,
    `UPDATE in_person_rounds
     SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP
     WHERE id = 'round-1'`
  );
  await run(
    db,
    `INSERT INTO in_person_rounds (id, tournament_id, stage, round_number)
     VALUES ('round-1-regenerated', 'ipt-1', 'swiss', 1)`
  );
});
