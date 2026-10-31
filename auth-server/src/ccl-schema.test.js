import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import { ensureCclSchema } from "./ccl-schema.js";

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, (error) => (error ? reject(error) : resolve()));
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function close(db) {
  return new Promise((resolve, reject) => {
    db.close((error) => (error ? reject(error) : resolve()));
  });
}

const PARENT_TABLES_SQL = `
  CREATE TABLE tournaments (id TEXT PRIMARY KEY COLLATE NOCASE);
  CREATE TABLE teams (id TEXT PRIMARY KEY);
  CREATE TABLE profiles (id TEXT PRIMARY KEY, association TEXT);
  CREATE TABLE tournament_teams (
    id TEXT PRIMARY KEY COLLATE NOCASE,
    tournament_id TEXT NOT NULL COLLATE NOCASE,
    team_id TEXT NOT NULL COLLATE NOCASE,
    captain_id TEXT,
    UNIQUE (tournament_id, team_id)
  );
  INSERT INTO tournaments (id) VALUES ('ETCOC-2026'), ('CCL-2027');
  INSERT INTO teams (id) VALUES ('UA');
  INSERT INTO profiles (id, association) VALUES ('p1', 'UA'), ('p2', 'UA'), ('p3', 'AR');
  INSERT INTO tournament_teams (id, tournament_id, team_id, captain_id) VALUES ('ETCOC-2026|UA', 'ETCOC-2026', 'UA', 'p1');
`;

const LEGACY_TOURNAMENT_PLAYERS_SQL = `
  CREATE TABLE tournament_players (
    id TEXT PRIMARY KEY COLLATE NOCASE,
    tournament_id TEXT NOT NULL COLLATE NOCASE,
    team_id TEXT NOT NULL COLLATE NOCASE,
    player_id TEXT NOT NULL,
    captain BOOLEAN NOT NULL DEFAULT 0 CHECK (captain IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (tournament_id, team_id, player_id),
    FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE,
    FOREIGN KEY (team_id) REFERENCES teams(id),
    FOREIGN KEY (player_id) REFERENCES profiles(id),
    FOREIGN KEY (tournament_id, team_id)
      REFERENCES tournament_teams(tournament_id, team_id)
      ON DELETE CASCADE
  );
  INSERT INTO tournament_players (id, tournament_id, team_id, player_id, captain, created_at, updated_at)
  VALUES
    ('ETCOC-2026|UA|p1', 'ETCOC-2026', 'UA', 'p1', 1, '2026-01-01 10:00:00', '2026-01-02 10:00:00'),
    ('ETCOC-2026|UA|p2', 'ETCOC-2026', 'UA', 'p2', 0, '2026-01-01 11:00:00', '2026-01-02 11:00:00');
`;

async function openDb(t, { legacy = true } = {}) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => close(db));
  await exec(db, "PRAGMA foreign_keys = ON");
  await exec(db, PARENT_TABLES_SQL);
  if (legacy) await exec(db, LEGACY_TOURNAMENT_PLAYERS_SQL);
  return db;
}

const insertSlot = (db, values) => run(
  db,
  `INSERT INTO tournament_players (id, tournament_id, team_id, player_id, association_id, entry_stage, league_status)
   VALUES (?, ?, ?, ?, ?, ?, ?)`,
  [
    values.id,
    values.tournament_id ?? "CCL-2027",
    values.team_id ?? null,
    values.player_id ?? null,
    values.association_id ?? null,
    values.entry_stage ?? null,
    values.league_status ?? null,
  ]
);

const insertEntry = (db, values) => run(
  db,
  `INSERT INTO ccl_stage_entries (
     tournament_id, stage, slot_id, qualified_by_name, qualified_by_link,
     player_id, elo, seed, elo_source, fixed_at, fixed_by
   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  [
    values.tournament_id ?? "CCL-2027",
    values.stage,
    values.slot_id,
    values.qualified_by_name ?? "Copa America Winner",
    values.qualified_by_link ?? null,
    values.player_id ?? null,
    values.elo ?? null,
    values.seed ?? null,
    values.elo_source ?? null,
    values.fixed_at ?? null,
    values.fixed_by ?? null,
  ]
);

test("legacy tournament_players is upgraded once and keeps team rosters intact", async (t) => {
  const db = await openDb(t);
  await ensureCclSchema(db);
  await ensureCclSchema(db);

  const columns = await all(db, "PRAGMA table_info(tournament_players)");
  const byName = new Map(columns.map((column) => [column.name, column]));
  for (const name of ["association_id", "entry_stage", "league_status", "created_by", "updated_by"]) {
    assert.ok(byName.has(name), `missing ${name}`);
  }
  assert.equal(byName.get("team_id").notnull, 0);
  assert.equal(byName.get("player_id").notnull, 0);

  assert.deepEqual(
    await all(db, "SELECT id, team_id, player_id, captain, created_at, updated_at, entry_stage FROM tournament_players ORDER BY id"),
    [
      { id: "ETCOC-2026|UA|p1", team_id: "UA", player_id: "p1", captain: 1, created_at: "2026-01-01 10:00:00", updated_at: "2026-01-02 10:00:00", entry_stage: null },
      { id: "ETCOC-2026|UA|p2", team_id: "UA", player_id: "p2", captain: 0, created_at: "2026-01-01 11:00:00", updated_at: "2026-01-02 11:00:00", entry_stage: null },
    ]
  );
  const indexes = (await all(db, "PRAGMA index_list(tournament_players)")).map((index) => index.name);
  for (const name of [
    "idx_tournament_players_tournament_team_player",
    "idx_tournament_players_player_id",
    "idx_tournament_players_ccl_stage",
    "idx_tournament_players_ccl_player",
  ]) {
    assert.ok(indexes.includes(name), `missing index ${name}`);
  }
  assert.equal((await all(db, "PRAGMA foreign_keys"))[0].foreign_keys, 1);
  assert.deepEqual(await all(db, "PRAGMA foreign_key_check"), []);
});

test("a database without tournament_players gets the CCL-ready table", async (t) => {
  const db = await openDb(t, { legacy: false });
  await ensureCclSchema(db);
  const columns = (await all(db, "PRAGMA table_info(tournament_players)")).map((column) => column.name);
  assert.ok(columns.includes("entry_stage"));
  assert.ok((await all(db, "PRAGMA table_info(ccl_stage_entries)")).length > 0);
});

test("tournament_players separates team rows from CCL slots", async (t) => {
  const db = await openDb(t);
  await ensureCclSchema(db);

  await insertSlot(db, { id: "s-empty", entry_stage: "Q3" });
  await insertSlot(db, { id: "s-unassigned", entry_stage: "Q3" });
  await insertSlot(db, { id: "s-main", entry_stage: "Main", association_id: "UA", player_id: "p1" });
  await insertSlot(db, { id: "s-q1", entry_stage: "Q1", association_id: "UA", player_id: "p2", league_status: "qualified" });

  const rejected = [
    // Team row without a team, CCL slot with a team.
    { id: "x1", tournament_id: "ETCOC-2026", player_id: "p1" },
    { id: "x2", entry_stage: "Main", team_id: "UA", association_id: "UA" },
    { id: "x3", entry_stage: "Q4" },
    // A player needs an association; a result needs a Q slot.
    { id: "x4", entry_stage: "Q2", player_id: "p3" },
    { id: "x5", entry_stage: "Main", league_status: "qualified" },
    { id: "x6", entry_stage: "Q2", league_status: "won" },
    // One slot per player in a tournament.
    { id: "x7", entry_stage: "Q2", association_id: "UA", player_id: "p1" },
  ];
  for (const values of rejected) {
    await assert.rejects(insertSlot(db, values), /constraint failed/, values.id);
  }
});

test("ccl_stage_entries keeps Elo fields consistent and follows its slot", async (t) => {
  const db = await openDb(t);
  await ensureCclSchema(db);
  await insertSlot(db, { id: "s-main", entry_stage: "Main", association_id: "UA", player_id: "p1" });
  await insertSlot(db, { id: "s-q1", entry_stage: "Q1", association_id: "UA", player_id: "p2" });

  await insertEntry(db, { stage: "Main", slot_id: "s-main" });
  await insertEntry(db, {
    stage: "Q1",
    slot_id: "s-q1",
    player_id: "p2",
    elo: null,
    elo_source: "gg_elo",
    seed: "Unseeded",
    fixed_at: "2026-11-12T10:00:00Z",
    fixed_by: "1",
  });

  const rejected = [
    { stage: "Main", slot_id: "s-q1", qualified_by_name: " " },
    { stage: "Q2", slot_id: "s-main", seed: "Pot 1", player_id: "p1", elo_source: "gg_elo", fixed_at: "now" },
    { stage: "Q2", slot_id: "s-q1", seed: "Seeded" },
    { stage: "Q2", slot_id: "s-q1", player_id: "p2", elo_source: "manual", fixed_at: "now" },
    { stage: "Q2", slot_id: "s-q1", player_id: "p2", fixed_at: "now" },
    { stage: "Q2", slot_id: "missing-slot" },
  ];
  for (const values of rejected) {
    await assert.rejects(insertEntry(db, values), /constraint failed/, JSON.stringify(values));
  }

  await run(db, "DELETE FROM tournament_players WHERE id = 's-q1'");
  assert.deepEqual(
    await all(db, "SELECT stage, slot_id FROM ccl_stage_entries ORDER BY stage"),
    [{ stage: "Main", slot_id: "s-main" }]
  );
});
