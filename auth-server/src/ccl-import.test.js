import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import sqlite3 from "sqlite3";

import { applyCclImportPlan, buildCclImportPlan, parseCclAssociationMap, readCclSourceRows } from "./ccl-import.js";
import { ensureCclSchema } from "./ccl-schema.js";

const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (error) => (error ? reject(error) : resolve())));
const all = (db, sql, params = []) => new Promise((resolve, reject) => {
  db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
});

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    PRAGMA foreign_keys = OFF;
    CREATE TABLE tournaments (id TEXT PRIMARY KEY COLLATE NOCASE, name TEXT, type TEXT, category TEXT);
    CREATE TABLE associations (code TEXT, name TEXT, flag TEXT);
    CREATE TABLE profiles (id TEXT PRIMARY KEY, bga_nickname TEXT, association TEXT, deleted_at TEXT);
    CREATE TABLE audit_trail (
      id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL, entity_type TEXT NOT NULL, action TEXT NOT NULL,
      record_id TEXT NOT NULL, actor_email TEXT, changes TEXT, metadata TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO tournaments VALUES
      ('CCL-2027', 'Carcassonne Champions League 2027', 'Individuals', 'CCL'),
      ('ETCOC-2026', 'ETCOC 2026', 'Teams', 'ETCOC');
    INSERT INTO associations VALUES
      ('AR', 'Argentina', NULL), ('UA', 'Ukraine', NULL), ('CZ', 'Czechia', NULL), ('US', 'USA', NULL), ('RCP', 'RCP', NULL);
    INSERT INTO profiles VALUES
      ('100', 'bignacho610', 'AR', NULL), ('200', 'adrear', 'Ukraine', NULL), ('300', 'posij118', 'UA', NULL);
  `);
  await ensureCclSchema(db);
  return db;
}

const sheet = {
  countries: [{ team: "United States", iso: "US" }],
  tournament_players: [
    { tournament_id: "CCL-2027Q", country: "Argentina", stage: "Main", qualified_by_name: "Associations 13-32", player_id: "" },
    { tournament_id: "CCL-2027Q", country: "Argentina", stage: "Q3", qualified_by_name: "KoC Championship Winner",
      qualified_by_link: "https://carcassonne.gg/King-of-Carcassonne/", player_id: "100", player: "bignacho610" },
    { tournament_id: "CCL-2027Q", country: "Ukraine", stage: "Q3", qualified_by_name: "MSO 2026 Winner", player_id: "200" },
    { tournament_id: "CCL-2027Q", country: "Ukraine", stage: "Q1", qualified_by_name: "Associations 5-12", player_id: "200" },
    { tournament_id: "CCL-2027Q", country: "Czech Republic", stage: "Q3", qualified_by_name: "ETCOC Winner", player_id: "300" },
    { tournament_id: "CCL-2027Q", country: "United States", stage: "Q2", qualified_by_name: "Associations 33-40", player_id: "999" },
    { tournament_id: "CCL-2027Q", country: "RCP", stage: "Q2", qualified_by_name: "Associations 33-40" },
    { tournament_id: "CCL-2027Q", country: "", stage: "Q3", qualified_by_name: "Fans Choice" },
    { tournament_id: "CCL-2026Q", country: "Argentina", stage: "Main", qualified_by_name: "Old season" },
  ],
};

async function planFor(db, overrides = {}) {
  const { rows, isoByCountry } = readCclSourceRows(sheet, "CCL-2027Q");
  return buildCclImportPlan(db, {
    rows,
    isoByCountry,
    associationMap: parseCclAssociationMap(["Czech Republic=CZ"]),
    tournamentId: "CCL-2027",
    sourceTournamentId: "CCL-2027Q",
    source: "sheet.json",
    ...overrides,
  });
}

test("the dry-run matches associations and keeps doubtful players out of their slots", async (t) => {
  const db = await createDatabase(t);
  const plan = await planFor(db);

  assert.deepEqual(plan.issues, []);
  assert.equal(plan.summary.ready, true);
  assert.deepEqual(plan.summary.by_stage, { Main: 1, Q1: 1, Q2: 2, Q3: 4 });
  assert.equal(plan.summary.slots, 8);
  assert.equal(plan.summary.without_association, 1);
  assert.deepEqual(
    plan.slots.map((slot) => [slot.association_id, slot.entry_stage, slot.player_id]),
    [
      ["AR", "Main", null],
      ["AR", "Q3", "100"],
      ["UA", "Q3", "200"],
      ["UA", "Q1", null], // duplicate player
      ["CZ", "Q3", null], // --map; the player belongs to Ukraine
      ["US", "Q2", null], // ISO code from the sheet; unknown player
      ["RCP", "Q2", null],
      [null, "Q3", null],
    ]
  );
  assert.deepEqual(plan.warnings.map((warning) => warning.code), ["PLAYER_DUPLICATE", "PLAYER_ASSOCIATION_MISMATCH", "PLAYER_NOT_FOUND"]);
  assert.equal(plan.slots[1].qualified_by_link, "https://carcassonne.gg/King-of-Carcassonne/");
  assert.equal(await all(db, "SELECT * FROM tournament_players").then((rows) => rows.length), 0);
});

test("unmatched countries, bad stages, bad links and a non-CCL target block the import", async (t) => {
  const db = await createDatabase(t);
  const broken = [
    { source_index: 0, country: "Atlantis", stage: "Main", qualified_by_name: "Associations 1-4", qualified_by_link: "", player_id: "" },
    { source_index: 1, country: "Argentina", stage: "Q4", qualified_by_name: "Associations 1-4", qualified_by_link: "", player_id: "" },
    { source_index: 2, country: "Argentina", stage: "Q1", qualified_by_name: "Cup", qualified_by_link: "ftp://x", player_id: "" },
  ];
  const plan = await planFor(db, { rows: broken });
  assert.deepEqual(plan.issues.map((issue) => issue.code), ["ASSOCIATION_NOT_FOUND", "STAGE_INVALID", "QUALIFIED_BY_INVALID"]);
  assert.equal(plan.summary.ready, false);

  const teams = await planFor(db, { tournamentId: "ETCOC-2026" });
  assert.deepEqual(teams.issues.map((issue) => issue.code), ["TOURNAMENT_NOT_CCL"]);
  await assert.rejects(applyCclImportPlan(db, plan), { code: "IMPORT_BLOCKED" });
});

test("apply writes the reviewed plan once, with a backup and an audit record", async (t) => {
  const db = await createDatabase(t);
  const backupDir = await mkdtemp(path.join(os.tmpdir(), "ccl-import-"));
  t.after(() => rm(backupDir, { recursive: true, force: true }));
  const plan = await planFor(db);

  const result = await applyCclImportPlan(db, plan, { backupDir });
  assert.equal(result.applied, true);
  assert.equal(result.slots, 8);
  assert.ok(existsSync(result.backup_path));

  const slots = await all(db, `
    SELECT p.association_id, p.entry_stage, p.player_id, p.created_by, e.qualified_by_name, e.qualified_by_link, e.elo, e.seed
    FROM tournament_players p JOIN ccl_stage_entries e ON e.slot_id = p.id AND e.stage = p.entry_stage
    WHERE p.tournament_id = 'CCL-2027'
  `);
  assert.equal(slots.length, 8);
  assert.ok(slots.every((slot) => slot.created_by === "import:ccl-slots" && slot.elo === null && slot.seed === null));
  assert.ok(slots.some((slot) => slot.player_id === "100" && slot.qualified_by_link === "https://carcassonne.gg/King-of-Carcassonne/"));
  const [audit] = await all(db, "SELECT * FROM audit_trail");
  assert.equal(audit.event_type, "ccl_slots.imported");
  assert.equal(JSON.parse(audit.metadata).slot_ids.length, 8);

  // The tournament is no longer empty: running the same plan again is blocked.
  await assert.rejects(applyCclImportPlan(db, plan, { backupDir }), { code: "IMPORT_BLOCKED" });
  assert.equal((await all(db, "SELECT id FROM tournament_players")).length, 8);
});

test("apply refuses edited plans and plans made stale by later database changes", async (t) => {
  const db = await createDatabase(t);
  const backupDir = await mkdtemp(path.join(os.tmpdir(), "ccl-import-"));
  t.after(() => rm(backupDir, { recursive: true, force: true }));
  const plan = await planFor(db);

  const edited = { ...plan, slots: plan.slots.map((slot) => ({ ...slot, player_id: "100" })) };
  await assert.rejects(applyCclImportPlan(db, edited, { backupDir }), { code: "INVALID_PLAN" });

  await exec(db, "UPDATE profiles SET association = 'AR' WHERE id = '300'");
  await assert.rejects(applyCclImportPlan(db, plan, { backupDir }), { code: "STALE_PLAN" });
  assert.equal((await all(db, "SELECT id FROM tournament_players")).length, 0);
});

test("--map entries need a country and a code", () => {
  assert.deepEqual(parseCclAssociationMap(["Czech Republic=CZ", " Hong Kong = HK "]), { "czech republic": "CZ", "hong kong": "HK" });
  assert.throws(() => parseCclAssociationMap(["Czech Republic"]), { code: "INVALID_MAP" });
});
