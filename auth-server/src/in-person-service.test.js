import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import { ensureInPersonSchema } from "./in-person/schema.js";
import { createInPersonService } from "./in-person/service.js";

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

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    PRAGMA foreign_keys = ON;
    CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      bga_id TEXT,
      email TEXT,
      name TEXT,
      admin INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE profiles (
      id TEXT PRIMARY KEY,
      bga_nickname TEXT,
      deleted_at TEXT
    );
    CREATE TABLE associations (
      code TEXT UNIQUE COLLATE NOCASE,
      name TEXT NOT NULL UNIQUE COLLATE NOCASE,
      flag TEXT
    );
    CREATE TABLE tournaments (id TEXT PRIMARY KEY);
    INSERT INTO associations (code, name, flag) VALUES
      ('UKR', 'Ukraine', 'ua'),
      ('POL', 'Poland', 'pl');
    INSERT INTO users (id, bga_id, email, name, admin) VALUES
      (1, 'p1', 'one@example.com', 'One', 0),
      (2, 'p2', 'two@example.com', 'Two', 0),
      (3, NULL, 'admin@example.com', 'Global Admin', 1);
    INSERT INTO profiles (id, bga_nickname) VALUES ('p1', 'one-bga'), ('p2', 'two-bga');
  `);
  await ensureInPersonSchema(db, { logger: silentLogger });
  let sequence = 0;
  const service = createInPersonService({
    db,
    idFactory: () => `00000000-0000-4000-8000-${String(++sequence).padStart(12, "0")}`,
  });
  return { db, service };
}

function tournamentPayload(overrides = {}) {
  return {
    slug: "ukraine-championship-2026",
    name_en: "Ukraine Championship 2026",
    name_local: "Чемпіонат України 2026",
    scope: "international",
    association_id: null,
    local_subtype: null,
    qualifier_city_id: null,
    start_date: "2026-08-22",
    end_date: "2026-08-23",
    organizer_name: "Carcassonne Ukraine",
    organizer_url: "https://carcassonne.com.ua",
    rules_url: "https://carcassonne.com.ua/rules",
    swiss_rounds_count: 6,
    playoff_first_round: "quarter_final",
    admin_user_ids: [1, 2],
    ...overrides,
  };
}

test("creates, updates, archives and restores cities with association-scoped uniqueness", async (t) => {
  const { service } = await createDatabase(t);
  const kyiv = await service.createCity({
    association_id: "ukr",
    name_en: "Kyiv",
    name_local: "Київ",
  });
  assert.match(kyiv.id, /^city_/);
  assert.equal(kyiv.association_id, "UKR");
  assert.equal(kyiv.archived, false);

  await assert.rejects(
    service.createCity({ association_id: "UKR", name_en: " kyiv " }),
    (error) => error?.status === 409 && error?.code === "DUPLICATE_CITY"
  );

  const polishKyiv = await service.createCity({ association_id: "POL", name_en: "Kyiv" });
  assert.equal(polishKyiv.association_id, "POL");

  const archived = await service.archiveCity(kyiv.id);
  assert.equal(archived.archived, true);
  assert.deepEqual((await service.listCities()).map((city) => city.id), [polishKyiv.id]);

  const replacement = await service.createCity({ association_id: "UKR", name_en: "Kyiv" });
  await assert.rejects(
    service.restoreCity(kyiv.id),
    (error) => error?.status === 409 && error?.code === "DUPLICATE_CITY"
  );
  await service.archiveCity(replacement.id);
  assert.equal((await service.restoreCity(kyiv.id)).archived, false);
});

test("creates international, local final and local qualifier tournament configurations", async (t) => {
  const { service } = await createDatabase(t);
  const kyiv = await service.createCity({ association_id: "UKR", name_en: "Kyiv" });

  const international = await service.createTournament(tournamentPayload());
  assert.match(international.id, /^ipt_/);
  assert.equal(international.status, "draft");
  assert.equal(international.association_id, null);
  assert.equal(international.admins.length, 2);
  assert.deepEqual(international.playoff_preview, {
    participant_count: 8,
    rounds: ["Quarter-final", "Semi-final", "Bronze medal match", "Final"],
    includes_bronze_match: true,
  });

  const localFinal = await service.createTournament(tournamentPayload({
    slug: "ukraine-final-2026",
    scope: "local",
    association_id: "UKR",
    local_subtype: "final",
    qualifier_city_id: kyiv.id,
    start_date: "2026-09-01",
    end_date: "2026-09-01",
  }));
  assert.equal(localFinal.association_id, "UKR");
  assert.equal(localFinal.qualifier_city_id, null, "local final must clear a hidden city value");
  assert.equal(localFinal.start_date, localFinal.end_date);

  const qualifier = await service.createTournament(tournamentPayload({
    slug: "kyiv-qualifier-2026",
    scope: "local",
    association_id: "UKR",
    local_subtype: "qualifier",
    qualifier_city_id: kyiv.id,
  }));
  assert.equal(qualifier.qualifier_city_name_en, "Kyiv");
});

test("rejects invalid conditional fields, periods, URLs and duplicate slugs", async (t) => {
  const { service } = await createDatabase(t);
  const warsaw = await service.createCity({ association_id: "POL", name_en: "Warsaw" });

  await assert.rejects(
    service.createTournament(tournamentPayload({
      scope: "local",
      association_id: "UKR",
      local_subtype: "qualifier",
      qualifier_city_id: warsaw.id,
    })),
    (error) => error?.code === "QUALIFIER_CITY_ASSOCIATION_MISMATCH"
  );
  await assert.rejects(
    service.createTournament(tournamentPayload({ end_date: "2026-08-21" })),
    (error) => error?.code === "INVALID_DATE_PERIOD"
  );
  await assert.rejects(
    service.createTournament(tournamentPayload({ organizer_url: "javascript:alert(1)" })),
    (error) => error?.code === "INVALID_URL"
  );

  await service.createTournament(tournamentPayload());
  await assert.rejects(
    service.createTournament(tournamentPayload({ name_en: "Duplicate" })),
    (error) => error?.status === 409 && error?.code === "DUPLICATE_SLUG"
  );
});

test("publishes a draft, locks its slug and cancels only before Swiss starts", async (t) => {
  const { db, service } = await createDatabase(t);
  const created = await service.createTournament(tournamentPayload());
  const published = await service.publishTournament(created.id);
  assert.equal(published.status, "registration");
  assert.ok(published.published_at);
  assert.equal((await service.publishTournament(created.id)).status, "registration");

  await assert.rejects(
    service.updateTournament(created.id, { slug: "changed-after-publish" }),
    (error) => error?.status === 409 && error?.code === "SLUG_IMMUTABLE"
  );
  const updated = await service.updateTournament(created.id, { name_en: "Updated name" });
  assert.equal(updated.name_en, "Updated name");

  await run(
    db,
    `INSERT INTO in_person_rounds (id, tournament_id, stage, round_number, status)
     VALUES ('r1', ?, 'swiss', 1, 'published')`,
    [created.id]
  );
  await assert.rejects(
    service.updateTournament(created.id, { swiss_rounds_count: 7 }),
    (error) => error?.status === 409 && error?.code === "FORMAT_LOCKED"
  );
  await assert.rejects(
    service.cancelTournament(created.id),
    (error) => error?.status === 409 && error?.code === "TOURNAMENT_ALREADY_STARTED"
  );
});

test("cancels a not-started tournament and makes it read-only", async (t) => {
  const { service } = await createDatabase(t);
  const created = await service.createTournament(tournamentPayload());
  const cancelled = await service.cancelTournament(created.id);
  assert.equal(cancelled.status, "cancelled");
  assert.ok(cancelled.cancelled_at);
  assert.equal((await service.cancelTournament(created.id)).status, "cancelled");
  await assert.rejects(
    service.updateTournament(created.id, { name_en: "No longer editable" }),
    (error) => error?.code === "TOURNAMENT_READ_ONLY"
  );
});

test("replaces tournament admins atomically and keeps the legacy access domain isolated", async (t) => {
  const { db, service } = await createDatabase(t);
  const created = await service.createTournament(tournamentPayload({ admin_user_ids: [1] }));
  await run(db, "INSERT INTO tournaments (id) VALUES (?)", [created.id]);
  await run(
    db,
    `INSERT INTO tournament_access_users
      (tournament_entity_type, tournament_id, user_id, role)
     VALUES ('tournament', ?, 1, 'captain')`,
    [created.id]
  );

  const admins = await service.replaceTournamentAdmins(created.id, [2, 3, 2]);
  assert.deepEqual(admins.map((admin) => admin.user_id), [3, 2]);
  const legacy = await get(
    db,
    `SELECT role FROM tournament_access_users
     WHERE tournament_entity_type = 'tournament' AND tournament_id = ? AND user_id = 1`,
    [created.id]
  );
  assert.deepEqual(legacy, { role: "captain" });

  await assert.rejects(
    service.replaceTournamentAdmins(created.id, [999]),
    (error) => error?.code === "UNKNOWN_ADMIN_USER"
  );
  assert.deepEqual(
    (await service.listTournamentAdmins(created.id)).map((admin) => admin.user_id),
    [3, 2],
    "failed replacement must roll back the previous admin list"
  );
});

test("invalid tournament admin makes the entire create command roll back", async (t) => {
  const { db, service } = await createDatabase(t);
  await assert.rejects(
    service.createTournament(tournamentPayload({ admin_user_ids: [999] })),
    (error) => error?.code === "UNKNOWN_ADMIN_USER"
  );
  assert.equal((await get(db, "SELECT COUNT(*) AS count FROM in_person_tournaments")).count, 0);
});
