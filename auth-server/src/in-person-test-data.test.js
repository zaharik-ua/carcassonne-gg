import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import { ensureInPersonSchema } from "./in-person/schema.js";
import { createInPersonService } from "./in-person/service.js";
import { IN_PERSON_TEST_PLAYERS } from "./in-person/test-players.js";

const silentLogger = { info() {} };

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
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
    CREATE TABLE profiles (id TEXT PRIMARY KEY, bga_nickname TEXT);
    CREATE TABLE associations (
      code TEXT UNIQUE COLLATE NOCASE,
      name TEXT NOT NULL UNIQUE COLLATE NOCASE,
      flag TEXT
    );
    CREATE TABLE tournaments (id TEXT PRIMARY KEY);
    INSERT INTO users (id, email, name, admin)
    VALUES (1, 'admin@example.com', 'Admin', 1);
  `);
  const associationRows = [...new Set(IN_PERSON_TEST_PLAYERS.map((player) => player.association_id))]
    .map((code) => `('${code}', '${code}')`)
    .join(",");
  await exec(db, `INSERT INTO associations (code, name) VALUES ${associationRows}`);
  await ensureInPersonSchema(db, { logger: silentLogger });
  let sequence = 0;
  return createInPersonService({
    db,
    idFactory: () => `00000000-0000-4000-8000-${String(++sequence).padStart(12, "0")}`,
    random: () => 0.37,
  });
}

function tournamentPayload(overrides = {}) {
  return {
    slug: "test-cup",
    name_en: "Test Cup",
    scope: "international",
    start_date: "2026-10-10",
    end_date: "2026-10-10",
    organizer_name: "Organizer",
    swiss_rounds_count: 5,
    playoff_first_round: "semi_final",
    admin_user_ids: [1],
    ...overrides,
  };
}

test("test tournaments add up to 60 random fixture players while manual additions remain unlimited", async (t) => {
  const service = await createDatabase(t);
  const regular = await service.createTournament(tournamentPayload({ slug: "regular-cup" }));
  await assert.rejects(
    service.addTestParticipants(regular.id, { count: 1 }),
    (error) => error?.code === "TEST_TOURNAMENT_REQUIRED"
  );

  const tournament = await service.createTournament(tournamentPayload({ is_test_tournament: true }));
  assert.equal(tournament.is_test_tournament, true);
  assert.equal((await service.getParticipantsOverview(tournament.id)).test_data.available_players, 60);

  assert.equal((await service.addTestParticipants(tournament.id, { count: 12 })).added, 12);
  assert.equal((await service.addTestParticipants(tournament.id, { count: 48 })).added, 48);
  const filled = await service.getParticipantsOverview(tournament.id);
  assert.equal(filled.participants.length, 60);
  assert.equal(filled.test_data.available_players, 0);
  assert.equal(new Set(filled.participants.map((player) => player.name_en)).size, 60);
  await assert.rejects(
    service.addTestParticipants(tournament.id, { count: 1 }),
    (error) => error?.code === "NOT_ENOUGH_TEST_PLAYERS_AVAILABLE"
  );

  await service.createParticipant(tournament.id, {
    name_en: "Manual Player 61",
    association_id: "UKR",
  });
  assert.equal((await service.getParticipantsOverview(tournament.id)).participants.length, 61);
});

test("bulk test check-in selects remaining players and assigns unique random draw numbers", async (t) => {
  const service = await createDatabase(t);
  const draft = await service.createTournament(tournamentPayload({ is_test_tournament: true }));
  await service.addTestParticipants(draft.id, { count: 20 });
  await service.publishTournament(draft.id);
  await service.startCheckIn(draft.id);

  assert.equal((await service.bulkCheckInTestParticipants(draft.id, { count: 7 })).checked_in, 7);
  assert.equal((await service.bulkCheckInTestParticipants(draft.id, { count: 13 })).checked_in, 13);
  const overview = await service.getParticipantsOverview(draft.id);
  assert.equal(overview.counters.checked_in, 20);
  assert.equal(overview.counters.awaiting_check_in, 0);
  const drawNumbers = overview.participants.map((participant) => participant.draw_number);
  assert.equal(new Set(drawNumbers).size, 20);
  drawNumbers.forEach((drawNumber) => {
    assert.ok(Number.isInteger(drawNumber));
    assert.ok(drawNumber >= 1 && drawNumber <= 20);
  });
  await assert.rejects(
    service.bulkCheckInTestParticipants(draft.id, { count: 1 }),
    (error) => error?.code === "INVALID_TEST_PLAYER_COUNT"
      || error?.code === "NOT_ENOUGH_TEST_PLAYERS_AVAILABLE"
  );
});

