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
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
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
  const service = createInPersonService({
    db,
    idFactory: () => `00000000-0000-4000-8000-${String(++sequence).padStart(12, "0")}`,
    random: () => 0.37,
  });
  return { db, service };
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
  const { service } = await createDatabase(t);
  const regular = await service.createTournament(tournamentPayload({ slug: "regular-cup" }));
  await assert.rejects(
    service.addTestParticipants(regular.id, { count: 1 }),
    (error) => error?.code === "TEST_TOURNAMENT_REQUIRED"
  );
  await assert.rejects(
    service.resetTestTournamentData(regular.id),
    (error) => error?.code === "TEST_TOURNAMENT_REQUIRED"
  );

  const tournament = await service.createTournament(tournamentPayload({ is_test_tournament: true }));
  assert.equal(tournament.is_test_tournament, true);
  await assert.rejects(
    service.resetTestTournamentData(tournament.id),
    (error) => error?.code === "TEST_TOURNAMENT_NOT_PUBLISHED"
  );
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
  const { service } = await createDatabase(t);
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

test("resetting a test tournament removes competition data and recreates an empty playoff structure", async (t) => {
  const { db, service } = await createDatabase(t);
  const draft = await service.createTournament(tournamentPayload({
    slug: "reset-test-cup",
    is_test_tournament: true,
  }));
  await service.addTestParticipants(draft.id, { count: 4 });
  await service.publishTournament(draft.id);
  await service.startCheckIn(draft.id);
  await service.bulkCheckInTestParticipants(draft.id, { count: 4 });
  const participants = (await service.getParticipantsOverview(draft.id)).participants;
  const [playoffRound] = await all(
    db,
    "SELECT id FROM in_person_rounds WHERE tournament_id = ? AND stage = 'playoff' ORDER BY round_order",
    [draft.id]
  );
  const [playoffMatch] = await all(
    db,
    "SELECT id FROM in_person_matches WHERE round_id = ? ORDER BY bracket_position",
    [playoffRound.id]
  );

  await run(
    db,
    `INSERT INTO in_person_rounds (
      id, tournament_id, stage, round_number, status, completed_at
    ) VALUES ('reset-swiss-round', ?, 'swiss', 1, 'completed', CURRENT_TIMESTAMP)`,
    [draft.id]
  );
  await run(
    db,
    `INSERT INTO in_person_matches (
      id, round_id, table_number, participant_a_id, participant_b_id,
      starting_participant_id, status, result_type, points_a, points_b,
      winner_participant_id, loser_participant_id
    ) VALUES (
      'reset-swiss-match', 'reset-swiss-round', 1, ?, ?, ?,
      'completed', 'points', 80, 70, ?, ?
    )`,
    [participants[0].id, participants[1].id, participants[0].id, participants[0].id, participants[1].id]
  );
  await run(
    db,
    `INSERT INTO in_person_standings (
      tournament_id, revision, source_completed_round_id, participant_id, position, wins
    ) VALUES (?, 1, 'reset-swiss-round', ?, 1, 1)`,
    [draft.id, participants[0].id]
  );
  await run(
    db,
    `UPDATE in_person_rounds
     SET status = 'published', published_at = CURRENT_TIMESTAMP
     WHERE id = ?`,
    [playoffRound.id]
  );
  await run(
    db,
    `UPDATE in_person_matches
     SET participant_a_id = ?, participant_b_id = ?, starting_participant_id = ?,
         status = 'completed', result_type = 'points', points_a = 90, points_b = 75,
         winner_participant_id = ?, loser_participant_id = ?
     WHERE id = ?`,
    [
      participants[2].id,
      participants[3].id,
      participants[2].id,
      participants[2].id,
      participants[3].id,
      playoffMatch.id,
    ]
  );
  await run(
    db,
    "UPDATE in_person_tournaments SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?",
    [draft.id]
  );

  const result = await service.resetTestTournamentData(draft.id);
  assert.equal(result.reset, true);
  assert.equal(result.deleted_players, 4);
  assert.equal(result.tournament.status, "registration");
  assert.equal(result.tournament.completed_at, null);
  assert.equal((await service.getParticipantsOverview(draft.id)).participants.length, 0);
  assert.equal((await all(
    db,
    "SELECT id FROM in_person_rounds WHERE tournament_id = ? AND stage = 'swiss'",
    [draft.id]
  )).length, 0);
  assert.equal((await all(
    db,
    "SELECT participant_id FROM in_person_standings WHERE tournament_id = ?",
    [draft.id]
  )).length, 0);
  const playoffRounds = await all(
    db,
    "SELECT id, status FROM in_person_rounds WHERE tournament_id = ? AND stage = 'playoff'",
    [draft.id]
  );
  assert.equal(playoffRounds.length, 3);
  assert.ok(playoffRounds.every((round) => round.status === "draft"));
  const playoffMatches = await all(
    db,
    `SELECT m.* FROM in_person_matches m
     JOIN in_person_rounds r ON r.id = m.round_id
     WHERE r.tournament_id = ? AND r.stage = 'playoff'`,
    [draft.id]
  );
  assert.equal(playoffMatches.length, 4);
  playoffMatches.forEach((match) => {
    assert.equal(match.status, "scheduled");
    assert.equal(match.participant_a_id, null);
    assert.equal(match.participant_b_id, null);
    assert.equal(match.result_type, null);
    assert.equal(match.winner_participant_id, null);
  });
});
