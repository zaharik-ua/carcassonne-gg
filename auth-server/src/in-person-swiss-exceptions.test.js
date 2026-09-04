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

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
  });
}

async function createContext(t, { faultInjector = null } = {}) {
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
    INSERT INTO associations (code, name, flag) VALUES ('UKR', 'Ukraine', 'ua');
    INSERT INTO users (id, email, name, admin)
    VALUES (1, 'organizer@example.com', 'Organizer', 0);
  `);
  await ensureInPersonSchema(db, { logger: silentLogger });
  let sequence = 0;
  const service = createInPersonService({
    db,
    faultInjector,
    idFactory: () => `10000000-0000-4000-8000-${String(++sequence).padStart(12, "0")}`,
  });
  return { db, service };
}

async function createReadyTournament(service, participantCount, suffix, swissRoundsCount = 3) {
  const tournament = await service.createTournament({
    slug: `exceptions-${suffix}`,
    name_en: `Exceptions ${suffix}`,
    scope: "international",
    start_date: "2026-11-01",
    end_date: "2026-11-01",
    organizer_name: "Organizer",
    swiss_rounds_count: swissRoundsCount,
    playoff_first_round: participantCount >= 8 ? "quarter_final" : "semi_final",
    admin_user_ids: [1],
  });
  await service.publishTournament(tournament.id);
  const participants = [];
  for (let index = 0; index < participantCount; index += 1) {
    participants.push(await service.createParticipant(tournament.id, {
      name_en: `Player ${suffix}-${index + 1}`,
      association_id: "UKR",
    }));
  }
  await service.startCheckIn(tournament.id);
  for (let index = 0; index < participants.length; index += 1) {
    await service.setParticipantCheckIn(tournament.id, participants[index].id, {
      checked_in: true,
      draw_number: index + 2,
    });
  }
  return { tournament, participants };
}

async function createPublishedRound(service, tournamentId, roundNumber) {
  let overview = await service.confirmSwissRound(tournamentId, { round_number: roundNumber });
  overview = await service.publishSwissRound(tournamentId, overview.current_round.id);
  return overview.current_round;
}

async function saveRoundResults(service, tournamentId, round) {
  for (const match of round.matches.filter((entry) => !entry.is_bye)) {
    await service.saveSwissMatchResult(tournamentId, match.id, {
      starting_participant_id: match.starting_participant_id,
      result_type: "simple",
      winner_participant_id: match.participant_a_id,
    });
  }
}

async function completeRound(service, tournamentId, roundNumber) {
  const round = await createPublishedRound(service, tournamentId, roundNumber);
  await saveRoundResults(service, tournamentId, round);
  return service.completeSwissRound(tournamentId, round.id);
}

test("previews and cancels exactly one last Swiss round, then rebuilds and regenerates", async (t) => {
  const { db, service } = await createContext(t);
  const { tournament } = await createReadyTournament(service, 4, "rollback");
  let overview = await completeRound(service, tournament.id, 1);
  const firstRound = overview.current_round;
  assert.equal(overview.standings.revision, 1);

  const secondRound = await createPublishedRound(service, tournament.id, 2);
  const playedMatch = secondRound.matches.find((match) => !match.is_bye);
  await service.saveSwissMatchResult(tournament.id, playedMatch.id, {
    starting_participant_id: playedMatch.starting_participant_id,
    result_type: "points",
    points_a: 90,
    points_b: 70,
  });
  const preview = await service.previewSwissRoundCancellation(tournament.id, secondRound.id);
  assert.equal(preview.completed_results_count, 1);
  assert.equal(preview.previous_standings_revision, 1);
  assert.equal(preview.completed_results[0].points_a, 90);

  overview = await service.cancelSwissRound(
    tournament.id,
    secondRound.id,
    { reason: "Correct round one result" },
    { id: 1 }
  );
  assert.equal(overview.cancelled, true);
  assert.equal(overview.current_round.id, firstRound.id);
  assert.equal(overview.standings.revision, 1);
  assert.equal(
    (await get(db, "SELECT status FROM in_person_rounds WHERE id = ?", [secondRound.id])).status,
    "cancelled"
  );
  assert.equal(
    (await get(db, "SELECT COUNT(*) AS count FROM in_person_matches WHERE round_id = ? AND status <> 'cancelled'", [secondRound.id])).count,
    0
  );

  const firstMatch = firstRound.matches.find((match) => !match.is_bye);
  const correctedWinner = firstMatch.winner_participant_id === firstMatch.participant_a_id
    ? firstMatch.participant_b_id
    : firstMatch.participant_a_id;
  overview = await service.saveSwissMatchResult(tournament.id, firstMatch.id, {
    starting_participant_id: firstMatch.starting_participant_id,
    result_type: "simple",
    winner_participant_id: correctedWinner,
  });
  assert.equal(overview.current_round.status, "published");
  assert.equal(overview.standings.revision, 0);
  overview = await service.completeSwissRound(tournament.id, firstRound.id);
  assert.equal(overview.standings.revision, 2);

  overview = await service.confirmSwissRound(tournament.id, { round_number: 2 });
  assert.notEqual(overview.current_round.id, secondRound.id);
  assert.equal(overview.current_round.round_number, 2);
  assert.equal(
    (await get(db, "SELECT COUNT(*) AS count FROM in_person_rounds WHERE tournament_id = ? AND round_number = 2", [tournament.id])).count,
    2
  );
});

test("supports sequential rollback of round 3 and then round 2", async (t) => {
  const { service } = await createContext(t);
  const { tournament } = await createReadyTournament(service, 4, "sequential");
  const first = await completeRound(service, tournament.id, 1);
  const second = await completeRound(service, tournament.id, 2);
  const third = await completeRound(service, tournament.id, 3);
  assert.equal(third.standings.revision, 3);

  await assert.rejects(
    service.cancelSwissRound(tournament.id, first.current_round.id, { reason: "Wrong target" }),
    (error) => error?.code === "NOT_LAST_SWISS_ROUND"
  );
  let overview = await service.cancelSwissRound(
    tournament.id,
    third.current_round.id,
    { reason: "Rollback round three" }
  );
  assert.equal(overview.current_round.id, second.current_round.id);
  assert.equal(overview.standings.revision, 2);
  const secondPreview = await service.previewSwissRoundCancellation(
    tournament.id,
    second.current_round.id
  );
  assert.equal(secondPreview.previous_standings_revision, 1);
  overview = await service.cancelSwissRound(
    tournament.id,
    second.current_round.id,
    { reason: "Rollback round two" }
  );
  assert.equal(overview.current_round.id, first.current_round.id);
  assert.equal(overview.standings.revision, 1);
});

test("withdrawal excludes a player from future pairings and proposes current-round recovery", async (t) => {
  const { service } = await createContext(t);
  const { tournament, participants } = await createReadyTournament(service, 8, "withdrawal");
  await completeRound(service, tournament.id, 1);

  let statusResult = await service.setParticipantInactive(tournament.id, participants[0].id, {
    status: "withdrawn",
    reason: "Left the venue",
  });
  assert.equal(statusResult.participant.draw_number, null);
  assert.equal(statusResult.resolution, null);
  let preview = await service.previewSwissRound(tournament.id, { round_number: 2 });
  assert.equal(preview.matches.filter((match) => match.is_bye).length, 1);
  assert.equal(preview.matches.some((match) => (
    match.participant_a_id === participants[0].id || match.participant_b_id === participants[0].id
  )), false);

  await service.setParticipantInactive(tournament.id, participants[1].id, {
    status: "disqualified",
    reason: "Judge decision",
  });
  preview = await service.previewSwissRound(tournament.id, { round_number: 2 });
  assert.equal(preview.matches.filter((match) => match.is_bye).length, 0);

  let overview = await service.confirmSwissRound(tournament.id, { round_number: 2 });
  const draftParticipant = overview.current_round.matches[0].participant_a_id;
  statusResult = await service.setParticipantInactive(tournament.id, draftParticipant, {
    status: "withdrawn",
    reason: "Draft-round no-show",
  });
  assert.equal(statusResult.resolution.type, "cancel_draft_round");
  assert.equal(statusResult.resolution.round_id, overview.current_round.id);
});

test("withdrawal and published no-show use explicit technical results", async (t) => {
  const { service } = await createContext(t);
  const { tournament } = await createReadyTournament(service, 4, "technical");
  const round = await createPublishedRound(service, tournament.id, 1);
  const withdrawalMatch = round.matches[0];
  const withdrawnId = withdrawalMatch.participant_a_id;
  let statusResult = await service.setParticipantInactive(tournament.id, withdrawnId, {
    status: "withdrawn",
    reason: "Stopped during the match",
  });
  assert.equal(statusResult.resolution.type, "technical_result");
  assert.equal(statusResult.resolution.suggested_result.finish_reason, "withdrawal");
  let overview = await service.saveSwissMatchResult(
    tournament.id,
    withdrawalMatch.id,
    statusResult.resolution.suggested_result
  );
  assert.equal(
    overview.current_round.matches.find((match) => match.id === withdrawalMatch.id).finish_reason,
    "withdrawal"
  );

  const noShowMatch = round.matches[1];
  overview = await service.saveSwissMatchResult(tournament.id, noShowMatch.id, {
    starting_participant_id: noShowMatch.starting_participant_id,
    result_type: "technical",
    winner_participant_id: noShowMatch.participant_a_id,
    finish_reason: "no_show",
  });
  assert.equal(
    overview.current_round.matches.find((match) => match.id === noShowMatch.id).finish_reason,
    "no_show"
  );
});

test("automatically gives a late bye or replaces the existing first-round bye", async (t) => {
  const lateByeContext = await createContext(t);
  const lateByeTournament = await createReadyTournament(
    lateByeContext.service,
    4,
    "late-bye",
    2
  );
  let overview = await completeRound(lateByeContext.service, lateByeTournament.tournament.id, 1);
  assert.equal(overview.current_round.matches.filter((match) => match.is_bye).length, 0);
  const lateByePayload = {
    name_en: "Late Bye Player",
    association_id: "UKR",
    mode: "late_bye",
  };
  const lateByePreview = await lateByeContext.service.previewLateParticipant(
    lateByeTournament.tournament.id,
    lateByePayload
  );
  assert.equal(lateByePreview.reopens_completed_round, true);
  overview = await lateByeContext.service.confirmLateParticipant(
    lateByeTournament.tournament.id,
    { ...lateByePayload, expected_round_revision: lateByePreview.round.revision },
    { id: 1 }
  );
  assert.equal(overview.current_round.status, "published");
  assert.equal(overview.current_round.matches.filter((match) => match.is_bye).length, 1);
  assert.equal(overview.standings.revision, 0);
  overview = await lateByeContext.service.completeSwissRound(
    lateByeTournament.tournament.id,
    overview.current_round.id
  );
  assert.equal(overview.standings.rows.length, 5);
  assert.equal(overview.standings.rows.reduce((sum, row) => sum + row.bye_count, 0), 1);

  const pairedContext = await createContext(t);
  const pairedTournament = await createReadyTournament(
    pairedContext.service,
    5,
    "pair-with-bye",
    2
  );
  overview = await pairedContext.service.confirmSwissRound(
    pairedTournament.tournament.id,
    { round_number: 1 }
  );
  const originalBye = overview.current_round.matches.find((match) => match.is_bye);
  const pairPayload = {
    name_en: "Late Paired Player",
    association_id: "UKR",
    mode: "pair_with_bye",
    table_number: 9,
    starting_participant: "late_participant",
  };
  const pairPreview = await pairedContext.service.previewLateParticipant(
    pairedTournament.tournament.id,
    pairPayload
  );
  await assert.rejects(
    pairedContext.service.previewLateParticipant(pairedTournament.tournament.id, {
      ...pairPayload,
      mode: "late_bye",
    }),
    (error) => (
      error?.code === "LATE_ENTRY_MODE_REQUIRED"
      && error?.details?.required_mode === "pair_with_bye"
    )
  );
  assert.equal(pairPreview.bye_match.id, originalBye.id);
  overview = await pairedContext.service.confirmLateParticipant(
    pairedTournament.tournament.id,
    {
      ...pairPayload,
      bye_match_id: pairPreview.bye_match.id,
      expected_round_revision: pairPreview.round.revision,
    },
    { id: 1 }
  );
  assert.equal(overview.current_round.status, "draft");
  assert.equal(overview.current_round.matches.filter((match) => match.is_bye).length, 0);
  const lateMatch = overview.current_round.matches.find((match) => match.id === overview.match_id);
  assert.equal(lateMatch.table_number, 9);
  assert.equal(lateMatch.starting_participant_id, overview.participant.id);
  assert.equal(
    (await get(pairedContext.db, "SELECT status FROM in_person_matches WHERE id = ?", [originalBye.id])).status,
    "cancelled"
  );
});

test("Swiss cancellation and late entry roll back atomically after injected faults", async (t) => {
  let failAt = null;
  const { db, service } = await createContext(t, {
    faultInjector(point) {
      if (point === failAt) throw new Error(`injected ${point}`);
    },
  });
  const { tournament } = await createReadyTournament(service, 4, "atomic", 2);
  let overview = await completeRound(service, tournament.id, 1);
  const roundId = overview.current_round.id;

  failAt = "swiss_cancellation_after_matches";
  await assert.rejects(
    service.cancelSwissRound(tournament.id, roundId, { reason: "Fault" }),
    /injected swiss_cancellation_after_matches/
  );
  assert.equal((await get(db, "SELECT status FROM in_person_rounds WHERE id = ?", [roundId])).status, "completed");
  assert.equal((await get(db, "SELECT COUNT(*) AS count FROM in_person_matches WHERE round_id = ? AND status = 'cancelled'", [roundId])).count, 0);

  failAt = "late_participant_after_match";
  await assert.rejects(
    service.confirmLateParticipant(tournament.id, {
      name_en: "Atomic Late Player",
      association_id: "UKR",
      mode: "late_bye",
    }),
    /injected late_participant_after_match/
  );
  assert.equal((await get(db, "SELECT COUNT(*) AS count FROM in_person_participants WHERE name_en = 'Atomic Late Player'")).count, 0);
  assert.equal((await get(db, "SELECT status FROM in_person_rounds WHERE id = ?", [roundId])).status, "completed");
});
