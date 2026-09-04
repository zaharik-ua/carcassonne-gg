import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import { ensureInPersonSchema } from "./in-person/schema.js";
import { buildPlayoffBracket } from "./in-person/playoff.js";
import { createInPersonService } from "./in-person/service.js";

const silentLogger = { info() {} };

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
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
    idFactory: () => `70000000-0000-4000-8000-${String(++sequence).padStart(12, "0")}`,
  });
  return { db, service };
}

async function createSwissCompleteTournament(service, suffix, participantCount = 4) {
  const tournament = await service.createTournament({
    slug: `playoff-${suffix}`,
    name_en: `Playoff ${suffix}`,
    scope: "international",
    start_date: "2026-12-12",
    end_date: "2026-12-12",
    organizer_name: "Organizer",
    swiss_rounds_count: 1,
    playoff_first_round: participantCount === 4 ? "semi_final" : "quarter_final",
    admin_user_ids: [1],
  });
  await service.publishTournament(tournament.id);
  const participants = [];
  for (let index = 0; index < participantCount; index += 1) {
    participants.push(await service.createParticipant(tournament.id, {
      name_en: `Player ${index + 1}`,
      association_id: "UKR",
    }));
  }
  await service.startCheckIn(tournament.id);
  for (let index = 0; index < participants.length; index += 1) {
    await service.setParticipantCheckIn(tournament.id, participants[index].id, {
      checked_in: true,
      draw_number: index + 1,
    });
  }
  let swiss = await service.confirmSwissRound(tournament.id, { round_number: 1, publish: true });
  for (const match of swiss.current_round.matches.filter((entry) => !entry.is_bye)) {
    await service.saveSwissMatchResult(tournament.id, match.id, {
      starting_participant_id: match.starting_participant_id,
      result_type: "simple",
      winner_participant_id: match.participant_a_id,
    });
  }
  swiss = await service.completeSwissRound(tournament.id, swiss.current_round.id);
  assert.equal(swiss.swiss_complete, true);
  return { tournament, participants };
}

function simpleResult(match, winnerParticipantId, adminNote = null) {
  return {
    starting_participant_id: match.participant_a_id,
    result_type: "simple",
    winner_participant_id: winnerParticipantId,
    admin_note: adminNote,
  };
}

test("builds complete deterministic playoff structures for every supported first round", () => {
  const cases = [
    ["round_of_32", 32, [16, 8, 4, 2, 1, 1]],
    ["round_of_16", 16, [8, 4, 2, 1, 1]],
    ["quarter_final", 8, [4, 2, 1, 1]],
    ["semi_final", 4, [2, 1, 1]],
  ];
  cases.forEach(([firstRound, participantCount, expectedMatchCounts]) => {
    const participantIds = Array.from({ length: participantCount }, (_, index) => `p${index + 1}`);
    const bracket = buildPlayoffBracket({
      first_round: firstRound,
      participant_ids: participantIds,
    });
    assert.equal(bracket.participant_count, participantCount);
    assert.deepEqual(bracket.rounds.map((round) => round.matches.length), expectedMatchCounts);
    assert.deepEqual(
      bracket.rounds.map((round) => round.round_key).slice(-2),
      ["bronze_medal_match", "final"]
    );
    const semifinals = bracket.rounds.find((round) => round.round_key === "semi_final");
    assert.deepEqual(
      semifinals.matches.map((match) => match.next_match_for_loser_key),
      ["bronze_medal_match:1", "bronze_medal_match:1"]
    );
    bracket.rounds.forEach((round) => {
      assert.equal(round.matches.filter((match) => match.table_number === 1).length, 1);
    });
  });
});

test("rejects missing and duplicate manual first-round slots", () => {
  assert.throws(
    () => buildPlayoffBracket({
      first_round: "semi_final",
      participant_ids: ["p1", "p2", "p3"],
    }),
    (error) => error?.code === "PLAYOFF_SLOTS_INCOMPLETE"
  );
  assert.throws(
    () => buildPlayoffBracket({
      first_round: "semi_final",
      participant_ids: ["p1", "p2", "p1", "p4"],
    }),
    (error) => (
      error?.code === "DUPLICATE_PLAYOFF_PARTICIPANT"
      && error?.details?.duplicates?.[0]?.slots?.join(",") === "1,3"
    )
  );
});

test("runs the playoff, swaps streaming table, propagates corrections and requires both medal matches", async (t) => {
  const { db, service } = await createContext(t);
  const { tournament, participants } = await createSwissCompleteTournament(service, "lifecycle");
  const participantIds = participants.map((participant) => participant.id);
  const preview = await service.previewPlayoff(tournament.id, { participant_ids: participantIds });
  assert.equal(preview.first_round, "semi_final");
  assert.equal(preview.rounds.length, 3);
  assert.equal(preview.rounds[0].matches[0].participant_a_name_en, "Player 1");

  let overview = await service.confirmPlayoff(tournament.id, {
    participant_ids: participantIds,
    expected_tournament_revision: preview.tournament_revision,
    expected_standings_revision: preview.standings_revision,
  });
  assert.equal(overview.created, true);
  assert.equal(overview.tournament.status, "playoff");
  let semifinals = overview.rounds.find((round) => round.round_key === "semi_final");
  assert.equal(semifinals.status, "published");
  assert.equal(overview.rounds.find((round) => round.round_key === "final").status, "draft");

  const [semiOne, semiTwo] = semifinals.matches;
  overview = await service.setPlayoffMatchTable(tournament.id, semiTwo.id, { table_number: 1 });
  semifinals = overview.rounds.find((round) => round.round_key === "semi_final");
  assert.equal(semifinals.matches.find((match) => match.id === semiTwo.id).table_number, 1);
  assert.equal(semifinals.matches.find((match) => match.id === semiOne.id).table_number, 2);
  const storedTables = await all(
    db,
    "SELECT table_number FROM in_person_matches WHERE round_id = ? ORDER BY table_number",
    [semifinals.id]
  );
  assert.deepEqual(storedTables.map((row) => row.table_number), [1, 2]);

  await service.savePlayoffMatchResult(
    tournament.id,
    semiOne.id,
    simpleResult(semiOne, semiOne.participant_a_id)
  );
  overview = await service.savePlayoffMatchResult(
    tournament.id,
    semiTwo.id,
    simpleResult(semiTwo, semiTwo.participant_a_id)
  );
  assert.equal(
    overview.rounds.find((round) => round.round_key === "semi_final").status,
    "completed"
  );
  let finalMatch = overview.rounds.find((round) => round.round_key === "final").matches[0];
  let bronzeMatch = overview.rounds.find((round) => round.round_key === "bronze_medal_match").matches[0];
  assert.deepEqual(
    [finalMatch.participant_a_id, finalMatch.participant_b_id],
    [semiOne.participant_a_id, semiTwo.participant_a_id]
  );
  assert.deepEqual(
    [bronzeMatch.participant_a_id, bronzeMatch.participant_b_id],
    [semiOne.participant_b_id, semiTwo.participant_b_id]
  );

  overview = await service.savePlayoffMatchResult(
    tournament.id,
    semiOne.id,
    simpleResult(semiOne, semiOne.participant_b_id, "Corrected winner")
  );
  finalMatch = overview.rounds.find((round) => round.round_key === "final").matches[0];
  bronzeMatch = overview.rounds.find((round) => round.round_key === "bronze_medal_match").matches[0];
  assert.equal(finalMatch.participant_a_id, semiOne.participant_b_id);
  assert.equal(bronzeMatch.participant_a_id, semiOne.participant_a_id);

  const finalRound = overview.rounds.find((round) => round.round_key === "final");
  overview = await service.publishPlayoffRound(tournament.id, finalRound.id);
  finalMatch = overview.rounds.find((round) => round.round_key === "final").matches[0];
  await service.savePlayoffMatchResult(
    tournament.id,
    finalMatch.id,
    simpleResult(finalMatch, finalMatch.participant_a_id)
  );
  await assert.rejects(
    service.savePlayoffMatchResult(
      tournament.id,
      semiOne.id,
      simpleResult(semiOne, semiOne.participant_a_id, "Too late")
    ),
    (error) => (
      error?.status === 409
      && error?.code === "PLAYOFF_DESCENDANT_PLAYED"
      && error?.details?.descendants?.some((descendant) => descendant.round_key === "final")
    )
  );
  await assert.rejects(
    service.completePlayoff(tournament.id),
    (error) => (
      error?.code === "PLAYOFF_MEDAL_MATCHES_INCOMPLETE"
      && error?.details?.missing_round_keys?.includes("bronze_medal_match")
    )
  );

  const bronzeRound = (await service.getPlayoffOverview(tournament.id)).rounds
    .find((round) => round.round_key === "bronze_medal_match");
  overview = await service.publishPlayoffRound(tournament.id, bronzeRound.id);
  bronzeMatch = overview.rounds.find((round) => round.round_key === "bronze_medal_match").matches[0];
  overview = await service.savePlayoffMatchResult(tournament.id, bronzeMatch.id, {
    starting_participant_id: bronzeMatch.participant_a_id,
    result_type: "technical",
    winner_participant_id: bronzeMatch.participant_b_id,
    finish_reason: "no_show",
    admin_note: "Bronze participant did not appear",
  });
  assert.equal(overview.can_complete, true);
  overview = await service.completePlayoff(tournament.id);
  assert.equal(overview.completed, true);
  assert.equal(overview.tournament.status, "completed");
  assert.equal(overview.placements.first, finalMatch.participant_a_id);
  assert.equal(overview.placements.third, bronzeMatch.participant_b_id);
});

test("rejects inactive participants and rolls back partial winner propagation", async (t) => {
  let failPropagation = false;
  let failTableSwap = false;
  const { db, service } = await createContext(t, {
    faultInjector(point) {
      if (failPropagation && point === "playoff_result_after_match") {
        throw new Error("injected playoff propagation failure");
      }
      if (failTableSwap && point === "playoff_after_table_swap") {
        throw new Error("injected playoff table failure");
      }
    },
  });
  const { tournament, participants } = await createSwissCompleteTournament(service, "rollback");
  await service.setParticipantInactive(tournament.id, participants[3].id, {
    status: "withdrawn",
    reason: "Cannot enter the playoff",
  });
  await assert.rejects(
    service.previewPlayoff(tournament.id, {
      participant_ids: participants.map((participant) => participant.id),
    }),
    (error) => error?.code === "INACTIVE_PLAYOFF_PARTICIPANT"
  );

  // Use a fresh valid tournament to isolate transaction rollback from roster validation.
  const valid = await createSwissCompleteTournament(service, "fault");
  let overview = await service.confirmPlayoff(valid.tournament.id, {
    participant_ids: valid.participants.map((participant) => participant.id),
  });
  const semifinalRound = overview.rounds.find((round) => round.round_key === "semi_final");
  const [semifinal, secondSemifinal] = semifinalRound.matches;
  failTableSwap = true;
  await assert.rejects(
    service.setPlayoffMatchTable(valid.tournament.id, secondSemifinal.id, { table_number: 1 }),
    /injected playoff table failure/
  );
  const tablesAfterRollback = await all(
    db,
    "SELECT id, table_number FROM in_person_matches WHERE round_id = ? ORDER BY bracket_position",
    [semifinalRound.id]
  );
  assert.deepEqual(tablesAfterRollback.map((row) => row.table_number), [1, 2]);
  failTableSwap = false;
  failPropagation = true;
  await assert.rejects(
    service.savePlayoffMatchResult(
      valid.tournament.id,
      semifinal.id,
      simpleResult(semifinal, semifinal.participant_a_id)
    ),
    /injected playoff propagation failure/
  );
  const rows = await all(
    db,
    `SELECT m.status, m.participant_a_id, m.participant_b_id, r.round_key
     FROM in_person_matches m JOIN in_person_rounds r ON r.id = m.round_id
     WHERE r.tournament_id = ? AND (m.id = ? OR r.round_key = 'final')
     ORDER BY r.round_key`,
    [valid.tournament.id, semifinal.id]
  );
  assert.equal(rows.find((row) => row.round_key === "semi_final").status, "scheduled");
  assert.equal(rows.find((row) => row.round_key === "final").participant_a_id, null);
});
