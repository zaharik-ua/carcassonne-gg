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

async function createContext(t) {
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
    idFactory: () => `00000000-0000-4000-8000-${String(++sequence).padStart(12, "0")}`,
  });
  return { db, service };
}

/** Check-in without draw numbers: imported pairings do not need them. */
async function createCheckedInTournament(service, participantCount, suffix) {
  const tournament = await service.createTournament({
    slug: `swiss-import-${suffix}`,
    name_en: `Swiss import ${suffix}`,
    scope: "international",
    start_date: "2026-10-10",
    end_date: "2026-10-10",
    organizer_name: "Organizer",
    swiss_rounds_count: 3,
    playoff_first_round: "semi_final",
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
  for (const participant of participants) {
    await service.setParticipantCheckIn(tournament.id, participant.id, { checked_in: true });
  }
  return { tournament, participants };
}

test("imports a first round with results, a bye and the first player as starter", async (t) => {
  const { service } = await createContext(t);
  const { tournament, participants: [p1, p2, p3, p4, p5] } = await createCheckedInTournament(
    service,
    5,
    "first"
  );
  const payload = {
    round_number: 1,
    matches: [
      {
        table_number: 4,
        participant_a_id: p1.id,
        participant_b_id: p2.id,
        result: { result_type: "points", points_a: 88, points_b: 88 },
      },
      {
        participant_a_id: p3.id,
        participant_b_id: p4.id,
        result: { result_type: "simple", winner_participant_id: p3.id },
      },
      { participant_a_id: p5.id, is_bye: true },
    ],
  };

  const preview = await service.previewImportedSwissRound(tournament.id, payload);
  assert.equal(preview.imported, true);
  assert.equal(preview.round_number, 1);
  assert.equal(preview.results_count, 2);
  assert.deepEqual(preview.unpaired_participant_ids, []);
  assert.deepEqual(preview.matches.map((match) => match.table_number), [4, 1, null]);
  const tie = preview.matches[0];
  assert.equal(tie.starting_participant_id, p1.id);
  assert.equal(tie.winner_participant_id, p2.id, "the second player wins a tied score");
  assert.equal(tie.participant_a_name_en, p1.name_en);
  assert.equal(preview.bye_participant_id, p5.id);

  const overview = await service.confirmImportedSwissRound(tournament.id, {
    ...payload,
    complete_round: true,
  });
  assert.equal(overview.created, true);
  assert.equal(overview.completed, true);
  assert.equal(overview.tournament.status, "swiss");
  assert.equal(overview.current_round.status, "completed");
  assert.equal(overview.current_round.matches.length, 3);
  assert.equal(overview.standings.rows.length, 5);
  assert.equal(overview.can_generate_next_round, true);
  const winners = new Set(
    overview.standings.rows.filter((row) => row.wins === 1).map((row) => row.participant_id)
  );
  assert.deepEqual(winners, new Set([p2.id, p3.id, p5.id]));
});

test("rejects imported pairings with duplicate, unknown or inactive players", async (t) => {
  const { service } = await createContext(t);
  const { tournament, participants: [p1, p2, p3, p4] } = await createCheckedInTournament(
    service,
    4,
    "invalid"
  );
  await assert.rejects(
    service.previewImportedSwissRound(tournament.id, { matches: [] }),
    (error) => error?.code === "MATCHES_REQUIRED"
  );
  await assert.rejects(
    service.previewImportedSwissRound(tournament.id, {
      matches: [
        { participant_a_id: p1.id, participant_b_id: p2.id },
        { participant_a_id: p3.id, participant_b_id: p1.id },
      ],
    }),
    (error) => error?.code === "DUPLICATE_PAIRED_PARTICIPANT"
      && error.details.row_number === 2
      && error.details.duplicate_row_number === 1
  );
  await assert.rejects(
    service.previewImportedSwissRound(tournament.id, {
      matches: [{ participant_a_id: p1.id, participant_b_id: "ipp_missing" }],
    }),
    (error) => error?.code === "PARTICIPANT_NOT_FOUND"
  );
  await assert.rejects(
    service.previewImportedSwissRound(tournament.id, {
      matches: [
        { table_number: 2, participant_a_id: p1.id, participant_b_id: p2.id },
        { table_number: 2, participant_a_id: p3.id, participant_b_id: p4.id },
      ],
    }),
    (error) => error?.code === "DUPLICATE_TABLE_NUMBER"
  );
  await assert.rejects(
    service.previewImportedSwissRound(tournament.id, {
      matches: [{
        participant_a_id: p1.id,
        participant_b_id: p2.id,
        result: { result_type: "points", points_a: 70 },
      }],
    }),
    (error) => error?.code === "POINTS_REQUIRED" && error.details.row_number === 1
  );
  await assert.rejects(
    service.confirmImportedSwissRound(tournament.id, {
      complete_round: true,
      matches: [
        {
          participant_a_id: p1.id,
          participant_b_id: p2.id,
          result: { result_type: "simple", winner_participant_id: p1.id },
        },
        { participant_a_id: p3.id, participant_b_id: p4.id },
      ],
    }),
    (error) => error?.code === "ROUND_RESULTS_INCOMPLETE"
  );
  const untouched = await service.getSwissOverview(tournament.id);
  assert.equal(untouched.rounds.length, 0, "a rejected import must not create a round");

  await service.setParticipantCheckIn(tournament.id, p4.id, { checked_in: false });
  await assert.rejects(
    service.previewImportedSwissRound(tournament.id, {
      matches: [{ participant_a_id: p3.id, participant_b_id: p4.id }],
    }),
    (error) => error?.code === "PARTICIPANT_NOT_ACTIVE"
  );
});

test("imports a later round with rematch warnings, then imports its results in bulk", async (t) => {
  const { service } = await createContext(t);
  const { tournament, participants: [p1, p2, p3, p4] } = await createCheckedInTournament(
    service,
    4,
    "later"
  );
  await service.confirmImportedSwissRound(tournament.id, {
    complete_round: true,
    matches: [
      {
        participant_a_id: p1.id,
        participant_b_id: p2.id,
        result: { result_type: "points", points_a: 101, points_b: 93 },
      },
      {
        participant_a_id: p3.id,
        participant_b_id: p4.id,
        result: { result_type: "simple", winner_participant_id: p4.id },
      },
    ],
  });

  const secondRound = {
    round_number: 2,
    matches: [
      { participant_a_id: p2.id, participant_b_id: p1.id },
      { participant_a_id: p3.id, participant_b_id: p4.id },
    ],
  };
  const preview = await service.previewImportedSwissRound(tournament.id, secondRound);
  assert.deepEqual(preview.matches.map((match) => match.warnings), [["rematch"], ["rematch"]]);
  assert.equal(preview.results_count, 0);

  let overview = await service.confirmImportedSwissRound(tournament.id, secondRound);
  assert.equal(overview.current_round.round_number, 2);
  assert.equal(overview.current_round.status, "published");
  assert.equal(overview.completed, false);
  const [tableOne, tableTwo] = overview.current_round.matches;
  assert.equal(tableOne.starting_participant_id, p2.id);

  await assert.rejects(
    service.importSwissRoundResults(tournament.id, overview.current_round.id, {
      results: [
        { match_id: tableOne.id, result_type: "points", points_a: 70, points_b: 75 },
        { match_id: tableOne.id, result_type: "points", points_a: 70, points_b: 75 },
      ],
    }),
    (error) => error?.code === "DUPLICATE_MATCH_RESULT"
  );
  await assert.rejects(
    service.importSwissRoundResults(tournament.id, overview.current_round.id, {
      complete_round: true,
      results: [{ match_id: tableOne.id, result_type: "points", points_a: 70, points_b: 75 }],
    }),
    (error) => error?.code === "ROUND_RESULTS_INCOMPLETE"
  );
  overview = await service.getSwissOverview(tournament.id);
  assert.equal(overview.current_round.progress.completed, 0, "a failed completion rolls back results");

  overview = await service.importSwissRoundResults(tournament.id, overview.current_round.id, {
    results: [{ match_id: tableOne.id, result_type: "points", points_a: 70, points_b: 75 }],
  });
  assert.equal(overview.updated_count, 1);
  assert.equal(overview.completed, false);
  overview = await service.importSwissRoundResults(tournament.id, overview.current_round.id, {
    complete_round: true,
    results: [
      { match_id: tableOne.id, result_type: "points", points_a: 70, points_b: 75 },
      { match_id: tableTwo.id, result_type: "technical", finish_reason: "no_show", winner_participant_id: p3.id },
    ],
  });
  assert.equal(overview.updated_count, 1, "unchanged results are skipped");
  assert.equal(overview.completed, true);
  assert.equal(overview.current_round.status, "completed");
  const completedTableOne = overview.current_round.matches.find((match) => match.id === tableOne.id);
  assert.equal(completedTableOne.winner_participant_id, p1.id);
  assert.equal(overview.standings.revision, 2);

  await assert.rejects(
    service.importSwissRoundResults(tournament.id, overview.current_round.id, {
      results: [{ match_id: tableOne.id, result_type: "points", points_a: 75, points_b: 70 }],
    }),
    (error) => error?.code === "INVALID_ROUND_STATUS"
  );
});

test("reports checked-in players missing from an imported round", async (t) => {
  const { service } = await createContext(t);
  const { tournament, participants: [p1, p2, p3] } = await createCheckedInTournament(
    service,
    3,
    "unpaired"
  );
  const preview = await service.previewImportedSwissRound(tournament.id, {
    matches: [{ participant_a_id: p1.id, participant_b_id: p2.id }],
  });
  assert.deepEqual(preview.unpaired_participant_ids, [p3.id]);
  assert.deepEqual(
    preview.warnings.find((warning) => warning.code === "not_paired")?.participant_ids,
    [p3.id]
  );
});
