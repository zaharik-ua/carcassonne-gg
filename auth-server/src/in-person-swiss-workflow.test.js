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
    idFactory: () => `00000000-0000-4000-8000-${String(++sequence).padStart(12, "0")}`,
  });
  return { db, service };
}

async function createReadyTournament(service, participantCount, suffix) {
  const tournament = await service.createTournament({
    slug: `swiss-${suffix}`,
    name_en: `Swiss ${participantCount}`,
    scope: "international",
    start_date: "2026-10-10",
    end_date: "2026-10-10",
    organizer_name: "Organizer",
    swiss_rounds_count: 2,
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

async function saveRoundResults(service, tournamentId, round) {
  for (const match of round.matches.filter((entry) => !entry.is_bye)) {
    await service.saveSwissMatchResult(tournamentId, match.id, {
      starting_participant_id: match.starting_participant_id,
      result_type: "simple",
      winner_participant_id: match.participant_a_id,
      admin_note: `Table ${match.table_number}`,
    });
  }
}

test("runs the full two-round Swiss lifecycle for 4, 5 and 8 participants", async (t) => {
  const { service } = await createContext(t);
  for (const participantCount of [4, 5, 8]) {
    const { tournament } = await createReadyTournament(
      service,
      participantCount,
      String(participantCount)
    );
    const firstPreview = await service.previewSwissRound(tournament.id);
    assert.equal(firstPreview.round_number, 1);
    assert.equal(firstPreview.matches.length, Math.ceil(participantCount / 2));
    assert.equal(firstPreview.bye_participant_id !== null, participantCount % 2 === 1);

    let overview = await service.confirmSwissRound(tournament.id, { round_number: 1 });
    assert.equal(overview.created, true);
    const firstRoundId = overview.current_round.id;
    const retry = await service.confirmSwissRound(tournament.id, { round_number: 1 });
    assert.equal(retry.created, false);
    assert.equal(retry.current_round.id, firstRoundId);
    assert.equal(retry.current_round.matches.length, Math.ceil(participantCount / 2));

    await assert.rejects(
      service.previewSwissRound(tournament.id, { round_number: 2 }),
      (error) => error?.code === "STALE_ROUND_PREVIEW" || error?.code === "SWISS_ROUND_IN_PROGRESS"
    );
    overview = await service.publishSwissRound(tournament.id, firstRoundId);
    assert.equal(overview.current_round.status, "published");
    await saveRoundResults(service, tournament.id, overview.current_round);
    overview = await service.getSwissOverview(tournament.id);
    assert.equal(overview.progress.completed, overview.progress.total);
    overview = await service.completeSwissRound(tournament.id, firstRoundId);
    assert.equal(overview.standings.revision, 1);
    assert.equal(overview.standings.rows.length, participantCount);
    const completeRetry = await service.completeSwissRound(tournament.id, firstRoundId);
    assert.equal(completeRetry.completed, false);
    assert.equal(completeRetry.standings.revision, 1);

    const secondPreview = await service.previewSwissRound(tournament.id, { round_number: 2 });
    assert.equal(secondPreview.round_number, 2);
    overview = await service.confirmSwissRound(tournament.id, { round_number: 2 });
    await service.publishSwissRound(tournament.id, overview.current_round.id);
    overview = await service.getSwissOverview(tournament.id);
    await saveRoundResults(service, tournament.id, overview.current_round);
    overview = await service.completeSwissRound(tournament.id, overview.current_round.id);
    assert.equal(overview.swiss_complete, true);
    assert.equal(overview.standings.revision, 2);
    assert.equal(overview.next_round_number, null);
  }
});

test("confirms and publishes a Swiss preview in one retry-safe command", async (t) => {
  const { service } = await createContext(t);
  const { tournament } = await createReadyTournament(service, 4, "confirm-publish");
  await service.previewSwissRound(tournament.id, { round_number: 1 });
  let overview = await service.confirmSwissRound(tournament.id, {
    round_number: 1,
    publish: true,
  });
  assert.equal(overview.created, true);
  assert.equal(overview.published, true);
  assert.equal(overview.current_round.status, "published");
  const roundId = overview.current_round.id;

  overview = await service.confirmSwissRound(tournament.id, {
    round_number: 1,
    publish: true,
  });
  assert.equal(overview.created, false);
  assert.equal(overview.published, false);
  assert.equal(overview.current_round.id, roundId);
  assert.equal(overview.rounds.length, 1);
});

test("reopens the last completed round before the next round is formed", async (t) => {
  const { db, service } = await createContext(t);
  const { tournament } = await createReadyTournament(service, 4, "reopen-complete");
  let overview = await service.confirmSwissRound(tournament.id, {
    round_number: 1,
    publish: true,
  });
  const roundId = overview.current_round.id;
  await saveRoundResults(service, tournament.id, overview.current_round);
  overview = await service.completeSwissRound(tournament.id, roundId);
  assert.equal(overview.current_round.status, "completed");
  assert.equal(overview.can_reopen_current_round, true);
  assert.equal(overview.standings.revision, 1);

  overview = await service.reopenSwissRound(tournament.id, roundId);
  assert.equal(overview.reopened, true);
  assert.equal(overview.current_round.status, "published");
  assert.equal(overview.current_round.progress.completed, overview.current_round.progress.total);
  assert.equal(overview.can_generate_next_round, false);
  assert.equal(overview.can_reopen_current_round, false);
  assert.equal(overview.standings.revision, 0);
  assert.equal(
    (await get(db, "SELECT completed_at FROM in_person_rounds WHERE id = ?", [roundId])).completed_at,
    null
  );

  const retry = await service.reopenSwissRound(tournament.id, roundId);
  assert.equal(retry.reopened, false);
  const match = retry.current_round.matches.find((entry) => !entry.is_bye);
  const correctedWinner = match.winner_participant_id === match.participant_a_id
    ? match.participant_b_id
    : match.participant_a_id;
  await service.saveSwissMatchResult(tournament.id, match.id, {
    starting_participant_id: match.starting_participant_id,
    result_type: "simple",
    winner_participant_id: correctedWinner,
    admin_note: "Corrected after reopening",
  });
  overview = await service.completeSwissRound(tournament.id, roundId);
  assert.equal(overview.standings.revision, 2);

  overview = await service.confirmSwissRound(tournament.id, {
    round_number: 2,
    publish: true,
  });
  assert.equal(overview.current_round.round_number, 2);
  await assert.rejects(
    service.reopenSwissRound(tournament.id, roundId),
    (error) => error?.code === "NEXT_ROUND_ALREADY_FORMED"
  );
});

test("rejects invalid or incomplete results and reports missing tables", async (t) => {
  const { service } = await createContext(t);
  const { tournament } = await createReadyTournament(service, 4, "validation");
  let overview = await service.confirmSwissRound(tournament.id, { round_number: 1 });
  overview = await service.publishSwissRound(tournament.id, overview.current_round.id);
  const [firstMatch] = overview.current_round.matches;

  await assert.rejects(
    service.saveSwissMatchResult(tournament.id, firstMatch.id, {
      result_type: "technical",
      winner_participant_id: firstMatch.participant_a_id,
      finish_reason: "unknown",
    }),
    (error) => error?.status === 400 && error?.code === "INVALID_FINISH_REASON"
  );
  await assert.rejects(
    service.completeSwissRound(tournament.id, overview.current_round.id),
    (error) => (
      error?.status === 409
      && error?.code === "ROUND_RESULTS_INCOMPLETE"
      && error?.details?.missing_table_numbers?.length === 2
    )
  );
});

test("lost-response retries return current publish and result state without new revisions", async (t) => {
  const { service } = await createContext(t);
  const { tournament } = await createReadyTournament(service, 4, "retry-state");
  let overview = await service.confirmSwissRound(tournament.id, { round_number: 1 });
  const roundId = overview.current_round.id;
  overview = await service.publishSwissRound(tournament.id, roundId);
  assert.equal(overview.published, true);
  const publishRetry = await service.publishSwissRound(tournament.id, roundId);
  assert.equal(publishRetry.published, false);
  assert.equal(publishRetry.current_round.status, "published");

  const match = publishRetry.current_round.matches.find((entry) => !entry.is_bye);
  const payload = {
    starting_participant_id: match.starting_participant_id,
    result_type: "points",
    points_a: 101,
    points_b: 84,
    admin_note: "verified",
  };
  const saved = await service.saveSwissMatchResult(tournament.id, match.id, payload);
  assert.equal(saved.changed, true);
  const savedRevision = saved.match.revision;
  const saveRetry = await service.saveSwissMatchResult(tournament.id, match.id, payload);
  assert.equal(saveRetry.changed, false);
  assert.equal(saveRetry.match.revision, savedRevision);
  assert.equal(saveRetry.match.points_a, 101);
  assert.equal(saveRetry.match.points_b, 84);
});

test("rolls back every partial row when Swiss generation fails", async (t) => {
  let fail = true;
  const { db, service } = await createContext(t, {
    faultInjector(point, context) {
      if (fail && point === "swiss_round_after_match_insert" && context.match_index === 0) {
        throw new Error("injected generation failure");
      }
    },
  });
  const { tournament } = await createReadyTournament(service, 4, "generation-rollback");
  await assert.rejects(
    service.confirmSwissRound(tournament.id, { round_number: 1 }),
    /injected generation failure/
  );
  assert.equal((await get(db, "SELECT COUNT(*) AS count FROM in_person_rounds")).count, 0);
  assert.equal((await get(db, "SELECT COUNT(*) AS count FROM in_person_matches")).count, 0);
  assert.equal((await get(db, "SELECT status FROM in_person_tournaments WHERE id = ?", [tournament.id])).status, "check_in");

  fail = false;
  const overview = await service.confirmSwissRound(tournament.id, { round_number: 1 });
  assert.equal(overview.created, true);
  assert.equal(overview.current_round.matches.length, 2);
});

test("rolls back round completion and standings together after a fault", async (t) => {
  let failCompletion = false;
  const { db, service } = await createContext(t, {
    faultInjector(point, context) {
      if (
        failCompletion
        && point === "swiss_standings_after_insert"
        && context.standing_index === 0
      ) {
        throw new Error("injected standings failure");
      }
    },
  });
  const { tournament } = await createReadyTournament(service, 4, "completion-rollback");
  let overview = await service.confirmSwissRound(tournament.id, { round_number: 1 });
  overview = await service.publishSwissRound(tournament.id, overview.current_round.id);
  await saveRoundResults(service, tournament.id, overview.current_round);

  failCompletion = true;
  await assert.rejects(
    service.completeSwissRound(tournament.id, overview.current_round.id),
    /injected standings failure/
  );
  assert.equal((await get(db, "SELECT status FROM in_person_rounds LIMIT 1")).status, "published");
  assert.equal((await get(db, "SELECT COUNT(*) AS count FROM in_person_standings")).count, 0);

  failCompletion = false;
  overview = await service.completeSwissRound(tournament.id, overview.current_round.id);
  assert.equal(overview.current_round.status, "completed");
  assert.equal(overview.standings.rows.length, 4);
});
