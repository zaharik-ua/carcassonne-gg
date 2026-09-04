import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import { ensureInPersonSchema } from "./in-person/schema.js";
import { serializePublicTournament } from "./in-person/public.js";
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
    idFactory: () => `80000000-0000-4000-8000-${String(++sequence).padStart(12, "0")}`,
  });
  return { db, service };
}

async function createTournament(service, suffix, { publish = true } = {}) {
  const tournament = await service.createTournament({
    slug: `public-${suffix}`,
    name_en: `Public ${suffix}`,
    name_local: `Публічний ${suffix}`,
    scope: "international",
    start_date: "2026-12-21",
    end_date: "2026-12-22",
    organizer_name: "Public organizer",
    organizer_url: "https://example.com/organizer",
    rules_url: "https://example.com/rules",
    swiss_rounds_count: 1,
    playoff_first_round: "semi_final",
    admin_user_ids: [1],
  });
  if (publish) await service.publishTournament(tournament.id);
  return tournament;
}

async function addCheckedInParticipants(service, tournamentId) {
  const participants = [];
  for (let index = 0; index < 4; index += 1) {
    participants.push(await service.createParticipant(tournamentId, {
      name_en: `Player ${index + 1}`,
      name_local: `Гравець ${index + 1}`,
      bga_nickname: `bga-${index + 1}`,
      association_id: "UKR",
    }));
  }
  await service.startCheckIn(tournamentId);
  for (let index = 0; index < participants.length; index += 1) {
    await service.setParticipantCheckIn(tournamentId, participants[index].id, {
      checked_in: true,
      draw_number: index + 1,
    });
  }
  return participants;
}

async function completeSwiss(service, tournamentId) {
  let overview = await service.confirmSwissRound(tournamentId, {
    round_number: 1,
    publish: true,
  });
  for (const match of overview.current_round.matches) {
    await service.saveSwissMatchResult(tournamentId, match.id, {
      starting_participant_id: match.starting_participant_id,
      result_type: "points",
      points_a: 90,
      points_b: 70,
      admin_note: "private judge note",
    });
  }
  overview = await service.completeSwissRound(tournamentId, overview.current_round.id);
  return overview;
}

test("public serializers use an allowlist and exclude admin-only tournament fields", () => {
  const serialized = serializePublicTournament({
    id: "ipt_public",
    slug: "public",
    name_en: "Public",
    scope: "international",
    start_date: "2026-01-01",
    end_date: "2026-01-01",
    organizer_name: "Organizer",
    swiss_rounds_count: 1,
    playoff_first_round: "semi_final",
    status: "registration",
    published_at: "2026-01-01 10:00:00",
    admin_user_ids: [7],
    admins: [{ user_id: 7, email: "private@example.com" }],
    cancellation_reason: "private",
  });
  const json = JSON.stringify(serialized);
  assert.doesNotMatch(json, /admin_user_ids|admins|private@example|cancellation_reason/);
});

test("public aggregate hides drafts, cancelled history and private result fields", async (t) => {
  const { service } = await createContext(t);
  const draft = await createTournament(service, "draft", { publish: false });
  await assert.rejects(
    service.getPublicTournamentAggregate(draft.id),
    (error) => error?.status === 404 && error?.code === "PUBLIC_TOURNAMENT_NOT_FOUND"
  );

  const tournament = await createTournament(service, "aggregate");
  const participants = await addCheckedInParticipants(service, tournament.id);
  let swiss = await service.confirmSwissRound(tournament.id, { round_number: 1 });
  let aggregate = await service.getPublicTournamentAggregate(tournament.id);
  assert.equal(aggregate.swiss.rounds.length, 0, "draft Swiss pairings stay private");
  const draftRevision = aggregate.revision;

  swiss = await service.publishSwissRound(tournament.id, swiss.current_round.id);
  const publishedAggregate = await service.getPublicTournamentAggregate(tournament.id);
  assert.ok(publishedAggregate.revision > draftRevision);
  const firstMatch = swiss.current_round.matches[0];
  await service.saveSwissMatchResult(tournament.id, firstMatch.id, {
    starting_participant_id: firstMatch.starting_participant_id,
    result_type: "technical",
    winner_participant_id: firstMatch.participant_a_id,
    finish_reason: "admin_decision",
    admin_note: "private judge note",
  });
  aggregate = await service.getPublicTournamentAggregate("public-aggregate");
  assert.ok(aggregate.revision > publishedAggregate.revision);
  assert.equal(aggregate.swiss.rounds.length, 1);
  assert.equal(aggregate.swiss.rounds[0].matches[0].finish_reason, "admin_decision");
  assert.doesNotMatch(JSON.stringify(aggregate), /admin_note|private judge note|admin_user_ids/);

  for (const match of swiss.current_round.matches.slice(1)) {
    await service.saveSwissMatchResult(tournament.id, match.id, {
      starting_participant_id: match.starting_participant_id,
      result_type: "simple",
      winner_participant_id: match.participant_a_id,
    });
  }
  await service.completeSwissRound(tournament.id, swiss.current_round.id);
  const beforePlayoff = await service.getPublicTournamentAggregate(tournament.id);
  assert.ok(beforePlayoff.revision > aggregate.revision);
  assert.equal(beforePlayoff.swiss.standings.rows.length, 4);
  assert.equal("sonneborn_berger" in beforePlayoff.swiss.standings.rows[0], false);
  assert.equal("bye_count" in beforePlayoff.swiss.standings.rows[0], false);

  await service.confirmPlayoff(tournament.id, {
    participant_ids: participants.map((participant) => participant.id),
  });
  aggregate = await service.getPublicTournamentAggregate(tournament.id);
  assert.deepEqual(
    aggregate.playoff.rounds.map((round) => round.round_key),
    ["semi_final"],
    "future draft playoff rounds stay private"
  );
  assert.equal(aggregate.players[0].name_local?.startsWith("Гравець"), true);
  assert.equal(aggregate.tournament.organizer_url, "https://example.com/organizer");
});

test("public reads follow active revisions after rollback and exclude cancelled tournaments", async (t) => {
  const { service } = await createContext(t);
  const tournament = await createTournament(service, "rollback");
  await addCheckedInParticipants(service, tournament.id);
  const completed = await completeSwiss(service, tournament.id);
  const completedAggregate = await service.getPublicTournamentAggregate(tournament.id);
  assert.equal(completedAggregate.swiss.rounds.length, 1);
  assert.equal(completedAggregate.swiss.standings.revision, 1);

  await service.cancelSwissRound(
    tournament.id,
    completed.current_round.id,
    { reason: "Public rollback test" }
  );
  const rolledBack = await service.getPublicTournamentAggregate(tournament.id);
  assert.ok(rolledBack.revision > completedAggregate.revision);
  assert.equal(rolledBack.swiss.rounds.length, 0);
  assert.equal(rolledBack.swiss.standings.revision, 0);

  const regenerated = await service.confirmSwissRound(tournament.id, {
    round_number: 1,
    publish: true,
  });
  const regeneratedAggregate = await service.getPublicTournamentAggregate(tournament.id);
  assert.equal(regeneratedAggregate.swiss.rounds.length, 1);
  assert.equal(regeneratedAggregate.swiss.rounds[0].id, regenerated.current_round.id);
  assert.notEqual(regeneratedAggregate.swiss.rounds[0].id, completed.current_round.id);

  const cancelled = await createTournament(service, "cancelled");
  await service.cancelTournament(cancelled.id);
  await assert.rejects(
    service.getPublicTournamentAggregate(cancelled.id),
    (error) => error?.status === 404 && error?.code === "PUBLIC_TOURNAMENT_NOT_FOUND"
  );
  const publicIds = (await service.listPublicTournaments()).map((entry) => entry.id);
  assert.equal(publicIds.includes(cancelled.id), false);
});
