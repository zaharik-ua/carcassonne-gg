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
    INSERT INTO associations (code, name, flag) VALUES
      ('UKR', 'Ukraine', 'ua'),
      ('POL', 'Poland', 'pl');
    INSERT INTO users (id, email, name, admin) VALUES
      (1, 'one@example.com', 'Organizer One', 0),
      (2, 'two@example.com', 'Organizer Two', 0),
      (3, 'admin@example.com', 'Global Admin', 1);
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
    slug: `cup-${String(overrides.slug_suffix || "main")}`,
    name_en: "In-Person Cup",
    scope: "international",
    start_date: "2026-10-10",
    end_date: "2026-10-10",
    organizer_name: "Organizer",
    swiss_rounds_count: 5,
    playoff_first_round: "semi_final",
    admin_user_ids: [1],
    ...overrides,
    slug_suffix: undefined,
  };
}

async function createPublishedTournament(service, overrides = {}) {
  const tournament = await service.createTournament(tournamentPayload(overrides));
  return service.publishTournament(tournament.id);
}

test("returns only assigned tournaments to an organizer and all active tournaments to a global admin", async (t) => {
  const { service } = await createDatabase(t);
  const first = await createPublishedTournament(service, { slug: "first", admin_user_ids: [1] });
  const second = await createPublishedTournament(service, { slug: "second", admin_user_ids: [2] });
  const cancelled = await createPublishedTournament(service, { slug: "cancelled", admin_user_ids: [1] });
  await service.cancelTournament(cancelled.id);

  assert.deepEqual(
    (await service.listAccessibleTournaments({ id: 1, admin: 0 })).map((item) => item.id),
    [first.id]
  );
  assert.deepEqual(
    new Set((await service.listAccessibleTournaments({ id: 3, admin: 1 })).map((item) => item.id)),
    new Set([first.id, second.id])
  );
  assert.deepEqual(await service.listAccessibleTournaments({ id: 999, admin: 0 }), []);
});

test("validates international and local participant locations", async (t) => {
  const { service } = await createDatabase(t);
  const international = await createPublishedTournament(service, { slug: "international" });
  const internationalPlayer = await service.createParticipant(international.id, {
    name_en: "Anna Player",
    association_id: "pol",
  });
  assert.equal(internationalPlayer.association_id, "POL");
  assert.equal(internationalPlayer.city_id, null);
  await assert.rejects(
    service.createParticipant(international.id, { name_en: "No Country" }),
    (error) => error?.code === "PARTICIPANT_ASSOCIATION_REQUIRED"
  );

  const kyiv = await service.createCity({ association_id: "UKR", name_en: "Kyiv" });
  const warsaw = await service.createCity({ association_id: "POL", name_en: "Warsaw" });
  const local = await createPublishedTournament(service, {
    slug: "local",
    scope: "local",
    association_id: "UKR",
    local_subtype: "final",
  });
  const localPlayer = await service.createParticipant(local.id, {
    name_en: "Local Player",
    city_id: kyiv.id,
  });
  assert.equal(localPlayer.city_id, kyiv.id);
  assert.equal(localPlayer.association_id, null);
  await assert.rejects(
    service.createParticipant(local.id, { name_en: "Wrong City", city_id: warsaw.id }),
    (error) => error?.code === "PARTICIPANT_CITY_ASSOCIATION_MISMATCH"
  );
});

test("warns about duplicate participants and supports an explicit namesake confirmation", async (t) => {
  const { service } = await createDatabase(t);
  const tournament = await createPublishedTournament(service, { slug: "duplicates" });
  const first = await service.createParticipant(tournament.id, {
    name_en: "Same Name",
    bga_nickname: "first-bga",
    association_id: "UKR",
  });

  await assert.rejects(
    service.createParticipant(tournament.id, {
      name_en: " same   name ",
      bga_nickname: "second-bga",
      association_id: "POL",
    }),
    (error) => (
      error?.status === 409
      && error?.code === "DUPLICATE_PARTICIPANT"
      && error?.details?.candidates?.[0]?.id === first.id
    )
  );
  const namesake = await service.createParticipant(tournament.id, {
    name_en: "Same Name",
    bga_nickname: "second-bga",
    association_id: "POL",
    confirm_duplicate: true,
  });
  assert.notEqual(namesake.id, first.id);

  await assert.rejects(
    service.createParticipant(tournament.id, {
      name_en: "Different Name",
      bga_nickname: " FIRST-BGA ",
      association_id: "UKR",
    }),
    (error) => error?.code === "DUPLICATE_PARTICIPANT"
  );
});

test("supports check-in with sparse draw numbers including a missing number one", async (t) => {
  const { service } = await createDatabase(t);
  const tournament = await createPublishedTournament(service, { slug: "check-in" });
  const participants = [];
  for (let index = 1; index <= 4; index += 1) {
    participants.push(await service.createParticipant(tournament.id, {
      name_en: `Player ${index}`,
      association_id: index % 2 ? "UKR" : "POL",
    }));
  }
  await service.startCheckIn(tournament.id);
  const drawNumbers = [2, 3, 5, 8];
  for (let index = 0; index < participants.length; index += 1) {
    await service.setParticipantCheckIn(tournament.id, participants[index].id, {
      checked_in: true,
      draw_number: drawNumbers[index],
    });
  }

  const overview = await service.getParticipantsOverview(tournament.id);
  assert.deepEqual(overview.counters, {
    total: 4,
    registered: 4,
    awaiting_check_in: 0,
    checked_in: 4,
    without_draw_number: 0,
    withdrawn_disqualified: 0,
  });
  assert.equal(overview.readiness.ready, true);
  assert.deepEqual(
    overview.participants.map((participant) => participant.draw_number),
    drawNumbers
  );
});

test("reports missing and duplicate draw numbers without requiring contiguous numbering", async (t) => {
  const { service } = await createDatabase(t);
  const tournament = await createPublishedTournament(service, { slug: "readiness" });
  const players = [];
  for (let index = 1; index <= 4; index += 1) {
    players.push(await service.createParticipant(tournament.id, {
      name_en: `Readiness ${index}`,
      association_id: "UKR",
    }));
  }
  await service.startCheckIn(tournament.id);
  await service.setParticipantCheckIn(tournament.id, players[0].id, { checked_in: true, draw_number: 2 });
  await service.setParticipantCheckIn(tournament.id, players[1].id, { checked_in: true, draw_number: 7 });
  await service.setParticipantCheckIn(tournament.id, players[2].id, { checked_in: true });
  await service.setParticipantCheckIn(tournament.id, players[3].id, { checked_in: true, draw_number: 9 });

  let overview = await service.getParticipantsOverview(tournament.id);
  assert.equal(overview.readiness.ready, false);
  assert.deepEqual(overview.readiness.missing_draw_numbers.map((player) => player.id), [players[2].id]);

  await assert.rejects(
    service.setParticipantCheckIn(tournament.id, players[2].id, { checked_in: true, draw_number: 7 }),
    (error) => error?.status === 409 && error?.code === "DRAW_NUMBER_TAKEN"
  );
  await service.setParticipantCheckIn(tournament.id, players[2].id, { checked_in: true, draw_number: 20 });
  overview = await service.getParticipantsOverview(tournament.id);
  assert.equal(overview.readiness.ready, true);

  const checkedOut = await service.setParticipantCheckIn(tournament.id, players[0].id, {
    checked_in: false,
    draw_number: 2,
  });
  assert.equal(checkedOut.status, "registered");
  assert.equal(checkedOut.draw_number, null);
});

test("keeps participant IDs stable when name or location changes after a match exists", async (t) => {
  const { db, service } = await createDatabase(t);
  const tournament = await createPublishedTournament(service, { slug: "stable-participant" });
  const first = await service.createParticipant(tournament.id, {
    name_en: "Before Name",
    association_id: "UKR",
  });
  const second = await service.createParticipant(tournament.id, {
    name_en: "Opponent",
    association_id: "POL",
  });
  await run(
    db,
    `INSERT INTO in_person_rounds (id, tournament_id, stage, round_number, status)
     VALUES ('round-one', ?, 'swiss', 1, 'published')`,
    [tournament.id]
  );
  await run(
    db,
    `INSERT INTO in_person_matches
      (id, round_id, participant_a_id, participant_b_id, starting_participant_id)
     VALUES ('match-one', 'round-one', ?, ?, ?)`,
    [first.id, second.id, first.id]
  );

  const updated = await service.updateParticipant(tournament.id, first.id, {
    name_en: "After Name",
    association_id: "POL",
  });
  assert.equal(updated.id, first.id);
  assert.equal(updated.name_en, "After Name");
  assert.equal(updated.association_id, "POL");
  await assert.rejects(
    service.deleteParticipant(tournament.id, first.id),
    (error) => error?.code === "PARTICIPANT_DELETE_LOCKED"
  );
});

test("locks check-in changes after the first Swiss round exists", async (t) => {
  const { db, service } = await createDatabase(t);
  const tournament = await createPublishedTournament(service, { slug: "locked-check-in" });
  const first = await service.createParticipant(tournament.id, { name_en: "One", association_id: "UKR" });
  await service.createParticipant(tournament.id, { name_en: "Two", association_id: "POL" });
  await service.startCheckIn(tournament.id);
  await run(
    db,
    `INSERT INTO in_person_rounds (id, tournament_id, stage, round_number, status)
     VALUES ('locked-round', ?, 'swiss', 1, 'draft')`,
    [tournament.id]
  );
  await assert.rejects(
    service.setParticipantCheckIn(tournament.id, first.id, { checked_in: true, draw_number: 2 }),
    (error) => error?.code === "CHECK_IN_LOCKED"
  );
});
