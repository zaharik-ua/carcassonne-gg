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
    CREATE TABLE profiles (id TEXT PRIMARY KEY, bga_nickname TEXT, gg_elo REAL, gg_rating_position INTEGER, bga_elo INTEGER);
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

test("stores and updates participant rating, description and photo fields", async (t) => {
  const { service } = await createDatabase(t);
  const tournament = await createPublishedTournament(service, { slug: "participant-profile" });
  const created = await service.createParticipant(tournament.id, {
    name_en: "Profile Player",
    bga_id: "123456789",
    gg_elo: 1650,
    gg_rank: 42,
    association_id: "UKR",
    current_elo: 1450,
    max_elo: 1512,
    player_information: "International championship participant.",
    player_photo: "https://example.com/player.webp",
  });
  assert.deepEqual(
    {
      bga_id: created.bga_id,
      gg_elo: created.gg_elo,
      gg_rank: created.gg_rank,
      current_elo: created.current_elo,
      max_elo: created.max_elo,
      player_information: created.player_information,
      player_photo: created.player_photo,
    },
    {
      bga_id: "123456789",
      gg_elo: 1650,
      gg_rank: 42,
      current_elo: 1450,
      max_elo: 1512,
      player_information: "International championship participant.",
      player_photo: "https://example.com/player.webp",
    }
  );

  const updated = await service.updateParticipant(tournament.id, created.id, {
    bga_id: "987654321",
    gg_elo: 1675,
    gg_rank: 38,
    current_elo: 1460,
    player_information: "Updated description.",
  });
  assert.equal(updated.bga_id, "987654321");
  assert.equal(updated.gg_elo, 1675);
  assert.equal(updated.gg_rank, 38);
  assert.equal(updated.current_elo, 1460);
  assert.equal(updated.max_elo, 1512);
  assert.equal(updated.player_information, "Updated description.");
  assert.equal(updated.player_photo, "https://example.com/player.webp");

  for (const [field, value] of [["bga_id", "abc"], ["bga_id", "0"], ["gg_elo", -1], ["gg_rank", 0], ["gg_rank", 1.5]]) {
    await assert.rejects(
      service.updateParticipant(tournament.id, created.id, { [field]: value }),
      (error) => error?.details?.field === field
    );
  }
  const cleared = await service.updateParticipant(tournament.id, created.id, {
    bga_id: null,
    gg_elo: null,
    gg_rank: null,
  });
  assert.equal(cleared.bga_id, null);
  assert.equal(cleared.gg_elo, null);
  assert.equal(cleared.gg_rank, null);

  await assert.rejects(
    service.updateParticipant(tournament.id, created.id, { current_elo: 1450.5 }),
    (error) => error?.code === "INVALID_NON_NEGATIVE_INTEGER"
      && error?.details?.field === "current_elo"
  );
  await assert.rejects(
    service.updateParticipant(tournament.id, created.id, { player_photo: "javascript:alert(1)" }),
    (error) => error?.code === "INVALID_URL" && error?.details?.field === "player_photo"
  );
});

test("creates a participant city in the local tournament association", async (t) => {
  const { service } = await createDatabase(t);
  const local = await createPublishedTournament(service, {
    slug: "local-city-create",
    scope: "local",
    association_id: "UKR",
    local_subtype: "final",
  });
  const city = await service.createParticipantCity(local.id, {
    association_id: "POL",
    name_en: "Lviv",
    name_local: "Львів",
    icon_url: "https://example.com/lviv.svg",
  });
  assert.equal(city.association_id, "UKR", "client association must be ignored");
  assert.equal(city.icon_url, "https://example.com/lviv.svg");
  assert.deepEqual((await service.listParticipantCities(local.id)).map((item) => item.id), [city.id]);

  const international = await createPublishedTournament(service, { slug: "no-local-cities" });
  await assert.rejects(
    service.createParticipantCity(international.id, { name_en: "Nowhere" }),
    (error) => error?.code === "CITY_CREATION_LOCAL_ONLY"
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

test("bulk registration creates players together and allows the same name in different countries", async (t) => {
  const { service } = await createDatabase(t);
  const tournament = await createPublishedTournament(service, { slug: "bulk-create" });
  await service.createParticipant(tournament.id, { name_en: "Same Name", association_id: "UKR" });
  const before = await service.getParticipantsOverview(tournament.id);

  const result = await service.createParticipantsBulk(tournament.id, {
    participants: [
      { name_en: " Same Name ", association_id: "pol", bga_nickname: " bga-player " },
      { name_en: "New Player", association_id: "UKR" },
    ],
  });
  assert.equal(result.created_count, 2);
  assert.deepEqual(result.participants.map((player) => [
    player.name_en, player.association_id, player.bga_nickname, player.status,
  ]), [
    ["Same Name", "POL", "bga-player", "registered"],
    ["New Player", "UKR", null, "registered"],
  ]);
  assert.notEqual(result.participants[0].id, result.participants[1].id);
  const after = await service.getParticipantsOverview(tournament.id);
  assert.equal(after.counters.total, 3);
  assert.equal(after.tournament.revision, before.tournament.revision + 1);
});

test("bulk registration rejects normalized name and country duplicates within the batch and tournament", async (t) => {
  const { service } = await createDatabase(t);
  const tournament = await createPublishedTournament(service, { slug: "bulk-duplicates" });
  const existing = await service.createParticipant(tournament.id, {
    name_en: "Existing Player", association_id: "UKR",
  });
  const before = await service.getParticipantsOverview(tournament.id);

  await assert.rejects(service.createParticipantsBulk(tournament.id, {
    participants: [
      { name_en: "Same Name", association_id: "POL", bga_nickname: "one" },
      { name_en: " same   name ", association_id: "pol", bga_nickname: "two", confirm_duplicate: true },
    ],
  }), (error) => error?.code === "DUPLICATE_PARTICIPANT"
    && error.status === 409
    && error.details.row_number === 2
    && error.details.duplicate_row_number === 1);
  await assert.rejects(service.createParticipantsBulk(tournament.id, {
    participants: [
      { name_en: "New Player", association_id: "POL" },
      { name_en: " existing   PLAYER ", association_id: "ukr" },
    ],
  }), (error) => error?.code === "DUPLICATE_PARTICIPANT"
    && error.details.row_number === 2
    && error.details.candidates[0].id === existing.id);
  assert.deepEqual(await service.getParticipantsOverview(tournament.id), before);
});

test("bulk registration validates every row without partially creating players", async (t) => {
  const { service } = await createDatabase(t);
  const tournament = await createPublishedTournament(service, { slug: "bulk-validation" });
  const before = await service.getParticipantsOverview(tournament.id);
  for (const participants of [undefined, [], {}, "players"]) {
    await assert.rejects(service.createParticipantsBulk(tournament.id, { participants }),
      (error) => error?.code === "PARTICIPANTS_REQUIRED");
  }
  for (const [invalid, expectedCode] of [
    [{ name_en: "   ", association_id: "UKR" }, "PARTICIPANT_NAME_REQUIRED"],
    [{ name_en: "No Country" }, "PARTICIPANT_ASSOCIATION_REQUIRED"],
    [{ name_en: "Unknown Country", association_id: "XXX" }, "UNKNOWN_ASSOCIATION"],
  ]) {
    await assert.rejects(service.createParticipantsBulk(tournament.id, {
      participants: [{ name_en: "Valid Player", association_id: "UKR" }, invalid],
    }), (error) => error?.code === expectedCode && error.details.row_number === 2);
  }
  assert.deepEqual(await service.getParticipantsOverview(tournament.id), before);
});

test("bulk local registration uses city and checks name and city duplicates", async (t) => {
  const { service } = await createDatabase(t);
  const kyiv = await service.createCity({ name_en: "Kyiv", association_id: "UKR" });
  const lviv = await service.createCity({ name_en: "Lviv", association_id: "UKR" });
  const warsaw = await service.createCity({ name_en: "Warsaw", association_id: "POL" });
  const tournament = await createPublishedTournament(service, {
    slug: "bulk-local", scope: "local", association_id: "UKR", local_subtype: "final",
  });
  const result = await service.createParticipantsBulk(tournament.id, {
    participants: [
      { name_en: "Same Name", city_id: kyiv.id },
      { name_en: "Same Name", city_id: lviv.id },
    ],
  });
  assert.deepEqual(result.participants.map((player) => [player.city_id, player.association_id]), [
    [kyiv.id, null], [lviv.id, null],
  ]);
  await assert.rejects(service.createParticipantsBulk(tournament.id, {
    participants: [{ name_en: " same   name ", city_id: lviv.id }],
  }), (error) => error?.code === "DUPLICATE_PARTICIPANT" && error.details.row_number === 1);
  for (const [cityId, expectedCode] of [
    [undefined, "PARTICIPANT_CITY_REQUIRED"],
    ["unknown", "UNKNOWN_PARTICIPANT_CITY"],
    [warsaw.id, "PARTICIPANT_CITY_ASSOCIATION_MISMATCH"],
  ]) {
    await assert.rejects(service.createParticipantsBulk(tournament.id, {
      participants: [{ name_en: "Local Player", city_id: cityId }],
    }), (error) => error?.code === expectedCode && error.details.row_number === 1);
  }
  await service.archiveCity(kyiv.id);
  await assert.rejects(service.createParticipantsBulk(tournament.id, {
    participants: [{ name_en: "Archived City Player", city_id: kyiv.id }],
  }), (error) => error?.code === "CITY_ARCHIVED" && error.details.row_number === 1);
  assert.equal((await service.getParticipantsOverview(tournament.id)).counters.total, 2);
});

test("bulk registration rolls back earlier inserts if a later insert fails", async (t) => {
  const { db, service } = await createDatabase(t);
  const tournament = await createPublishedTournament(service, { slug: "bulk-rollback" });
  const before = await service.getParticipantsOverview(tournament.id);
  await exec(db, `
    CREATE TRIGGER reject_bulk_player BEFORE INSERT ON in_person_participants
    WHEN NEW.name_en = 'Rejected Player'
    BEGIN SELECT RAISE(ABORT, 'Injected insert failure'); END;
  `);
  await assert.rejects(service.createParticipantsBulk(tournament.id, {
    participants: [
      { name_en: "Valid Player", association_id: "UKR" },
      { name_en: "Rejected Player", association_id: "POL" },
    ],
  }), /Injected insert failure/);
  assert.deepEqual(await service.getParticipantsOverview(tournament.id), before);
});

test("bulk registration follows read-only and late-entry restrictions", async (t) => {
  const { db, service } = await createDatabase(t);
  const tournament = await createPublishedTournament(service, { slug: "bulk-locked" });
  const payload = { participants: [{ name_en: "Player", association_id: "UKR" }] };
  await run(db, `INSERT INTO in_person_rounds (id, tournament_id, stage, round_number, status)
    VALUES ('bulk-locked-round', ?, 'swiss', 1, 'draft')`, [tournament.id]);
  await assert.rejects(service.createParticipantsBulk(tournament.id, payload),
    (error) => error?.code === "LATE_ENTRY_REQUIRED");
  const cancelled = await createPublishedTournament(service, { slug: "bulk-cancelled" });
  await service.cancelTournament(cancelled.id);
  await assert.rejects(service.createParticipantsBulk(cancelled.id, payload),
    (error) => error?.code === "TOURNAMENT_READ_ONLY");
  assert.equal((await service.getParticipantsOverview(tournament.id)).counters.total, 0);
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


test("fills only missing BGA IDs from unique exact nickname matches in this tournament", async (t) => {
  const { db, service } = await createDatabase(t);
  await exec(db, `
    INSERT INTO profiles (id, bga_nickname) VALUES
      ('86771532', 'ExactNick'), ('100', 'OtherNick'),
      ('101', 'Ambiguous'), ('102', 'Ambiguous'), ('invalid', 'InvalidId');
  `);
  const tournament = await createPublishedTournament(service, { slug: "fill-bga-ids" });
  const other = await createPublishedTournament(service, { slug: "other-fill-bga-ids" });
  const create = (name, nickname, bgaId = null, tournamentId = tournament.id) => service.createParticipant(tournamentId, {
    name_en: name, association_id: "UKR", bga_nickname: nickname, bga_id: bgaId, confirm_duplicate: true,
  });
  const exact = await create("Exact", "ExactNick");
  const existing = await create("Existing", "OtherNick", "999");
  const wrongCase = await create("Wrong Case", "exactnick");
  const partial = await create("Partial", "Exact");
  const unknown = await create("Unknown", "UnknownNick");
  const noNickname = await create("No Nickname", null);
  const ambiguous = await create("Ambiguous", "Ambiguous");
  const invalid = await create("Invalid ID", "InvalidId");
  const otherPlayer = await create("Other Tournament", "ExactNick", null, other.id);
  assert.deepEqual(await service.fillParticipantBgaIds(tournament.id), { updated_count: 1 });
  const overview = await service.getParticipantsOverview(tournament.id);
  const ids = new Map(overview.participants.map((player) => [player.id, player.bga_id]));
  assert.equal(ids.get(exact.id), "86771532");
  assert.equal(ids.get(existing.id), "999");
  for (const player of [wrongCase, partial, unknown, noNickname, ambiguous, invalid]) {
    assert.equal(ids.get(player.id), null);
  }
  const otherOverview = await service.getParticipantsOverview(other.id);
  assert.equal(otherOverview.participants.find((player) => player.id === otherPlayer.id).bga_id, null);
  const revision = overview.tournament.revision;
  assert.deepEqual(await service.fillParticipantBgaIds(tournament.id), { updated_count: 0 });
  assert.equal((await service.getParticipantsOverview(tournament.id)).tournament.revision, revision);
  await service.cancelTournament(tournament.id);
  await assert.rejects(service.fillParticipantBgaIds(tournament.id), (error) => error?.code === "TOURNAMENT_READ_ONLY");
});


test("updates player ratings by existing BGA ID or exact nickname and keeps missing source values", async (t) => {
  const { db, service } = await createDatabase(t);
  await exec(db, `
    INSERT INTO profiles (id, bga_nickname, gg_elo, gg_rating_position, bga_elo) VALUES
      ('86771532', 'NewNick', 1650.75, 42, 555),
      ('100', 'OldNick', 999, 99, 999),
      ('101', 'NoData', NULL, NULL, NULL),
      ('102', 'ZeroRating', 0, 1, 0);
  `);
  const tournament = await createPublishedTournament(service, { slug: "update-player-data" });
  const create = (name, nickname, bgaId = null) => service.createParticipant(tournament.id, {
    name_en: name, association_id: "UKR", bga_nickname: nickname, bga_id: bgaId,
    gg_elo: 100.5, gg_rank: 5, current_elo: 123, max_elo: 600, confirm_duplicate: true,
  });
  const existing = await create("Existing", "OldNick", "86771532");
  const missing = await create("Missing", "NewNick");
  const noData = await create("No Data", "NoData");
  const zero = await create("Zero", "ZeroRating");
  const unknownId = await create("Unknown ID", "NewNick", "9999");
  assert.deepEqual(await service.fillParticipantBgaIds(tournament.id), { updated_count: 4 });
  const overview = await service.getParticipantsOverview(tournament.id);
  const byId = new Map(overview.participants.map((player) => [player.id, player]));
  for (const participant of [existing, missing]) {
    const updated = byId.get(participant.id);
    assert.equal(updated.bga_id, "86771532");
    assert.equal(updated.gg_elo, 1650.75);
    assert.equal(updated.gg_rank, 42);
    assert.equal(updated.current_elo, 555);
    assert.equal(updated.max_elo, 600);
  }
  assert.equal(byId.get(existing.id).bga_nickname, "OldNick");
  assert.equal(byId.get(noData.id).bga_id, "101");
  for (const participant of [noData, unknownId]) {
    const updated = byId.get(participant.id);
    assert.equal(updated.gg_elo, 100.5);
    assert.equal(updated.gg_rank, 5);
    assert.equal(updated.current_elo, 123);
  }
  assert.equal(byId.get(unknownId.id).bga_id, "9999");
  assert.equal(byId.get(zero.id).gg_elo, 0);
  assert.equal(byId.get(zero.id).current_elo, 0);
  assert.equal(byId.get(zero.id).gg_rank, 1);
  assert.deepEqual(await service.fillParticipantBgaIds(tournament.id), { updated_count: 0 });
  await exec(db, "UPDATE profiles SET gg_elo = 1675.25, gg_rating_position = 38, bga_elo = 570 WHERE id = '86771532'");
  assert.deepEqual(await service.fillParticipantBgaIds(tournament.id), { updated_count: 2 });
  const refreshed = await service.getParticipantsOverview(tournament.id);
  assert.equal(refreshed.participants.find((player) => player.id === existing.id).gg_elo, 1675.25);
  const edited = await service.updateParticipant(tournament.id, existing.id, { player_information: "Updated" });
  assert.equal(edited.gg_elo, 1675.25);
});
