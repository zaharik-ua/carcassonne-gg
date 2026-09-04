import assert from "node:assert/strict";
import test from "node:test";
import express from "express";
import sqlite3 from "sqlite3";
import { createRequireInPersonTournamentAdmin } from "./in-person/access.js";
import { registerInPersonRoutes } from "./in-person/routes.js";
import { ensureInPersonSchema } from "./in-person/schema.js";
import { createInPersonService } from "./in-person/service.js";

const silentLogger = { info() {}, error() {} };

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

async function startApi(t) {
  const db = new sqlite3.Database(":memory:");
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
    INSERT INTO users (id, email, name, admin) VALUES
      (1, 'one@example.com', 'Organizer One', 0),
      (2, 'two@example.com', 'Organizer Two', 0),
      (3, 'admin@example.com', 'Global Admin', 1);
    INSERT INTO associations (code, name, flag) VALUES ('UKR', 'Ukraine', 'ua');
  `);
  await ensureInPersonSchema(db, { logger: silentLogger });
  const service = createInPersonService({ db });
  const first = await service.createTournament({
    slug: "first-cup",
    name_en: "First Cup",
    scope: "international",
    start_date: "2026-10-10",
    end_date: "2026-10-10",
    organizer_name: "Organizer",
    swiss_rounds_count: 5,
    playoff_first_round: "semi_final",
    admin_user_ids: [1],
  });
  const second = await service.createTournament({
    slug: "second-cup",
    name_en: "Second Cup",
    scope: "international",
    start_date: "2026-10-11",
    end_date: "2026-10-11",
    organizer_name: "Organizer",
    swiss_rounds_count: 5,
    playoff_first_round: "semi_final",
    admin_user_ids: [2],
  });
  await service.publishTournament(first.id);
  await service.publishTournament(second.id);

  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    const userId = Number(req.get("x-test-user"));
    if (userId) req.user = { id: userId, admin: userId === 3 ? 1 : 0 };
    next();
  });
  const requireAuthenticated = (req, res, next) => (
    req.user ? next() : res.status(401).json({ ok: false, message: "Unauthorized" })
  );
  const requireAdmin = (req, res, next) => (
    Number(req.user?.admin) === 1
      ? next()
      : res.status(req.user ? 403 : 401).json({ ok: false, message: req.user ? "Forbidden" : "Unauthorized" })
  );
  registerInPersonRoutes(app, {
    db,
    service,
    requireAdmin,
    requireAuthenticated,
    requireInPersonTournamentAdmin: createRequireInPersonTournamentAdmin({ db }),
    logger: silentLogger,
  });
  app.use((error, _req, res, _next) => {
    res.status(500).json({ ok: false, message: error?.message || "Internal error" });
  });
  const server = await new Promise((resolve) => {
    const listener = app.listen(0, "127.0.0.1", () => resolve(listener));
  });
  t.after(async () => {
    await new Promise((resolve) => server.close(resolve));
    await new Promise((resolve) => db.close(resolve));
  });
  return {
    baseUrl: `http://127.0.0.1:${server.address().port}`,
    first,
    second,
  };
}

async function api(baseUrl, path, { userId = null, ...options } = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(userId ? { "x-test-user": String(userId) } : {}),
      ...(options.headers || {}),
    },
  });
  return { response, data: await response.json() };
}

test("accessible endpoint and participant mutations enforce organizer access", async (t) => {
  const { baseUrl, first, second } = await startApi(t);
  const unauthorized = await api(baseUrl, "/in-person-tournaments/accessible");
  assert.equal(unauthorized.response.status, 401);

  const accessible = await api(baseUrl, "/in-person-tournaments/accessible", { userId: 1 });
  assert.equal(accessible.response.status, 200);
  assert.deepEqual(accessible.data.tournaments.map((item) => item.id), [first.id]);

  const forbidden = await api(baseUrl, `/in-person-tournaments/${second.id}/participants`, {
    userId: 1,
  });
  assert.equal(forbidden.response.status, 403);

  const created = await api(baseUrl, `/in-person-tournaments/${first.id}/participants`, {
    userId: 1,
    method: "POST",
    body: JSON.stringify({ name_en: "Player One", association_id: "UKR" }),
  });
  assert.equal(created.response.status, 201);
  assert.equal(created.data.participant.name_en, "Player One");

  const globalAdmin = await api(baseUrl, `/in-person-tournaments/${second.id}/participants`, {
    userId: 3,
  });
  assert.equal(globalAdmin.response.status, 200);
});

test("participant API returns duplicate details and check-in readiness", async (t) => {
  const { baseUrl, first } = await startApi(t);
  const playerIds = [];
  for (let index = 1; index <= 4; index += 1) {
    const created = await api(baseUrl, `/in-person-tournaments/${first.id}/participants`, {
      userId: 1,
      method: "POST",
      body: JSON.stringify({ name_en: `Player ${index}`, association_id: "UKR" }),
    });
    assert.equal(created.response.status, 201);
    playerIds.push(created.data.participant.id);
  }
  const duplicate = await api(baseUrl, `/in-person-tournaments/${first.id}/participants`, {
    userId: 1,
    method: "POST",
    body: JSON.stringify({ name_en: " player 1 ", association_id: "UKR" }),
  });
  assert.equal(duplicate.response.status, 409);
  assert.equal(duplicate.data.code, "DUPLICATE_PARTICIPANT");
  assert.equal(duplicate.data.details.candidates[0].id, playerIds[0]);

  const start = await api(baseUrl, `/in-person-tournaments/${first.id}/start-check-in`, {
    userId: 1,
    method: "POST",
    body: "{}",
  });
  assert.equal(start.response.status, 200);
  for (let index = 0; index < playerIds.length; index += 1) {
    const checkedIn = await api(
      baseUrl,
      `/in-person-tournaments/${first.id}/participants/${playerIds[index]}/check-in`,
      {
        userId: 1,
        method: "PATCH",
        body: JSON.stringify({ checked_in: true, draw_number: [2, 3, 5, 8][index] }),
      }
    );
    assert.equal(checkedIn.response.status, 200);
  }
  const readiness = await api(
    baseUrl,
    `/in-person-tournaments/${first.id}/check-in-readiness`,
    { userId: 1 }
  );
  assert.equal(readiness.response.status, 200);
  assert.equal(readiness.data.readiness.ready, true);
  assert.equal(readiness.data.counters.without_draw_number, 0);
});

test("assigned organizer creates a city with the local tournament country enforced", async (t) => {
  const { baseUrl } = await startApi(t);
  const localTournament = await api(baseUrl, "/in-person-tournaments", {
    userId: 3,
    method: "POST",
    body: JSON.stringify({
      slug: "local-city-api",
      name_en: "Local City API",
      scope: "local",
      association_id: "UKR",
      local_subtype: "final",
      start_date: "2026-10-12",
      end_date: "2026-10-12",
      organizer_name: "Organizer",
      swiss_rounds_count: 5,
      playoff_first_round: "semi_final",
      admin_user_ids: [1],
    }),
  });
  assert.equal(localTournament.response.status, 201);
  const tournamentId = localTournament.data.tournament.id;

  const created = await api(
    baseUrl,
    `/in-person-tournaments/${tournamentId}/participant-cities`,
    {
      userId: 1,
      method: "POST",
      body: JSON.stringify({
        association_id: "OTHER",
        name_en: "Lviv",
        name_local: "Львів",
        icon_url: "https://example.com/lviv.svg",
      }),
    }
  );
  assert.equal(created.response.status, 201);
  assert.equal(created.data.city.association_id, "UKR");
  assert.equal(created.data.city.icon_url, "https://example.com/lviv.svg");

  const forbidden = await api(
    baseUrl,
    `/in-person-tournaments/${tournamentId}/participant-cities`,
    {
      userId: 2,
      method: "POST",
      body: JSON.stringify({ name_en: "Forbidden city" }),
    }
  );
  assert.equal(forbidden.response.status, 403);
});

test("organizer API completes a Swiss round for 4, 5 and 8 participants", async (t) => {
  const { baseUrl } = await startApi(t);
  for (const participantCount of [4, 5, 8]) {
    const createdTournament = await api(baseUrl, "/in-person-tournaments", {
      userId: 3,
      method: "POST",
      body: JSON.stringify({
        slug: `api-swiss-${participantCount}`,
        name_en: `API Swiss ${participantCount}`,
        scope: "international",
        start_date: "2026-10-20",
        end_date: "2026-10-20",
        organizer_name: "Organizer",
        swiss_rounds_count: 1,
        playoff_first_round: participantCount >= 8 ? "quarter_final" : "semi_final",
        admin_user_ids: [1],
      }),
    });
    assert.equal(createdTournament.response.status, 201);
    const tournamentId = createdTournament.data.tournament.id;
    const published = await api(baseUrl, `/in-person-tournaments/${tournamentId}/publish`, {
      userId: 3,
      method: "POST",
      body: "{}",
    });
    assert.equal(published.response.status, 200);

    const participantIds = [];
    for (let index = 0; index < participantCount; index += 1) {
      const createdParticipant = await api(
        baseUrl,
        `/in-person-tournaments/${tournamentId}/participants`,
        {
          userId: 1,
          method: "POST",
          body: JSON.stringify({
            name_en: `API Player ${participantCount}-${index + 1}`,
            association_id: "UKR",
          }),
        }
      );
      assert.equal(createdParticipant.response.status, 201);
      participantIds.push(createdParticipant.data.participant.id);
    }
    await api(baseUrl, `/in-person-tournaments/${tournamentId}/start-check-in`, {
      userId: 1,
      method: "POST",
      body: "{}",
    });
    for (let index = 0; index < participantIds.length; index += 1) {
      const checkIn = await api(
        baseUrl,
        `/in-person-tournaments/${tournamentId}/participants/${participantIds[index]}/check-in`,
        {
          userId: 1,
          method: "PATCH",
          body: JSON.stringify({ checked_in: true, draw_number: index + 2 }),
        }
      );
      assert.equal(checkIn.response.status, 200);
    }

    const preview = await api(
      baseUrl,
      `/in-person-tournaments/${tournamentId}/swiss/rounds/preview`,
      { userId: 1, method: "POST", body: JSON.stringify({ round_number: 1 }) }
    );
    assert.equal(preview.response.status, 200);
    assert.equal(preview.data.preview.matches.length, Math.ceil(participantCount / 2));
    const confirmed = await api(
      baseUrl,
      `/in-person-tournaments/${tournamentId}/swiss/rounds/confirm`,
      { userId: 1, method: "POST", body: JSON.stringify({ round_number: 1 }) }
    );
    assert.equal(confirmed.response.status, 200);
    const round = confirmed.data.current_round;
    const publishedRound = await api(
      baseUrl,
      `/in-person-tournaments/${tournamentId}/swiss/rounds/${round.id}/publish`,
      { userId: 1, method: "POST", body: "{}" }
    );
    assert.equal(publishedRound.response.status, 200);
    for (const match of publishedRound.data.current_round.matches.filter((entry) => !entry.is_bye)) {
      const result = await api(
        baseUrl,
        `/in-person-tournaments/${tournamentId}/swiss/matches/${match.id}/result`,
        {
          userId: 1,
          method: "PUT",
          body: JSON.stringify({
            starting_participant_id: match.starting_participant_id,
            result_type: "simple",
            winner_participant_id: match.participant_a_id,
          }),
        }
      );
      assert.equal(result.response.status, 200);
    }
    const completed = await api(
      baseUrl,
      `/in-person-tournaments/${tournamentId}/swiss/rounds/${round.id}/complete`,
      { userId: 1, method: "POST", body: "{}" }
    );
    assert.equal(completed.response.status, 200);
    assert.equal(completed.data.swiss_complete, true);
    assert.equal(completed.data.standings.rows.length, participantCount);
  }
});

test("organizer API previews late entry and rolls back exactly the active Swiss round", async (t) => {
  const { baseUrl, first } = await startApi(t);
  const participantIds = [];
  for (let index = 0; index < 5; index += 1) {
    const created = await api(baseUrl, `/in-person-tournaments/${first.id}/participants`, {
      userId: 1,
      method: "POST",
      body: JSON.stringify({ name_en: `Late API Player ${index + 1}`, association_id: "UKR" }),
    });
    assert.equal(created.response.status, 201);
    participantIds.push(created.data.participant.id);
  }
  await api(baseUrl, `/in-person-tournaments/${first.id}/start-check-in`, {
    userId: 1,
    method: "POST",
    body: "{}",
  });
  for (let index = 0; index < participantIds.length; index += 1) {
    const checkedIn = await api(
      baseUrl,
      `/in-person-tournaments/${first.id}/participants/${participantIds[index]}/check-in`,
      {
        userId: 1,
        method: "PATCH",
        body: JSON.stringify({ checked_in: true, draw_number: index + 2 }),
      }
    );
    assert.equal(checkedIn.response.status, 200);
  }
  const confirmed = await api(
    baseUrl,
    `/in-person-tournaments/${first.id}/swiss/rounds/confirm`,
    {
      userId: 1,
      method: "POST",
      body: JSON.stringify({ round_number: 1, publish: true }),
    }
  );
  assert.equal(confirmed.response.status, 200);
  const round = confirmed.data.current_round;
  assert.equal(round.status, "published");
  const latePayload = {
    name_en: "Late API Arrival",
    association_id: "UKR",
    mode: "pair_with_bye",
    table_number: 9,
    starting_participant: "late_participant",
  };
  const latePreview = await api(
    baseUrl,
    `/in-person-tournaments/${first.id}/participants/late/preview`,
    { userId: 1, method: "POST", body: JSON.stringify(latePayload) }
  );
  assert.equal(latePreview.response.status, 200);
  assert.equal(latePreview.data.preview.change.type, "replace_bye_with_match");
  const lateConfirm = await api(
    baseUrl,
    `/in-person-tournaments/${first.id}/participants/late/confirm`,
    {
      userId: 1,
      method: "POST",
      body: JSON.stringify({
        ...latePayload,
        bye_match_id: latePreview.data.preview.bye_match.id,
        expected_round_revision: latePreview.data.preview.round.revision,
      }),
    }
  );
  assert.equal(lateConfirm.response.status, 201);
  assert.equal(lateConfirm.data.participant.is_late_entry, true);
  assert.equal(lateConfirm.data.current_round.matches.some((match) => match.is_bye), false);

  const inactive = await api(
    baseUrl,
    `/in-person-tournaments/${first.id}/participants/${participantIds[0]}/status`,
    {
      userId: 1,
      method: "PATCH",
      body: JSON.stringify({ status: "withdrawn", reason: "API draft no-show" }),
    }
  );
  assert.equal(inactive.response.status, 200);
  assert.equal(inactive.data.participant.status, "withdrawn");
  assert.equal(inactive.data.resolution.type, "technical_result");

  const cancellationPreview = await api(
    baseUrl,
    `/in-person-tournaments/${first.id}/swiss/rounds/${round.id}/cancellation-preview`,
    { userId: 1 }
  );
  assert.equal(cancellationPreview.response.status, 200);
  assert.equal(cancellationPreview.data.preview.round.id, round.id);
  assert.equal(cancellationPreview.data.preview.completed_results_count, 0);
  const cancelled = await api(
    baseUrl,
    `/in-person-tournaments/${first.id}/swiss/rounds/${round.id}/cancel`,
    { userId: 1, method: "POST", body: JSON.stringify({ reason: "API recovery" }) }
  );
  assert.equal(cancelled.response.status, 200);
  assert.equal(cancelled.data.cancelled, true);
  assert.equal(cancelled.data.current_round, null);
  assert.equal(cancelled.data.tournament.status, "check_in");

  const forbidden = await api(
    baseUrl,
    `/in-person-tournaments/${first.id}/participants/late/preview`,
    { userId: 2, method: "POST", body: JSON.stringify(latePayload) }
  );
  assert.equal(forbidden.response.status, 403);
});
