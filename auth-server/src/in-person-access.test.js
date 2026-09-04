import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import {
  createRequireInPersonTournamentAdmin,
  hasInPersonTournamentAdminAccess,
} from "./in-person/access.js";
import { ensureInPersonSchema } from "./in-person/schema.js";

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
    CREATE TABLE users (id INTEGER PRIMARY KEY, admin INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE associations (
      code TEXT UNIQUE COLLATE NOCASE,
      name TEXT NOT NULL UNIQUE COLLATE NOCASE
    );
    CREATE TABLE tournaments (id TEXT PRIMARY KEY);
  `);
  await ensureInPersonSchema(db, { logger: silentLogger });
  await exec(db, `
    INSERT INTO users (id, admin) VALUES (1, 0), (2, 0), (3, 1);
    INSERT INTO tournaments (id) VALUES ('same-id');
    INSERT INTO in_person_tournaments (
      id, slug, name_en, scope, start_date, end_date,
      organizer_name, swiss_rounds_count, playoff_first_round
    ) VALUES (
      'same-id', 'same-id', 'In-person cup', 'international',
      '2026-09-05', '2026-09-05', 'Organizer', 5, 'round_of_16'
    );
    INSERT INTO tournament_access_users (
      tournament_entity_type, tournament_id, user_id, role
    ) VALUES
      ('tournament', 'same-id', 1, 'admin'),
      ('in_person_tournament', 'same-id', 2, 'admin');
  `);
  return db;
}

function createResponse() {
  return {
    statusCode: 200,
    payload: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
}

test("grants only global or explicitly assigned in-person admins", async (t) => {
  const db = await createDatabase(t);

  assert.equal(await hasInPersonTournamentAdminAccess(db, null, "same-id"), false);
  assert.equal(
    await hasInPersonTournamentAdminAccess(db, { id: 1, admin: 0 }, "same-id"),
    false,
    "access to the legacy tournament must not leak into the in-person domain"
  );
  assert.equal(
    await hasInPersonTournamentAdminAccess(db, { id: 2, admin: 0 }, "same-id"),
    true
  );
  assert.equal(
    await hasInPersonTournamentAdminAccess(db, { id: 999, admin: 1 }, "same-id"),
    true
  );
});

test("permission middleware returns 401, 400 and 403 before allowing access", async (t) => {
  const db = await createDatabase(t);
  const middleware = createRequireInPersonTournamentAdmin({ db });

  const unauthorizedResponse = createResponse();
  await middleware({ params: { tournamentId: "same-id" } }, unauthorizedResponse, () => {
    assert.fail("unauthenticated request must not continue");
  });
  assert.equal(unauthorizedResponse.statusCode, 401);

  const invalidResponse = createResponse();
  await middleware({ user: { id: 2, admin: 0 }, params: {} }, invalidResponse, () => {
    assert.fail("request without a tournament id must not continue");
  });
  assert.equal(invalidResponse.statusCode, 400);

  const forbiddenResponse = createResponse();
  await middleware(
    { user: { id: 1, admin: 0 }, params: { tournamentId: "same-id" } },
    forbiddenResponse,
    () => assert.fail("legacy tournament access must not continue")
  );
  assert.equal(forbiddenResponse.statusCode, 403);

  const allowedResponse = createResponse();
  let continued = false;
  const request = { user: { id: 2, admin: 0 }, params: { tournamentId: "same-id" } };
  await middleware(request, allowedResponse, () => {
    continued = true;
  });
  assert.equal(continued, true);
  assert.equal(request.inPersonTournamentId, "same-id");
});
