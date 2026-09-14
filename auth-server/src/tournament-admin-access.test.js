import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import {
  canManageTournamentAccessUsers,
  createRequireTournamentAdmin,
  hasTournamentAdminAccess,
} from "./tournament-admin-access.js";

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    CREATE TABLE tournament_access_users (
      tournament_entity_type TEXT NOT NULL,
      tournament_id TEXT NOT NULL,
      user_id INTEGER NOT NULL,
      role TEXT NOT NULL
    );
    INSERT INTO tournament_access_users (
      tournament_entity_type, tournament_id, user_id, role
    ) VALUES
      ('tournament', 'managed-cup', 1, 'admin'),
      ('tournament', 'captained-cup', 2, 'captain'),
      ('in_person_tournament', 'in-person-cup', 3, 'admin');
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

test("grants tournament editing only to global admins or assigned tournament admins", async (t) => {
  const db = await createDatabase(t);

  assert.equal(await hasTournamentAdminAccess(db, null, "managed-cup"), false);
  assert.equal(await hasTournamentAdminAccess(db, { id: 1, admin: 0 }, "MANAGED-CUP"), true);
  assert.equal(await hasTournamentAdminAccess(db, { id: 2, admin: 0 }, "captained-cup"), false);
  assert.equal(await hasTournamentAdminAccess(db, { id: 3, admin: 0 }, "in-person-cup"), false);
  assert.equal(await hasTournamentAdminAccess(db, { id: 999, admin: 1 }, "any-cup"), true);
});

test("only global admins may manage tournament access-user records", () => {
  assert.equal(canManageTournamentAccessUsers({ id: 1, admin: 0 }), false);
  assert.equal(canManageTournamentAccessUsers({ id: 2, admin: 1 }), true);
});

test("tournament-admin middleware rejects unauthorized users and accepts assigned admins", async (t) => {
  const db = await createDatabase(t);
  const middleware = createRequireTournamentAdmin({ db });

  const unauthorizedResponse = createResponse();
  await middleware({ params: { id: "managed-cup" } }, unauthorizedResponse, () => {
    assert.fail("unauthenticated request must not continue");
  });
  assert.equal(unauthorizedResponse.statusCode, 401);

  const invalidResponse = createResponse();
  await middleware({ user: { id: 1, admin: 0 }, params: {} }, invalidResponse, () => {
    assert.fail("request without a tournament id must not continue");
  });
  assert.equal(invalidResponse.statusCode, 400);

  const forbiddenResponse = createResponse();
  await middleware(
    { user: { id: 2, admin: 0 }, body: { tournament_id: "captained-cup" } },
    forbiddenResponse,
    () => assert.fail("a tournament captain must not continue")
  );
  assert.equal(forbiddenResponse.statusCode, 403);

  const allowedResponse = createResponse();
  const request = { user: { id: 1, admin: 0 }, query: { tournament_id: "managed-cup" } };
  let continued = false;
  await middleware(request, allowedResponse, () => {
    continued = true;
  });
  assert.equal(continued, true);
  assert.equal(request.managedTournamentId, "managed-cup");
});
