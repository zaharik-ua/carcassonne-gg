import assert from "node:assert/strict";
import test from "node:test";
import express from "express";
import sqlite3 from "sqlite3";
import { registerInPersonRoutes } from "./in-person/routes.js";
import { ensureInPersonSchema } from "./in-person/schema.js";

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
    INSERT INTO users (id, email, name, admin) VALUES (1, 'organizer@example.com', 'Organizer', 0);
    INSERT INTO associations (code, name, flag) VALUES ('UKR', 'Ukraine', 'ua');
  `);
  await ensureInPersonSchema(db, { logger: silentLogger });

  const app = express();
  app.use(express.json());
  const requireAdmin = (req, _res, next) => {
    req.user = { id: 99, admin: 1 };
    next();
  };
  registerInPersonRoutes(app, {
    enabled: true,
    db,
    requireAdmin,
    requireInPersonTournamentAdmin: requireAdmin,
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
  const address = server.address();
  return `http://127.0.0.1:${address.port}`;
}

async function api(baseUrl, path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
  });
  return { response, data: await response.json() };
}

test("stage-two admin API creates a qualifier, assigns an organizer and publishes it", async (t) => {
  const baseUrl = await startApi(t);
  const cityResponse = await api(baseUrl, "/cities", {
    method: "POST",
    body: JSON.stringify({ association_id: "UKR", name_en: "Kyiv", name_local: "Київ" }),
  });
  assert.equal(cityResponse.response.status, 201);
  assert.equal(cityResponse.data.city.name_en, "Kyiv");

  const createResponse = await api(baseUrl, "/in-person-tournaments", {
    method: "POST",
    body: JSON.stringify({
      slug: "ukraine-championship-2026",
      name_en: "Ukraine Championship 2026",
      name_local: "Чемпіонат України 2026",
      scope: "local",
      association_id: "UKR",
      local_subtype: "qualifier",
      qualifier_city_id: cityResponse.data.city.id,
      start_date: "2026-08-22",
      end_date: "2026-08-23",
      organizer_name: "Carcassonne Ukraine",
      organizer_url: "https://carcassonne.com.ua",
      rules_url: "https://carcassonne.com.ua/rules",
      swiss_rounds_count: 6,
      playoff_first_round: "quarter_final",
      admin_user_ids: [1],
    }),
  });
  assert.equal(createResponse.response.status, 201);
  assert.equal(createResponse.data.tournament.status, "draft");
  assert.deepEqual(createResponse.data.tournament.admin_user_ids, [1]);

  const tournamentId = createResponse.data.tournament.id;
  const publishResponse = await api(
    baseUrl,
    `/in-person-tournaments/${encodeURIComponent(tournamentId)}/publish`,
    { method: "POST", body: "{}" }
  );
  assert.equal(publishResponse.response.status, 200);
  assert.equal(publishResponse.data.tournament.status, "registration");

  const listResponse = await api(baseUrl, "/in-person-tournaments");
  assert.equal(listResponse.response.status, 200);
  assert.equal(listResponse.data.tournaments.length, 1);
  assert.equal(listResponse.data.tournaments[0].qualifier_city_name_en, "Kyiv");

  const invalidPatch = await api(
    baseUrl,
    `/in-person-tournaments/${encodeURIComponent(tournamentId)}`,
    { method: "PATCH", body: JSON.stringify({ slug: "new-slug" }) }
  );
  assert.equal(invalidPatch.response.status, 409);
  assert.equal(invalidPatch.data.code, "SLUG_IMMUTABLE");
});
