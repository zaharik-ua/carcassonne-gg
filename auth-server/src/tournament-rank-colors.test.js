import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import sqlite3 from "sqlite3";
import { normalizeTournamentLineupType, resolveTournamentTextPatch } from "./tournament-update.js";
import { normalizeTournamentRankColors, resolveTournamentRankColorsPatch } from "./tournament-rank-colors.js";

const expected = { stages: { "Stage 1": {
  rankColors: { 1: "blue", 2: "blue", 3: "green", 4: "green" },
  rankColorLegend: { blue: "Advance to the Final", green: "Advance to the Bronze match" },
} } };

test("accepts structured UI settings and stored JSON", () => {
  for (const input of [JSON.stringify(expected), expected]) {
    assert.deepEqual(JSON.parse(normalizeTournamentRankColors(input)), expected);
  }
  assert.equal(normalizeTournamentRankColors(expected.stages), JSON.stringify(expected));
});

test("preserves punctuation, quotes and Unicode in legend labels", () => {
  const label = 'Фінал: місця 1, 2 {A} — "Final"';
  const settings = { stages: {
    "Stage 2": { rankColors: { 1: "GOLD" }, rankColorLegend: { gold: label } },
    "Stage 3": { rankColors: { 2: "silver", 3: "bronze" } },
  } };
  const parsed = JSON.parse(normalizeTournamentRankColors(settings));
  assert.equal(parsed.stages["Stage 2"].rankColors[1], "gold");
  assert.equal(parsed.stages["Stage 2"].rankColorLegend.gold, label);
  assert.deepEqual(parsed.stages["Stage 3"].rankColors, { 2: "silver", 3: "bronze" });
});

test("rejects invalid structure, ranks, colors and malformed input", () => {
  for (const value of [
    "not settings", "stages: {Stage 1: {rankColors: {1: blue}}", "[]", "null",
    { stages: [] }, { stages: { "Stage 4": {} } },
    { stages: { "Stage 1": { rankColors: { 0: "blue" } } } },
    { stages: { "Stage 1": { rankColors: { 1.5: "blue" } } } },
    { stages: { "Stage 1": { rankColors: { 1: "purple" } } } },
    { stages: { "Stage 1": { rankColorLegend: { blue: 42 } } } },
    { stages: { "Stage 1": { rankColorLegend: { blue: " " } } } },
    { stages: { "Stage 1": { rankColours: {} } } },
  ]) assert.throws(() => normalizeTournamentRankColors(value), /Rank colors:/);
});

test("clears empty settings and preserves settings omitted from a partial update", () => {
  for (const value of [undefined, null, "", "  ", {}, { stages: {} }]) {
    assert.equal(normalizeTournamentRankColors(value), null);
  }
  const current = { rank_colors: JSON.stringify(expected) };
  assert.equal(resolveTournamentRankColorsPatch({ name: "Renamed" }, current), current.rank_colors);
  assert.equal(resolveTournamentRankColorsPatch({ rank_colors: "" }, current), null);
});

const server = readFileSync(new URL("./server.js", import.meta.url), "utf8");
async function createApi(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise(resolve => db.close(resolve)));
  const schema = server.match(/function ensureTournamentsSchema\(\) \{\s*db.run\(`([\s\S]*?)`/)[1];
  await new Promise((resolve, reject) => db.exec(schema, error => error ? reject(error) : resolve()));
  const handlers = new Map();
  const nullable = value => String(value ?? "").trim() || null;
  const context = vm.createContext({
    db,
    app: Object.fromEntries(["get", "post", "patch"].map(method => [method, (path, ...callbacks) => {
      handlers.set(`${method} ${path}`, callbacks.at(-1));
    }])),
    requireAdmin() {}, requireTournamentAdmin() {},
    normalizeTournamentRankColors, resolveTournamentRankColorsPatch, resolveTournamentTextPatch,
    normalizeTournamentLineupType,
    normalizeNullableText: nullable, normalizeUtcTimestamp: nullable,
    normalizeBooleanInt: value => value ? 1 : 0,
    normalizeStandingsScoring: value => value || "standard",
    bountyTprNumber: (value, fallback) => value == null ? fallback : Number(value),
    normalizeTournamentType: value => value || "Teams",
    normalizeTeamType: value => value || "National", isValidTeamType: () => true,
    normalizeCategoryName: nullable, resolveTournamentCategory: async () => null,
    normalizeTournamentSubtypeForType: () => "Friendly",
    normalizeTournamentPlayerHubVisibility: () => "Visible",
    normalizeTournamentLineupSizeType: () => 2, normalizeTournamentLineupSize: () => null,
    normalizeTournamentTeamSize: value => value || 10, isValidTournamentTeamSize: () => true,
    normalizeTournamentStageSettings: () => ({
      tournamentFormat: "2 Stages", stage1Groups: 1, stage1Format: "Round-robin", stage2Format: "Single Elimination",
    }),
    normalizeTournamentAccessUsers: () => [], canManageTournamentAccessUsers: () => false,
    validateTournamentAccessUserIds: (_users, done) => done(null, []),
    replaceTournamentAccessUsers: (_id, _users, done) => done(null),
    loadTournamentRowById: (id, _include, done) => db.get("SELECT * FROM tournaments WHERE id = ?", [id], done),
    getTournamentLookupVariants: id => [id, id.toUpperCase()],
    buildTournamentLookupWhereClause: column => `(upper(trim(${column})) = upper(trim(?)) OR upper(trim(${column})) = upper(trim(?)))`,
    TOURNAMENT_TYPES: { TEAMS: "Teams", INDIVIDUALS: "Individuals" },
    TOURNAMENT_ACCESS_TYPES: { FRIENDLY: "Friendly", OFFICIAL: "Official" },
    TOURNAMENT_LINEUP_SIZE_TYPES: { FIXED: 1 },
  });
  vm.runInContext(server.slice(server.indexOf('app.get("/public/tournaments/:id",'), server.indexOf('app.get("/tournaments",')), context);
  vm.runInContext(server.slice(server.indexOf('app.post("/tournaments",'), server.indexOf('app.get("/tournament-teams",')), context);
  return (method, body = {}) => new Promise((resolve, reject) => {
    const path = method === "get" ? "/public/tournaments/:id" : method === "post" ? "/tournaments" : "/tournaments/:id";
    Promise.resolve(handlers.get(`${method} ${path}`)({ body, params: { id: "ETCOC-2026" }, user: {} }, {
      statusCode: 200,
      status(code) { this.statusCode = code; return this; },
      json(payload) { resolve({ statusCode: this.statusCode, ...payload }); },
    }, reject)).catch(reject);
  });
}

test("SQLite API persists, updates, publishes and clears rank settings without losing partial updates", async t => {
  const request = await createApi(t);
  const created = await request("post", { id: "ETCOC-2026", name: "ETCOC", rank_colors: expected });
  assert.equal(created.statusCode, 200);
  assert.deepEqual(JSON.parse(created.tournament.rank_colors), expected);
  const renamed = await request("patch", { name: "Renamed ETCOC" });
  assert.equal(renamed.statusCode, 200);
  assert.equal(renamed.tournament.rank_colors, created.tournament.rank_colors);
  const publicTournament = await request("get");
  assert.equal(publicTournament.statusCode, 200);
  assert.equal(publicTournament.tournament.rank_colors, created.tournament.rank_colors);
  const invalid = await request("patch", { name: "Bad update", rank_colors: "bad settings" });
  assert.equal(invalid.statusCode, 400);
  assert.equal((await request("get")).tournament.rank_colors, created.tournament.rank_colors);
  const changed = await request("patch", { name: "ETCOC", rank_colors: { stages: { "Stage 1": { rankColors: { 1: "gold" } } } } });
  assert.equal(changed.statusCode, 200);
  assert.equal(JSON.parse(changed.tournament.rank_colors).stages["Stage 1"].rankColors[1], "gold");
  assert.equal((await request("patch", { name: "ETCOC", rank_colors: "" })).statusCode, 200);
  assert.equal((await request("get")).tournament.rank_colors, null);
});

test("ETCOC loads database settings and creates stage-specific badges and legends", async () => {
  const html = readFileSync(new URL("../../gg-html/ETCOC-2026.html", import.meta.url), "utf8");
  const context = vm.createContext({
    currentTournament: { rankColors: null }, currentTournamentId: "ETCOC-2026", API_BASE: "https://example.test",
    fetch: async () => ({ ok: true, json: async () => ({ tournament: { rank_colors: normalizeTournamentRankColors(expected) } }) }),
    document: { createElement: () => ({}) },
  });
  vm.runInContext(html.slice(html.indexOf("  function parseRankColorsField("), html.indexOf("  function getFlagByPlayerId(")), context);
  vm.runInContext(html.slice(html.indexOf("  async function loadTournamentHtmlFromDatabase("), html.indexOf("  async function loadTournamentTeamsFromDatabase(")), context);
  await context.loadTournamentHtmlFromDatabase();
  for (const [position, color] of [[1, "blue"], [2, "blue"], [3, "green"], [4, "green"]]) {
    const badge = context.createRankBadge(position, context.currentTournament, "Stage 1");
    assert.equal(badge.className, `rank-badge rank-badge--${color}`);
    assert.equal(badge.textContent, String(position));
  }
  assert.equal(context.createRankBadge(5, context.currentTournament, "Stage 1"), null);
  assert.equal(context.createRankBadge(1, context.currentTournament, "Stage 2"), null);
  assert.deepEqual(JSON.parse(JSON.stringify(context.getRankColorLegendForStage(context.currentTournament, "Stage 1"))), expected.stages["Stage 1"].rankColorLegend);
  assert.equal(context.getRankColorLegendForStage(context.currentTournament, "Stage 2"), null);
  for (const match of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)) {
    assert.doesNotThrow(() => new Function(match[1]));
  }
});
