import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import {
  getTournamentAccessType, readTournamentClassification, resolveTournamentClassificationInput,
  TOURNAMENT_CATEGORIES, tournamentAccessTypeSql, tournamentCategorySql, tournamentTeamTypeSql, tournamentTypeSql,
} from "./tournament-classification.js";

test("all eleven category rules accept the new contract and derive compatible legacy fields", () => {
  for (const category of TOURNAMENT_CATEGORIES) {
    const result = resolveTournamentClassificationInput({ type: category.type, category: category.name });
    assert.equal(result.type, category.type);
    assert.equal(result.category, category.name);
    assert.equal(result.access_type, category.mode === "official" ? "Official" : "Friendly");
    assert.equal(result.team_type, category.name === "Clubs" ? "Club" : "National");
    assert.equal(result.subtype, category.type === "Teams" ? result.access_type : null);
  }
});

test("legacy National is individual; KoC and category-less Friendly retain their intended identities", () => {
  for (const payload of [{ tournament_type: "National", category: null }, { type: "Individuals", category: "National" }]) {
    assert.equal(resolveTournamentClassificationInput(payload).category, "National");
    assert.equal(resolveTournamentClassificationInput(payload).type, "Individuals");
  }
  assert.equal(resolveTournamentClassificationInput({ tournament_type: "Individuals", category: "KoC" }).category, "King of Carcassonne");
  assert.equal(resolveTournamentClassificationInput({ tournament_type: "Teams", subtype: "Friendly" }).category, "Friendly");
  assert.equal(resolveTournamentClassificationInput({ tournament_type: "Clubs", team_type: "National" }).team_type, "Club");
  // Friendly has no special ID or singleton validation.
  assert.equal(resolveTournamentClassificationInput({ id: "another-friendly", type: "Teams", category: "Friendly" }).category, "Friendly");
});

test("invalid types, mismatched pairs and contradictory legacy fields are rejected", () => {
  for (const payload of [
    {}, { type: "Teams" }, { type: "National", category: "National" },
    { type: "Individuals", category: "WTCOC" }, { type: "Teams", category: "Rivals" },
    { type: "Individuals", category: "Local" }, { type: "Teams", category: "Other" },
    { type: "Teams", tournament_type: "Individuals", category: "WTCOC" },
    { type: "Teams", category: "WTCOC", team_type: "Club" },
    { tournament_type: "Teams", category: "ETCOC", subtype: "Friendly" },
    { type: "Teams", category: "Friendly", access_type: "Official" },
    { type: "Teams", category: "Friendly", subtype: "typo" },
  ]) assert.throws(() => resolveTournamentClassificationInput(payload), { code: "INVALID_TOURNAMENT_CLASSIFICATION" });
});

test("metadata patches retain canonical classification and ignore obsolete stored values", () => {
  for (const current of [
    { type: "Teams", category: "WTCOC", subtype: "Friendly" },
    { type: "Individuals", category: "Rivals", standings_scoring: "bounty_tpr" },
    { type: "Individuals", category: "National" },
    { type: "Teams", category: "Old Cup", subtype: "Official" },
  ]) {
    assert.deepEqual(resolveTournamentClassificationInput({ name: "Renamed" }, current), readTournamentClassification(current));
  }
  assert.equal(getTournamentAccessType({ type: "Teams", category: "WTCOC", subtype: "Friendly" }), "Official");
  assert.equal(getTournamentAccessType({ type: "Teams", category: "Friendly", subtype: "Official" }), "Friendly");
});

test("SQL filters and JavaScript rules agree with no legacy columns", async (t) => {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  const run = (sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, (error) => error ? reject(error) : resolve()));
  await run("CREATE TABLE tournaments (id INTEGER PRIMARY KEY, type TEXT, category TEXT)");
  const rows = [
    ...TOURNAMENT_CATEGORIES.map(({ name, type }) => ({ category: name, type })),
    { type: "Individuals", category: "KoC" },
    { type: "Teams", category: "Other" },
    { type: "Individuals", category: "Other" },
    { type: "Teams", category: "WTCOC", subtype: "Friendly", team_type: "Club" },
    { type: "Teams", category: "Friendly", subtype: "Official" },
    { type: null, category: null },
  ];
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    await run("INSERT INTO tournaments VALUES (?, ?, ?)", [i, row.type ?? null, row.category ?? null]);
  }
  const results = await new Promise((resolve, reject) => db.all(`SELECT id,
    ${tournamentTypeSql("t")} AS type, ${tournamentCategorySql("t")} AS category,
    ${tournamentTeamTypeSql("t")} AS team_type, ${tournamentAccessTypeSql("t")} AS access_type
    FROM tournaments t ORDER BY id`, (error, results) => error ? reject(error) : resolve(results)));
  for (const { id, ...result } of results) {
    const { subtype, ...expected } = readTournamentClassification(rows[id]);
    assert.deepEqual(result, expected, JSON.stringify(rows[id]));
  }
});
