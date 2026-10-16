import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import sqlite3 from "sqlite3";
import { ensureCategoriesSchema } from "./categories.js";
import { applyTournamentClassificationPlan, buildTournamentClassificationPlan, openClassificationDatabase } from "./tournament-classification-migration.js";
import { applyTournamentCleanupPlan, buildTournamentCleanupPlan, LEGACY_TOURNAMENT_COLUMNS } from "./tournament-classification-cleanup.js";

const root = fileURLToPath(new URL("../", import.meta.url));
const source = readFileSync(new URL("./server.js", import.meta.url), "utf8");
const all = (db, sql) => new Promise((resolve, reject) => db.all(sql, (error, rows) => error ? reject(error) : resolve(rows)));
const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (error) => error ? reject(error) : resolve()));
const close = (db) => new Promise((resolve) => db.close(resolve));
const stop = (child) => child.exitCode !== null ? Promise.resolve() : new Promise((resolve) => { child.once("exit", resolve); child.kill("SIGTERM"); });

test("real server runs before cleanup and after two restarts without recreating obsolete columns", { timeout: 30000 }, async (t) => {
  const folder = await mkdtemp(path.join(tmpdir(), "classification startup "));
  const filename = path.join(folder, "auth.sqlite");
  const setup = new sqlite3.Database(filename);
  await exec(setup, source.match(/CREATE TABLE matches \([\s\S]*?\n        \);/)[0]);
  await exec(setup, readFileSync(new URL("./fixtures/tournaments-legacy.sql", import.meta.url), "utf8"));
  await exec(setup, source.match(/CREATE TABLE IF NOT EXISTS profiles \([\s\S]*?\n            \)/)[0]);
  await exec(setup, `ALTER TABLE profiles ADD COLUMN deleted_at TEXT;
    ALTER TABLE profiles ADD COLUMN created_by TEXT;
    ALTER TABLE profiles ADD COLUMN updated_by TEXT;
    ALTER TABLE profiles ADD COLUMN deleted_by TEXT;
    CREATE TABLE job_runs (job_name TEXT, last_success_at TEXT);
    INSERT INTO tournaments (id, name, tournament_type, category, subtype, access_type, player_hub_visibility) VALUES
      ('CCL-2027Q', 'CCL', 'Individuals', 'CCL', NULL, 'Friendly', 'Visible'),
      ('ETCOC-2026', 'ETCOC', 'Teams', 'ETCOC', 'Official', 'Official', 'Visible'),
      ('Friendly-Matches', 'Friendly', 'Teams', NULL, 'Friendly', 'Friendly', 'Visible'),
      ('WTCOC-2026', 'WTCOC', 'Teams', 'WTCOC', 'Official', 'Official', 'Hidden'),
      ('GG-Rivals-S1', 'Rivals', 'Individuals', 'Rivals', NULL, 'Friendly', 'Hidden');
    UPDATE tournaments SET standings_scoring = 'bounty_tpr', tpr_target_games = 17, tpr_smoothing = 0.8 WHERE id = 'GG-Rivals-S1';`);
  await ensureCategoriesSchema(setup);
  await applyTournamentClassificationPlan(setup, await buildTournamentClassificationPlan(setup), { backupDir: path.join(folder, "backups") });
  await close(setup);
  let child;
  t.after(async () => { if (child) await stop(child); await rm(folder, { recursive: true, force: true }); });

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const listener = createServer();
    await new Promise((resolve) => listener.listen(0, "127.0.0.1", resolve));
    const port = listener.address().port;
    await new Promise((resolve) => listener.close(resolve));
    let logs = "";
    child = spawn(process.execPath, ["src/server.js"], { cwd: root, env: { ...process.env,
      DB_PATH: filename, PORT: String(port), UPLOADS_DIR: path.join(folder, "uploads"), SESSION_SECRET: "cleanup-test-secret",
      GOOGLE_CLIENT_ID: "cleanup-test", GOOGLE_CLIENT_SECRET: "cleanup-test", GOOGLE_CALLBACK_URL: "http://127.0.0.1/auth/google/callback",
      NODE_ENV: "development", COOKIE_SAME_SITE: "lax", COOKIE_SECURE: "false",
    }, stdio: ["ignore", "pipe", "pipe"] });
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Startup timed out\n${logs}`)), 12000);
      const receive = (chunk) => { logs += chunk; if (logs.includes("Auth server running on port")) { clearTimeout(timer); resolve(); } };
      child.stdout.on("data", receive); child.stderr.on("data", receive);
      child.once("error", (error) => { clearTimeout(timer); reject(error); });
      child.once("exit", (code) => { clearTimeout(timer); reject(new Error(`Startup exited ${code}\n${logs}`)); });
    });
    const payloads = new Map();
    for (const endpoint of ["/health", "/tournaments", "/categories", "/public/main-page-matches", "/public/team-official-matches",
      "/public/team-official-matches/filters", "/public/friendly-matches", "/public/tournaments/GG-Rivals-S1"]) {
      const response = await fetch(`http://127.0.0.1:${port}${endpoint}`);
      assert.equal(response.status, 200, `${endpoint}\n${await response.clone().text()}\n${logs}`);
      payloads.set(endpoint, await response.json());
    }
    const visible = payloads.get("/tournaments").tournaments;
    assert.deepEqual(visible.map((row) => row.id), ["CCL-2027Q", "Friendly-Matches"]);
    assert.equal(visible.find((row) => row.id === "Friendly-Matches").category, "Friendly");
    assert.equal(visible.find((row) => row.id === "CCL-2027Q").type, "Individuals");
    assert.equal(payloads.get("/categories").categories.filter((row) => row.is_read_only).length, 11);
    assert.equal(payloads.get("/public/friendly-matches").tournament.id, "Friendly-Matches");
    assert.ok(!logs.includes("Failed to"), logs);
    await stop(child); child = null;
    const db = await openClassificationDatabase(filename, { readOnly: false });
    try {
      const [rivals] = await all(db, "SELECT type, category, standings_scoring, tpr_target_games, tpr_smoothing FROM tournaments WHERE id = 'GG-Rivals-S1'");
      assert.deepEqual(rivals, { type: "Individuals", category: "Rivals", standings_scoring: "bounty_tpr", tpr_target_games: 17, tpr_smoothing: 0.8 });
      if (attempt === 0) await applyTournamentCleanupPlan(db, await buildTournamentCleanupPlan(db), { backupDir: path.join(folder, "backups") });
      const info = await all(db, "PRAGMA table_info(tournaments)");
      assert.ok(info.every((column) => !LEGACY_TOURNAMENT_COLUMNS.includes(column.name)));
    } finally { await close(db); }
  }
});
