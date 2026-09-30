import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import sqlite3 from "sqlite3";

import {
  calculateLineupDeadlineUtc,
  ensureSecretLineupsSchema,
  isBlindLineupType,
  softDeleteMatchLineupDataInTransaction,
} from "./secret-lineups.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const authServerRoot = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(authServerRoot, ".env") });

function readArgument(name) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || "").trim() : "";
}

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
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

function close(db) {
  return new Promise((resolve, reject) => {
    db.close((error) => (error ? reject(error) : resolve()));
  });
}

const matchId = readArgument("--match-id");
const apply = process.argv.includes("--apply");
const configuredDbPath = readArgument("--db-path")
  || process.env.DB_PATH
  || process.env.AUTH_SQLITE_PATH
  || "./data/auth.sqlite";
const dbPath = path.isAbsolute(configuredDbPath)
  ? configuredDbPath
  : path.resolve(authServerRoot, configuredDbPath);

if (!matchId) {
  console.error("Usage: node src/reset-match-lineups-after-time-change.js --match-id MATCH_ID [--db-path PATH] [--apply]");
  process.exitCode = 1;
} else {
  const db = new sqlite3.Database(dbPath);
  let transactionOpen = false;
  try {
    if (apply) {
      await run(db, "BEGIN IMMEDIATE TRANSACTION");
      transactionOpen = true;
      await ensureSecretLineupsSchema(db);
    }
    const submissionColumns = new Set(
      (await all(db, "PRAGMA table_info(match_lineup_submissions)"))
        .map((column) => String(column?.name || "").trim())
    );
    const entryColumns = new Set(
      (await all(db, "PRAGMA table_info(match_lineup_entries)"))
        .map((column) => String(column?.name || "").trim())
    );
    const activeSubmissionFilter = submissionColumns.has("deleted_at") ? "AND deleted_at IS NULL" : "";
    const activeEntryFilter = entryColumns.has("deleted_at") ? "AND deleted_at IS NULL" : "";
    const match = await get(
      db,
      `
        SELECT
          id,
          time_utc,
          lineup_type,
          lineup_deadline_h,
          lineup_deadline_utc,
          lineups_published_at,
          team_1_lineup_added,
          team_2_lineup_added,
          status
        FROM matches
        WHERE trim(COALESCE(id, '')) = trim(?)
          AND deleted_at IS NULL
        LIMIT 1
      `,
      [matchId]
    );
    if (!match) throw new Error(`Match ${matchId} was not found`);

    const storedDeadlineHours = Number(match.lineup_deadline_h);
    const deadlineHours = isBlindLineupType(match.lineup_type)
      ? ([6, 12, 24, 48].includes(storedDeadlineHours) ? storedDeadlineHours : 24)
      : null;
    const lineupDeadlineUtc = calculateLineupDeadlineUtc(match.time_utc, deadlineHours);
    if (isBlindLineupType(match.lineup_type) && !lineupDeadlineUtc) {
      throw new Error(`Match ${matchId} has no valid time_utc for recalculating its lineup deadline`);
    }

    const counts = await get(
      db,
      `
        SELECT
          (SELECT COUNT(*) FROM duels WHERE trim(COALESCE(match_id, '')) = trim(?) AND deleted_at IS NULL) AS duels,
          (SELECT COUNT(*) FROM match_lineup_entries WHERE trim(COALESCE(match_id, '')) = trim(?) ${activeEntryFilter}) AS entries,
          (SELECT COUNT(*) FROM match_lineup_submissions WHERE trim(COALESCE(match_id, '')) = trim(?) ${activeSubmissionFilter}) AS submissions
      `,
      [matchId, matchId, matchId]
    );
    const duelIds = (await all(
      db,
      "SELECT id FROM duels WHERE trim(COALESCE(match_id, '')) = trim(?) AND deleted_at IS NULL",
      [matchId]
    )).map((row) => String(row?.id || "").trim()).filter(Boolean);
    const gamesTable = await get(db, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'games' LIMIT 1");
    const streamsTable = await get(db, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'streams' LIMIT 1");
    const games = duelIds.length && gamesTable
      ? await get(
          db,
          `SELECT COUNT(*) AS count FROM games WHERE trim(COALESCE(duel_id, '')) IN (${duelIds.map(() => "?").join(", ")}) AND deleted_at IS NULL`,
          duelIds
        )
      : { count: 0 };
    const streams = duelIds.length && streamsTable
      ? await get(
          db,
          `SELECT COUNT(*) AS count FROM streams WHERE lower(trim(COALESCE(entity_type, ''))) = 'duel' AND trim(COALESCE(entity_id, '')) IN (${duelIds.map(() => "?").join(", ")}) AND deleted_at IS NULL`,
          duelIds
        )
      : { count: 0 };

    const preview = {
      mode: apply ? "apply" : "dry-run",
      db_path: dbPath,
      match_before: match,
      lineup_deadline_utc_after: lineupDeadlineUtc,
      active_records_to_soft_delete: {
        duels: Number(counts?.duels) || 0,
        entries: Number(counts?.entries) || 0,
        submissions: Number(counts?.submissions) || 0,
        games: Number(games?.count) || 0,
        streams: Number(streams?.count) || 0,
      },
    };
    console.log(JSON.stringify(preview, null, 2));

    if (apply) {
      const reset = await softDeleteMatchLineupDataInTransaction(db, matchId, "manual-time-change-reset");
      await run(
        db,
        `
          UPDATE matches
          SET
            lineup_deadline_h = ?,
            lineup_deadline_utc = ?,
            lineups_published_at = NULL,
            team_1_lineup_added = 0,
            team_2_lineup_added = 0,
            dw1 = NULL,
            dw2 = NULL,
            gw1 = NULL,
            gw2 = NULL,
            rating = NULL,
            gg_rating = NULL,
            updated_by = 'manual-time-change-reset',
            updated_at = CURRENT_TIMESTAMP
          WHERE trim(COALESCE(id, '')) = trim(?)
            AND deleted_at IS NULL
        `,
        [deadlineHours, lineupDeadlineUtc, matchId]
      );
      await run(db, "COMMIT");
      transactionOpen = false;
      console.log(JSON.stringify({ ok: true, match_id: matchId, lineup_deadline_utc: lineupDeadlineUtc, reset }, null, 2));
    }
  } catch (error) {
    if (transactionOpen) await run(db, "ROLLBACK").catch(() => {});
    console.error(error?.stack || error?.message || String(error));
    process.exitCode = 1;
  } finally {
    await close(db);
  }
}
