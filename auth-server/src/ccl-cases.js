// Cases about a CCL match (one duels row): a player reports a problem from their match list,
// or resolves a match in Error that has no games played (a no-show or an agreed reschedule).
import { getCclResultEditAccess } from "./ccl-result-edit.js";
import { CCL_MATCH_SOURCE_TYPE } from "./ccl-matches.js";
import { loadCclTournament } from "./ccl-routes.js";
import { getDuelSide } from "./duel-time-proposals.js";
import {
  caseAuditEntry,
  createOrAppendTournamentCase,
  formatSubmittedCase,
  normalizeCaseDetails,
} from "./tournament-cases.js";

export const CCL_RESOLVE_ACTIONS = new Set(["no_show", "reschedule"]);
// The Match problem category a player sends from each scenario: Report a problem, or Resolve → Other
// for a match in Error without games (no_show scenario → Match was not played).
export const CCL_REPORT_CATEGORIES = {
  scheduling: "scheduling",
  no_show: "not_played",
  incomplete: "incomplete",
  result_dispute: "result_dispute",
};
// Resolve of a partly played match: the match time may be moved by this much to find its games on BGA.
export const CCL_TIME_ADJUST_MAX_MINUTES = 60;

const text = (value) => String(value ?? "").trim();
const hasValue = (value) => value !== null && value !== undefined && text(value) !== "";

function dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
  });
}

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function dbRun(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) reject(error);
      else resolve(this);
    });
  });
}

function requestError(status, message) {
  return Object.assign(new Error(message), { status });
}

// A game with a score, a rank or a No show is a played game.
export function isCclGamePlayed(game) {
  return [game?.player_1_score, game?.player_2_score, game?.player_1_rank, game?.player_2_rank].some(hasValue)
    || text(game?.status).toLowerCase() === "no show";
}

// Which case the player may send about the match now, or null:
// - scheduling: planned match with both players and no agreed time, or an agreed time with a pending
//   change proposal (Report a problem next to the time button);
// - no_show: Error and no game played (Resolve: a no-show, an agreed reschedule or Other);
// - incomplete: Error with at least one game played (Resolve);
// - result_dispute: Done and the players' result editing window is over (Report a problem).
export function getCclCaseScenario({ match, games = [], playerId, now = new Date() }) {
  if (!getDuelSide(match, playerId)) return null;
  if (!text(match?.player_1_id) || !text(match?.player_2_id)) return null;
  const status = text(match?.status || "Planned").toLowerCase();
  if (status === "planned") {
    const changePending = text(match?.proposed_time_status).toLowerCase() === "change_pending";
    return !hasValue(match?.time_utc) || changePending ? "scheduling" : null;
  }
  if (status === "error") return (games || []).some(isCclGamePlayed) ? "incomplete" : "no_show";
  if (status === "done") {
    const access = getCclResultEditAccess({ duel: match, playerId, isAdmin: false, now });
    return access.expired ? "result_dispute" : null;
  }
  return null;
}

async function loadCclDuel(db, duelId) {
  return dbGet(
    db,
    `
      SELECT id, tournament_id, status, time_utc, player_1_id, player_2_id, deadline_utc,
        proposed_time_utc, proposed_time_by_player_id, proposed_time_status
      FROM duels
      WHERE deleted_at IS NULL
        AND source_type = ?
        AND upper(trim(id)) = upper(trim(?))
      LIMIT 1
    `,
    [CCL_MATCH_SOURCE_TYPE, text(duelId)]
  );
}

async function loadDuelGames(db, duelId) {
  return dbAll(
    db,
    `SELECT player_1_score, player_2_score, player_1_rank, player_2_rank, status
     FROM games WHERE deleted_at IS NULL AND duel_id = ?`,
    [duelId]
  );
}

async function inTransaction(db, work) {
  await dbRun(db, "BEGIN IMMEDIATE TRANSACTION");
  try {
    const result = await work();
    await dbRun(db, "COMMIT");
    return result;
  } catch (error) {
    await dbRun(db, "ROLLBACK").catch(() => {});
    throw error;
  }
}

const actorPlayerId = (user) => text(user?.player_id ?? user?.bga_id);
const actorUserId = (user) => (Number.isInteger(Number(user?.id)) && Number(user?.id) > 0 ? Number(user.id) : null);

// Loads the match, checks that the user plays in it and returns its case scenario.
async function loadCaseContext(db, user, duelId, now) {
  const playerId = actorPlayerId(user);
  if (!playerId) throw requestError(403, "Link your BGA profile to report a problem");
  const match = await loadCclDuel(db, duelId);
  if (!match) throw requestError(404, "Match not found");
  const tournament = await loadCclTournament(db, match.tournament_id);
  if (!tournament) throw requestError(404, "Match not found");
  if (!getDuelSide(match, playerId)) throw requestError(403, "Only the players of this match can report a problem");
  const games = await loadDuelGames(db, match.id);
  return { match, tournament, playerId, scenario: getCclCaseScenario({ match, games, playerId, now }) };
}

const DUEL_RESET_FIELDS = ["status", "time_utc", "proposed_time_utc", "proposed_time_by_player_id", "proposed_time_status"];

// Clears the agreed time of a match in Error so the players agree on a new one.
async function resetMatchTime(db, duelId, playerId) {
  const result = await dbRun(
    db,
    `
      UPDATE duels
      SET status = 'Planned',
        time_utc = NULL,
        proposed_time_utc = NULL,
        proposed_time_by_player_id = NULL,
        proposed_time_status = NULL,
        results_last_error = NULL,
        results_checked_at = NULL,
        updated_by = ?,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = ? AND deleted_at IS NULL AND status = 'Error'
    `,
    [playerId, duelId]
  );
  if (!Number(result?.changes || 0)) throw requestError(409, "The match has changed. Reload the page and try again");
}

function changedFields(before, after, fields) {
  const changes = {};
  fields.forEach((field) => {
    if ((before?.[field] ?? null) !== (after?.[field] ?? null)) {
      changes[field] = { old: before?.[field] ?? null, new: after?.[field] ?? null };
    }
  });
  return changes;
}

async function playerName(db, playerId) {
  const row = await dbGet(db, "SELECT bga_nickname, name FROM profiles WHERE id = ? LIMIT 1", [playerId]);
  return text(row?.bga_nickname) || text(row?.name) || playerId;
}

// "DD.MM.YYYY HH:MM UTC", or "" when the time is missing or invalid.
export function formatCclCaseTimeUtc(timeUtc) {
  const ts = Date.parse(text(timeUtc));
  if (!Number.isFinite(ts)) return "";
  const date = new Date(ts);
  const pad = (value) => String(value).padStart(2, "0");
  return `${pad(date.getUTCDate())}.${pad(date.getUTCMonth() + 1)}.${date.getUTCFullYear()} `
    + `${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())} UTC`;
}

// Default Details of a no-show case: the player and the scheduled match time (before the reset).
export function cclNoShowDetails(name, timeUtc) {
  const time = formatCclCaseTimeUtc(timeUtc);
  return `Player ${name} did not show up for the match${time ? ` scheduled for ${time}` : ""}.`;
}

// { value: ISO time } when `nextTimeUtc` moves the match time by 1..CCL_TIME_ADJUST_MAX_MINUTES minutes.
export function validateCclTimeAdjustment(currentTimeUtc, nextTimeUtc) {
  const current = Date.parse(text(currentTimeUtc));
  const next = Date.parse(text(nextTimeUtc));
  if (!Number.isFinite(next)) return { error: "Choose the corrected match time" };
  if (!Number.isFinite(current)) return { error: "The match has no time to correct" };
  const minutes = Math.abs(next - current) / 60000;
  if (minutes < 1) return { error: "Choose a time different from the current match time" };
  if (minutes > CCL_TIME_ADJUST_MAX_MINUTES) {
    return { error: `The match time can be moved by ${CCL_TIME_ADJUST_MAX_MINUTES} minutes at most` };
  }
  return { value: new Date(next).toISOString() };
}

const DUEL_RESULT_FIELDS = ["time_utc", "dw1", "dw2", "status", "results_last_error", "results_checked_at", "updated_by", "updated_at"];

export async function snapshotDuelResult(db, duelId) {
  const duel = await dbGet(db, `SELECT ${DUEL_RESULT_FIELDS.join(", ")} FROM duels WHERE id = ?`, [duelId]);
  const games = await dbAll(db, "SELECT * FROM games WHERE duel_id = ?", [duelId]);
  return { duel, games };
}

// Puts the duel and its games back as they were before a failed time adjustment: games the updater
// added are removed (with their queued replays), changed or removed games get their saved values back.
export async function restoreDuelResult(db, duelId, snapshot) {
  await inTransaction(db, async () => {
    await dbRun(
      db,
      `UPDATE duels SET ${DUEL_RESULT_FIELDS.map((field) => `${field} = ?`).join(", ")} WHERE id = ?`,
      [...DUEL_RESULT_FIELDS.map((field) => snapshot.duel?.[field] ?? null), duelId]
    );
    const keptIds = snapshot.games.map((game) => game.id);
    const current = await dbAll(db, "SELECT id FROM games WHERE duel_id = ?", [duelId]);
    const addedIds = current.map((game) => game.id).filter((id) => !keptIds.includes(id));
    if (addedIds.length) {
      const marks = addedIds.map(() => "?").join(", ");
      await dbRun(db, `DELETE FROM game_replays WHERE game_id IN (${marks})`, addedIds).catch(() => {});
      await dbRun(db, `DELETE FROM games WHERE id IN (${marks})`, addedIds);
    }
    for (const game of snapshot.games) {
      const columns = Object.keys(game);
      const exists = await dbGet(db, "SELECT id FROM games WHERE id = ?", [game.id]);
      if (exists) {
        const updated = columns.filter((column) => column !== "id");
        await dbRun(
          db,
          `UPDATE games SET ${updated.map((column) => `${column} = ?`).join(", ")} WHERE id = ?`,
          [...updated.map((column) => game[column]), game.id]
        );
      } else {
        await dbRun(
          db,
          `INSERT INTO games (${columns.join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`,
          columns.map((column) => game[column])
        );
      }
    }
  });
}

export function registerCclCaseRoutes(app, {
  db,
  requireAuthenticated,
  requireTrustedJsonRequest = (_req, _res, next) => next(),
  getAuditActor = () => ({}),
  logAuditEvent = null,
  logger = console,
  now = () => new Date(),
  // Fetches the duel's games from BGA for its time_utc (run_update_matches.py --duel-id).
  runDuelResultsUpdate = null,
}) {
  if (!app || !db || typeof requireAuthenticated !== "function") {
    throw new Error("app, db and requireAuthenticated are required");
  }
  const adjustingDuels = new Set();

  const audit = (req, entry) => {
    if (typeof logAuditEvent === "function") logAuditEvent({ ...getAuditActor(req.user), ...entry }, () => {});
  };

  const handle = (failureMessage, handler) => async (req, res) => {
    try {
      return await handler(req, res);
    } catch (error) {
      const status = Number(error?.status) || 500;
      if (status >= 500) logger.error?.(failureMessage, error);
      return res.status(status).json({ ok: false, message: status >= 500 ? failureMessage : error.message });
    }
  };

  // Report a problem (scenarios scheduling, incomplete, result_dispute) or Resolve → Other for a match
  // in Error without games (Match problem / Match was not played).
  app.post(
    "/ccl/matches/:id/cases",
    requireAuthenticated,
    requireTrustedJsonRequest,
    handle("Failed to send the case", async (req, res) => {
      const details = normalizeCaseDetails(req.body?.details);
      if (details.error) throw requestError(400, details.error);
      const created = await inTransaction(db, async () => {
        const { match, playerId, scenario } = await loadCaseContext(db, req.user, req.params.id, now());
        const expected = CCL_REPORT_CATEGORIES[scenario];
        if (!expected) throw requestError(409, "A problem cannot be reported for this match now");
        const category = text(req.body?.category).toLowerCase();
        if (category && category !== expected) throw requestError(409, "The match has changed. Reload the page and try again");
        return createOrAppendTournamentCase(db, {
          case_type: "problem",
          category: expected,
          details: details.value,
          submitted_by_user_id: actorUserId(req.user),
          submitted_by_player_id: playerId,
          duel_id: match.id,
          tournament_id: match.tournament_id,
        });
      });
      audit(req, caseAuditEntry(created, { tournament_id: created.case?.tournament_id, duel_id: created.case?.duel_id }));
      return res.status(201).json({ ok: true, case: formatSubmittedCase(created.case), appended: created.appended });
    })
  );

  // Resolve a match in Error with no games played: a player did not show up (Complaint / No-show
  // about that player + new time), or the players agreed to reschedule (new time, no case).
  // Other goes to POST /ccl/matches/:id/cases (Match problem / Match was not played).
  app.post(
    "/ccl/matches/:id/resolve",
    requireAuthenticated,
    requireTrustedJsonRequest,
    handle("Failed to resolve the match", async (req, res) => {
      const action = text(req.body?.action).toLowerCase();
      if (!CCL_RESOLVE_ACTIONS.has(action)) throw requestError(400, "Choose what happened with the match");
      const result = await inTransaction(db, async () => {
        const { match, playerId, scenario } = await loadCaseContext(db, req.user, req.params.id, now());
        if (scenario !== "no_show") throw requestError(409, "The match has changed. Reload the page and try again");
        let created = null;
        if (action === "no_show") {
          const noShowPlayerId = text(req.body?.no_show_player_id);
          if (![text(match.player_1_id), text(match.player_2_id)].includes(noShowPlayerId)) {
            throw requestError(400, "Choose the player who did not show up");
          }
          // The player's Details (prefilled in the modal), or the default no-show text.
          const submitted = text(req.body?.details) ? normalizeCaseDetails(req.body.details) : null;
          if (submitted?.error) throw requestError(400, submitted.error);
          created = await createOrAppendTournamentCase(db, {
            case_type: "complaint",
            category: "no_show",
            details: submitted?.value || cclNoShowDetails(await playerName(db, noShowPlayerId), match.time_utc),
            submitted_by_user_id: actorUserId(req.user),
            submitted_by_player_id: playerId,
            reported_player_id: noShowPlayerId,
            duel_id: match.id,
            tournament_id: match.tournament_id,
          });
        }
        await resetMatchTime(db, match.id, playerId);
        return { before: match, after: await loadCclDuel(db, match.id), created };
      });

      audit(req, {
        event_type: "ccl.match.issue_resolved",
        entity_type: "duel",
        action: "update",
        record_id: result.after.id,
        changes: changedFields(result.before, result.after, DUEL_RESET_FIELDS),
        metadata: { tournament_id: result.after.tournament_id, action, case_id: result.created?.case?.id ?? null },
      });
      if (result.created) {
        audit(req, caseAuditEntry(result.created, { tournament_id: result.after.tournament_id, duel_id: result.after.id }));
      }
      return res.json({
        ok: true,
        case: result.created ? formatSubmittedCase(result.created.case) : null,
        appended: !!result.created?.appended,
      });
    })
  );

  // Resolve of a partly played match: move the match time by up to an hour and fetch the games from
  // BGA again. A result without a winner puts the match and its games back as they were.
  app.post(
    "/ccl/matches/:id/adjust-time",
    requireAuthenticated,
    requireTrustedJsonRequest,
    handle("Failed to get the match results", async (req, res) => {
      if (typeof runDuelResultsUpdate !== "function") throw requestError(503, "Getting results is not available now");
      const { match, playerId, scenario } = await loadCaseContext(db, req.user, req.params.id, now());
      if (scenario !== "incomplete") throw requestError(409, "The match has changed. Reload the page and try again");
      const adjustment = validateCclTimeAdjustment(match.time_utc, req.body?.time_utc);
      if (adjustment.error) throw requestError(400, adjustment.error);
      if (adjustingDuels.has(match.id)) throw requestError(409, "Results of this match are being checked. Try again in a minute");

      adjustingDuels.add(match.id);
      try {
        const snapshot = await snapshotDuelResult(db, match.id);
        const moved = await dbRun(
          db,
          "UPDATE duels SET time_utc = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'Error' AND deleted_at IS NULL",
          [adjustment.value, playerId, match.id]
        );
        if (!Number(moved?.changes || 0)) throw requestError(409, "The match has changed. Reload the page and try again");
        try {
          await runDuelResultsUpdate(match.id);
        } catch (error) {
          logger.error?.(`Failed to get results of CCL match ${match.id}`, error);
        }
        const after = await dbGet(db, "SELECT time_utc, status, dw1, dw2 FROM duels WHERE id = ?", [match.id]);
        if (text(after?.status).toLowerCase() !== "done") {
          await restoreDuelResult(db, match.id, snapshot);
          return res.status(409).json({
            ok: false,
            code: "still_incomplete",
            message: "This match time does not give a correct result either. Nothing was changed.",
          });
        }
        audit(req, {
          event_type: "ccl.match.time_adjusted",
          entity_type: "duel",
          action: "update",
          record_id: match.id,
          changes: changedFields({ ...snapshot.duel }, after, ["time_utc", "status", "dw1", "dw2"]),
          metadata: { tournament_id: match.tournament_id },
        });
        return res.json({ ok: true, match: { id: match.id, time_utc: after.time_utc, status: after.status, dw1: after.dw1, dw2: after.dw2 } });
      } finally {
        adjustingDuels.delete(match.id);
      }
    })
  );
}
