// Cases about a CCL match (one duels row): a player reports a problem from their match list,
// or resolves a match in Error that has no games played.
import { getCclResultEditAccess } from "./ccl-result-edit.js";
import { CCL_MATCH_SOURCE_TYPE } from "./ccl-matches.js";
import { loadCclTournament } from "./ccl-routes.js";
import { getDuelSide } from "./duel-time-proposals.js";
import {
  MATCH_CASE_CHOICES,
  caseAuditEntry,
  createOrAppendTournamentCase,
  formatSubmittedCase,
  normalizeCaseChoice,
  normalizeCaseDetails,
} from "./tournament-cases.js";

export const CCL_RESOLVE_ACTIONS = new Set(["no_show", "reschedule"]);

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
// - scheduling: planned match with both players and no agreed time (Report a problem next to the time button);
// - not_played: Error and no game played (Resolve);
// - incomplete: Error with at least one game played (Report a problem);
// - result_dispute: Done and the players' result editing window is over (Report a problem).
export function getCclCaseScenario({ match, games = [], playerId, now = new Date() }) {
  if (!getDuelSide(match, playerId)) return null;
  if (!text(match?.player_1_id) || !text(match?.player_2_id)) return null;
  const status = text(match?.status || "Planned").toLowerCase();
  if (status === "planned") return hasValue(match?.time_utc) ? null : "scheduling";
  if (status === "error") return (games || []).some(isCclGamePlayed) ? "incomplete" : "not_played";
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

export function registerCclCaseRoutes(app, {
  db,
  requireAuthenticated,
  requireTrustedJsonRequest = (_req, _res, next) => next(),
  getAuditActor = () => ({}),
  logAuditEvent = null,
  logger = console,
  now = () => new Date(),
}) {
  if (!app || !db || typeof requireAuthenticated !== "function") {
    throw new Error("app, db and requireAuthenticated are required");
  }

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

  // Report a problem (scenarios scheduling, incomplete, result_dispute) or Resolve → Other
  // (any match case type and category) for a match in Error without games.
  app.post(
    "/ccl/matches/:id/cases",
    requireAuthenticated,
    requireTrustedJsonRequest,
    handle("Failed to send the case", async (req, res) => {
      const details = normalizeCaseDetails(req.body?.details);
      if (details.error) throw requestError(400, details.error);
      const created = await inTransaction(db, async () => {
        const { match, playerId, scenario } = await loadCaseContext(db, req.user, req.params.id, now());
        if (!scenario) throw requestError(409, "A problem cannot be reported for this match now");
        let choice;
        if (scenario === "not_played") {
          choice = normalizeCaseChoice(req.body, MATCH_CASE_CHOICES);
          if (choice.error) throw requestError(400, choice.error);
        } else {
          const category = text(req.body?.category).toLowerCase();
          if (category && category !== scenario) throw requestError(409, "The match has changed. Reload the page and try again");
          choice = { case_type: "problem", category: scenario };
        }
        return createOrAppendTournamentCase(db, {
          ...choice,
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

  // Resolve a match in Error with no games played: a player did not show up (case + new time),
  // or the players agreed to reschedule (new time, no case).
  app.post(
    "/ccl/matches/:id/resolve",
    requireAuthenticated,
    requireTrustedJsonRequest,
    handle("Failed to resolve the match", async (req, res) => {
      const action = text(req.body?.action).toLowerCase();
      if (!CCL_RESOLVE_ACTIONS.has(action)) throw requestError(400, "Choose what happened with the match");
      const result = await inTransaction(db, async () => {
        const { match, playerId, scenario } = await loadCaseContext(db, req.user, req.params.id, now());
        if (scenario !== "not_played") throw requestError(409, "The match has changed. Reload the page and try again");
        let created = null;
        if (action === "no_show") {
          const noShowPlayerId = text(req.body?.no_show_player_id);
          if (![text(match.player_1_id), text(match.player_2_id)].includes(noShowPlayerId)) {
            throw requestError(400, "Choose the player who did not show up");
          }
          created = await createOrAppendTournamentCase(db, {
            case_type: "problem",
            category: "not_played",
            details: `Player ${await playerName(db, noShowPlayerId)} did not show up for the match.`,
            submitted_by_user_id: actorUserId(req.user),
            submitted_by_player_id: playerId,
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
}
