// Moves players through the CCL Qualifiers brackets.
//
// The bracket is recomputed from the draw and the results every time (idempotent sync), because
// results arrive from several places: the BGA updater (Python, writes duels directly), the result
// editor and admin results. A sync never rewrites a match that has started (a time, a score or a
// later status): when such a match should now hold other players, it is reported as a conflict.
//
// Bye: a missing Unseeded player. A match with a Bye is not played: the other player goes on
// (status Cancelled, cancellation_reason 'bye'), and the Bye drops to the lower bracket like a loser.
// Group Final Reset: played only when the lower-bracket finalist (player 2) wins the Group Final;
// otherwise it is Cancelled with cancellation_reason 'not_needed'.

import { loadCclTournament, writeCclQualificationResult } from "./ccl-routes.js";
import { buildBracketLinks, parseSlotSource } from "./double-elimination.js";
import { hasTournamentAdminAccess } from "./tournament-admin-access.js";

export const BYE = "__BYE__";
const STARTED_STATUSES = new Set(["Done", "In progress", "Error"]);
const text = (value) => String(value ?? "").trim();
const idKey = (value) => text(value).toUpperCase();

// A match has started when it has a time, a score or a later status; a sync then keeps its players.
export function isStartedMatch(match) {
  return !!text(match.time_utc)
    || (match.dw1 !== null && match.dw1 !== undefined)
    || (match.dw2 !== null && match.dw2 !== undefined)
    || STARTED_STATUSES.has(text(match.status))
    || text(match.proposed_time_status) === "accepted";
}

const isReal = (player) => !!player && player !== BYE;

// Pure. matches: duels of one group; positions: Map(draw position → player id) once drawn.
// Returns the expected state of every match, the outcomes and who leaves the group.
export function resolveBracket({ matches, positions, drawn, advance = [] }) {
  const sorted = [...matches].sort((a, b) => a.bracket_no - b.bracket_no);
  const byNo = new Map(sorted.map((match) => [match.bracket_no, match]));
  const links = buildBracketLinks(sorted.map((match) => ({
    ...match,
    is_conditional: Number(match.is_conditional) === 1 || match.is_conditional === true,
    depends_on: match.bracket_no - 1,
  })));
  const outcomes = new Map();
  const expected = new Map();
  const conflicts = [];

  const sideOf = (source) => {
    const parsed = parseSlotSource(source);
    if (!parsed) return null;
    if (parsed.kind === "draw") return drawn ? positions.get(parsed.ref) || BYE : null;
    const outcome = outcomes.get(parsed.ref);
    if (!outcome) return null;
    return parsed.kind === "winner" ? outcome.winner : outcome.loser;
  };

  for (const match of sorted) {
    const conditional = Number(match.is_conditional) === 1 || match.is_conditional === true;
    let state;
    if (conditional) {
      // The reset follows the match right before it (the Group Final).
      const final = outcomes.get(match.bracket_no - 1);
      const finalMatch = byNo.get(match.bracket_no - 1);
      if (!final) state = { player_1_id: null, player_2_id: null, kind: "pending" };
      else if (final.winner === expected.get(finalMatch.bracket_no).player_1_id) state = { player_1_id: null, player_2_id: null, kind: "not_needed" };
    }
    if (!state) {
      const p1 = sideOf(match.p1_source);
      const p2 = sideOf(match.p2_source);
      if (p1 === null || p2 === null) state = { player_1_id: p1, player_2_id: p2, kind: "pending" };
      else if (p1 === BYE || p2 === BYE) state = { player_1_id: p1, player_2_id: p2, kind: "bye" };
      else state = { player_1_id: p1, player_2_id: p2, kind: "play" };
    }
    expected.set(match.bracket_no, state);

    const samePlayers = text(match.player_1_id) === text(isReal(state.player_1_id) ? state.player_1_id : "")
      && text(match.player_2_id) === text(isReal(state.player_2_id) ? state.player_2_id : "");
    if (isStartedMatch(match) && (!samePlayers || state.kind !== "play")) {
      conflicts.push({ duel_id: match.id, bracket_no: match.bracket_no, expected: state });
      continue; // its outcome stays unknown, so nothing after it moves
    }
    if (state.kind === "bye") {
      const winner = isReal(state.player_1_id) ? state.player_1_id : isReal(state.player_2_id) ? state.player_2_id : BYE;
      outcomes.set(match.bracket_no, { winner, loser: BYE, bye: true });
    } else if (state.kind === "play" && text(match.status) === "Done" && samePlayers) {
      const dw1 = Number(match.dw1);
      const dw2 = Number(match.dw2);
      if (Number.isFinite(dw1) && Number.isFinite(dw2) && dw1 !== dw2) {
        outcomes.set(match.bracket_no, dw1 > dw2
          ? { winner: state.player_1_id, loser: state.player_2_id }
          : { winner: state.player_2_id, loser: state.player_1_id });
      }
    } else if (state.kind === "not_needed") {
      outcomes.set(match.bracket_no, { not_needed: true });
    }
  }

  // Players leaving the group for the League Stage, in finishing order.
  const qualified = [];
  advance.forEach(({ source, label }, index) => {
    const match = /^winner:([\d|]+)$/.exec(text(source));
    if (!match) return;
    // "winner:14|15": the winner of the last of these matches that was needed.
    const numbers = match[1].split("|").map(Number);
    const played = numbers.map((no) => outcomes.get(no)).filter((outcome) => outcome && !outcome.not_needed);
    const decided = numbers.every((no) => outcomes.has(no));
    const winner = decided ? played.at(-1)?.winner : null;
    if (isReal(winner)) qualified.push({ player_id: winner, label, place: index + 1 });
  });

  // A loser is out when the loss sends them nowhere: lower-bracket matches, and a Group Final
  // lost by the lower-bracket finalist (the reset is then not needed).
  const eliminated = [];
  for (const match of sorted) {
    const outcome = outcomes.get(match.bracket_no);
    if (!outcome || outcome.not_needed || !isReal(outcome.loser)) continue;
    const nextLose = links.get(match.bracket_no)?.next_lose;
    const next = nextLose ? outcomes.get(nextLose) : null;
    const sameNextForBoth = nextLose && links.get(match.bracket_no)?.next_win === nextLose;
    if (!nextLose || (sameNextForBoth && next?.not_needed)) eliminated.push(outcome.loser);
  }

  return { expected, outcomes, conflicts, qualified, eliminated, complete: advance.length > 0 && qualified.length === advance.length };
}

// ---------------------------------------------------------------------------------------------
// Database

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
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

async function loadBracketStages(db, tournamentId) {
  return dbAll(
    db,
    `SELECT * FROM tournament_stages
     WHERE upper(trim(tournament_id)) = upper(trim(?)) AND format = 'double_elimination'
     ORDER BY sort_order`,
    [tournamentId]
  );
}

// What a sync would change in one tournament, without writing anything.
export async function planCclBracketSync(db, tournamentId) {
  const plan = { tournament_id: tournamentId, updates: [], conflicts: [], results: [], stages: [] };
  for (const stage of await loadBracketStages(db, tournamentId)) {
    const settings = JSON.parse(stage.settings_json || "{}");
    const drawn = text(stage.status) !== "structure";
    const groups = await dbAll(
      db,
      "SELECT id, group_key FROM tournament_groups WHERE upper(trim(stage_id)) = upper(trim(?)) ORDER BY sort_order",
      [stage.id]
    );
    let anyResult = false;
    let complete = groups.length > 0;
    for (const group of groups) {
      const matches = await dbAll(
        db,
        `SELECT id, bracket_no, p1_source, p2_source, is_conditional, player_1_id, player_2_id, status, dw1, dw2,
                time_utc, proposed_time_status, cancellation_reason
         FROM duels WHERE upper(trim(group_id)) = upper(trim(?)) AND deleted_at IS NULL`,
        [group.id]
      );
      const entries = await dbAll(
        db,
        `SELECT slot_id, player_id, draw_position FROM ccl_stage_entries
         WHERE upper(trim(group_id)) = upper(trim(?)) AND draw_position IS NOT NULL`,
        [group.id]
      );
      const positions = new Map(entries.map((entry) => [Number(entry.draw_position), text(entry.player_id)]));
      const resolved = resolveBracket({ matches, positions, drawn, advance: settings.advance || [] });
      resolved.conflicts.forEach((conflict) => plan.conflicts.push({ ...conflict, stage: stage.stage_key, group: group.group_key }));
      if (matches.some((match) => text(match.status) === "Done")) anyResult = true;
      if (!resolved.complete) complete = false;

      for (const match of matches) {
        const state = resolved.expected.get(match.bracket_no);
        if (!state || resolved.conflicts.some((conflict) => conflict.duel_id === match.id)) continue;
        const player1 = isReal(state.player_1_id) ? state.player_1_id : null;
        const player2 = isReal(state.player_2_id) ? state.player_2_id : null;
        let status;
        let reason = null;
        if (state.kind === "bye" || state.kind === "not_needed") {
          status = "Cancelled";
          reason = state.kind;
        } else if (state.kind === "pending" || !player1 || !player2) {
          status = "Draft";
        } else {
          // Both players known: keep a later status (Done, In progress, ...) of a started match.
          status = isStartedMatch(match) ? text(match.status) : "Planned";
        }
        const changed = text(match.player_1_id) !== text(player1)
          || text(match.player_2_id) !== text(player2)
          || text(match.status) !== status
          || (status === "Cancelled" && text(match.cancellation_reason) !== reason);
        if (changed) {
          plan.updates.push({
            duel_id: match.id,
            stage: stage.stage_key,
            group: group.group_key,
            bracket_no: match.bracket_no,
            from: { player_1_id: match.player_1_id || null, player_2_id: match.player_2_id || null, status: match.status || null },
            to: { player_1_id: player1, player_2_id: player2, status, cancellation_reason: reason },
            players_changed: text(match.player_1_id) !== text(player1) || text(match.player_2_id) !== text(player2),
          });
        }
      }
      for (const { player_id: playerId, label } of resolved.qualified) {
        plan.results.push({ stage: stage.stage_key, group: group.group_key, player_id: playerId, league_status: "qualified", label });
      }
      for (const playerId of resolved.eliminated) {
        plan.results.push({ stage: stage.stage_key, group: group.group_key, player_id: playerId, league_status: "eliminated" });
      }
    }
    if (drawn) {
      const status = complete ? "done" : anyResult ? "in_progress" : "drawn";
      if (status !== text(stage.status)) plan.stages.push({ id: stage.id, stage: stage.stage_key, from: stage.status, to: status });
    }
  }
  return plan;
}

// The qualification result of a slot, written only while the slot has none: an admin's own
// result (PATCH /ccl/slots/:id/result) is never overwritten by a sync.
async function loadPendingResults(db, tournamentId, results) {
  const pending = [];
  for (const result of results) {
    const slot = await dbGet(
      db,
      `SELECT id, league_status FROM tournament_players
       WHERE upper(trim(tournament_id)) = upper(trim(?)) AND trim(player_id) = ? AND entry_stage = ? LIMIT 1`,
      [tournamentId, text(result.player_id), result.stage]
    );
    if (slot && !text(slot.league_status)) pending.push({ ...result, slot_id: slot.id });
  }
  return pending;
}

// Writes a plan (inside the caller's transaction).
async function applyPlan(db, plan, { actorId = null } = {}) {
  for (const update of plan.updates) {
    const { to } = update;
    await dbRun(
      db,
      `UPDATE duels
       SET player_1_id = ?, player_2_id = ?, status = ?, cancellation_reason = ?,
           proposed_time_utc = CASE WHEN ? THEN NULL ELSE proposed_time_utc END,
           proposed_time_by_player_id = CASE WHEN ? THEN NULL ELSE proposed_time_by_player_id END,
           proposed_time_status = CASE WHEN ? THEN NULL ELSE proposed_time_status END,
           updated_by = COALESCE(?, updated_by), updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [
        to.player_1_id, to.player_2_id, to.status, to.status === "Cancelled" ? to.cancellation_reason : null,
        update.players_changed ? 1 : 0, update.players_changed ? 1 : 0, update.players_changed ? 1 : 0,
        actorId, update.duel_id,
      ]
    );
  }
  for (const stage of plan.stages) {
    await dbRun(db, "UPDATE tournament_stages SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", [stage.to, stage.id]);
  }
  const results = await loadPendingResults(db, plan.tournament_id, plan.results);
  for (const result of results) {
    await writeCclQualificationResult(db, {
      tournamentId: plan.tournament_id,
      slotId: result.slot_id,
      leagueStatus: result.league_status,
      qualifiedBy: result.league_status === "qualified"
        ? { qualified_by_name: `${result.stage}, Group ${result.group} ${result.label}`, qualified_by_link: null }
        : null,
      actorId,
    });
  }
  return { ...plan, results };
}

const hasChanges = (plan) => plan.updates.length > 0 || plan.stages.length > 0;

// Sync inside an open transaction (the draw and admin results call it there).
export async function syncCclBracketsInTransaction(db, tournamentId, options = {}) {
  const plan = await planCclBracketSync(db, tournamentId);
  const pendingResults = await loadPendingResults(db, tournamentId, plan.results);
  if (!hasChanges(plan) && !pendingResults.length) return { ...plan, results: [] };
  return applyPlan(db, plan, options);
}

// Standalone sync: reads first and opens a transaction only when something has to change.
export async function syncCclBrackets(db, tournamentId, options = {}) {
  const preview = await planCclBracketSync(db, tournamentId);
  const pendingResults = await loadPendingResults(db, tournamentId, preview.results);
  if (!hasChanges(preview) && !pendingResults.length) return { ...preview, results: [], changed: false };
  await dbRun(db, "BEGIN IMMEDIATE TRANSACTION");
  try {
    const applied = await syncCclBracketsInTransaction(db, tournamentId, options);
    await dbRun(db, "COMMIT");
    return { ...applied, changed: true };
  } catch (error) {
    await dbRun(db, "ROLLBACK").catch(() => {});
    throw error;
  }
}

// Tournaments with a drawn path that is not finished yet.
export async function listActiveCclBracketTournaments(db) {
  const rows = await dbAll(
    db,
    `SELECT DISTINCT tournament_id FROM tournament_stages
     WHERE format = 'double_elimination' AND status IN ('drawn', 'in_progress')`
  );
  return rows.map((row) => row.tournament_id);
}

// Background sync for results the BGA updater writes straight into duels.
export function startCclBracketSync(db, {
  intervalMs = 60000,
  logAuditEvent = null,
  logger = console,
} = {}) {
  let running = false;
  const tick = async () => {
    if (running) return;
    running = true;
    try {
      for (const tournamentId of await listActiveCclBracketTournaments(db)) {
        const result = await syncCclBrackets(db, tournamentId);
        if (result.changed && typeof logAuditEvent === "function") {
          logAuditEvent(bracketAuditEntry(result, { actor_user_id: null }), () => {});
        }
        if (result.conflicts.length) logger.warn?.(`CCL bracket conflicts in ${tournamentId}`, result.conflicts);
      }
    } catch (error) {
      logger.error?.("CCL bracket sync failed", error);
    } finally {
      running = false;
    }
  };
  const timer = setInterval(tick, intervalMs);
  timer.unref?.();
  return { tick, stop: () => clearInterval(timer) };
}

export function bracketAuditEntry(result, actor = {}) {
  return {
    ...actor,
    event_type: "ccl.bracket.advanced",
    entity_type: "tournament",
    action: "update",
    record_id: result.tournament_id,
    changes: {},
    metadata: {
      tournament_id: result.tournament_id,
      updates: result.updates.map(({ stage, group, bracket_no: no, to }) => ({ stage, group, no, ...to })),
      stages: result.stages,
      results: result.results.map(({ stage, group, player_id: playerId, league_status: status }) => ({ stage, group, player_id: playerId, status })),
      conflicts: result.conflicts.map(({ stage, group, bracket_no: no }) => ({ stage, group, no })),
    },
  };
}

// ---------------------------------------------------------------------------------------------
// Routes

function requestError(status, message, data = undefined) {
  return Object.assign(new Error(message), { status, data });
}

// "2:1" in the duel's format: the winner has exactly games_to_win, the loser fewer.
async function validateScore(db, duel, dw1, dw2) {
  const format = await dbGet(db, "SELECT games_to_win FROM duel_formats WHERE lower(trim(format)) = lower(trim(?)) LIMIT 1", [duel.duel_format]);
  const gamesToWin = Math.max(1, Number(format?.games_to_win) || 1);
  const valid = [dw1, dw2].every((value) => Number.isInteger(value) && value >= 0)
    && Math.max(dw1, dw2) === gamesToWin && Math.min(dw1, dw2) < gamesToWin;
  if (!valid) throw requestError(400, `The score must be won ${gamesToWin} games to fewer (${duel.duel_format || "format"})`);
}

export function registerCclBracketRoutes(app, {
  db,
  requireAuthenticated,
  getAuditActor = () => ({}),
  logAuditEvent = null,
  logger = console,
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
      return res.status(status).json({
        ok: false,
        message: status >= 500 ? failureMessage : error.message,
        ...(status < 500 && error.data ? error.data : {}),
      });
    }
  };
  const actorId = (user) => text(user?.player_id ?? user?.bga_id) || text(user?.email) || null;
  const requireAdmin = async (req, tournamentId) => {
    const tournament = await loadCclTournament(db, tournamentId);
    if (!tournament) throw requestError(404, "CCL tournament not found");
    if (!await hasTournamentAdminAccess(db, req.user, tournament.id)) throw requestError(403, "Forbidden");
    return tournament;
  };
  const inTransaction = async (work) => {
    await dbRun(db, "BEGIN IMMEDIATE TRANSACTION");
    try {
      const result = await work();
      await dbRun(db, "COMMIT");
      return result;
    } catch (error) {
      await dbRun(db, "ROLLBACK").catch(() => {});
      throw error;
    }
  };

  // Manual sync, e.g. right after results were imported.
  app.post("/ccl/:tournamentId/brackets/sync", requireAuthenticated, handle("Failed to sync CCL brackets", async (req, res) => {
    const tournament = await requireAdmin(req, req.params.tournamentId);
    const result = await syncCclBrackets(db, tournament.id, { actorId: actorId(req.user) });
    if (result.changed) audit(req, bracketAuditEntry(result));
    return res.json({ ok: true, ...result });
  }));

  // Admin result of a bracket match (a no-show, a result entered by hand), or { clear: true }.
  // Refused when it would change the players of a match that has already started.
  app.post("/ccl/matches/:id/result", requireAuthenticated, handle("Failed to save the match result", async (req, res) => {
    const duel = await dbGet(
      db,
      `SELECT * FROM duels WHERE upper(trim(id)) = upper(trim(?)) AND deleted_at IS NULL
         AND source_type = 'ccl' AND group_id IS NOT NULL LIMIT 1`,
      [text(req.params.id)]
    );
    if (!duel) throw requestError(404, "Bracket match not found");
    const tournament = await requireAdmin(req, duel.tournament_id);
    const clear = req.body?.clear === true;
    const dw1 = clear ? null : Number(req.body?.dw1);
    const dw2 = clear ? null : Number(req.body?.dw2);
    if (!clear) {
      if (!text(duel.player_1_id) || !text(duel.player_2_id)) throw requestError(409, "The match has no two players yet");
      await validateScore(db, duel, dw1, dw2);
    }
    const by = actorId(req.user);
    const synced = await inTransaction(async () => {
      const before = new Set((await planCclBracketSync(db, tournament.id)).conflicts.map((conflict) => conflict.duel_id));
      await dbRun(
        db,
        `UPDATE duels SET dw1 = ?, dw2 = ?, status = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [dw1, dw2, clear ? (text(duel.player_1_id) && text(duel.player_2_id) ? "Planned" : "Draft") : "Done", by, duel.id]
      );
      const result = await syncCclBracketsInTransaction(db, tournament.id, { actorId: by });
      const added = result.conflicts.filter((conflict) => !before.has(conflict.duel_id));
      if (added.length) {
        throw requestError(409, "Later matches already have a time or a result; change them first", { conflicts: added });
      }
      return result;
    });
    audit(req, {
      event_type: clear ? "ccl.match.result_cleared" : "ccl.match.result_set",
      entity_type: "duel",
      action: "update",
      record_id: duel.id,
      changes: { dw1: { old: duel.dw1 ?? null, new: dw1 }, dw2: { old: duel.dw2 ?? null, new: dw2 } },
      metadata: { tournament_id: tournament.id },
    });
    if (synced.updates?.length || synced.stages?.length || synced.results?.length) audit(req, bracketAuditEntry(synced));
    return res.json({ ok: true, updates: synced.updates || [], results: synced.results || [], stages: synced.stages || [] });
  }));
}
