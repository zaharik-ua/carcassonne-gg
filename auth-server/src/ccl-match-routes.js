import { randomUUID } from "node:crypto";

import {
  CCL_MATCH_SOURCE_TYPE,
  CCL_MATCH_STAGES,
  compareCclMatches,
  getCclEligiblePlayerIds,
  getCclMatchPlayersError,
  normalizeCclMatchInput,
} from "./ccl-matches.js";
import { loadAssociationsById, loadCclTournament, loadProfilesById, loadSlots } from "./ccl-routes.js";
import {
  DUEL_TIME_PROPOSAL_STATUSES,
  getDuelSide,
  isDuelProposalSnapshotCurrent,
  isPlannedDuel,
  planDuelTimeAcceptance,
  planDuelTimeProposal,
} from "./duel-time-proposals.js";
import { hasTournamentAdminAccess } from "./tournament-admin-access.js";

// A CCL match is one duels row; stage, round and proposal live in the generic duels columns.
const MATCH_SELECT_SQL = `
  SELECT
    id,
    tournament_id,
    duel_format,
    time_utc,
    COALESCE(NULLIF(trim(status), ''), 'Planned') AS status,
    dw1,
    dw2,
    player_1_id,
    player_2_id,
    stage,
    group_name,
    round_name,
    round_order,
    deadline_utc,
    proposed_time_utc,
    proposed_time_by_player_id,
    proposed_time_status
  FROM duels
  WHERE deleted_at IS NULL
    AND source_type = '${CCL_MATCH_SOURCE_TYPE}'
`;
const EDITABLE_FIELDS = [
  "stage", "group_name", "round_name", "round_order", "deadline_utc",
  "player_1_id", "player_2_id", "duel_format", "time_utc",
];
const PROPOSAL_FIELDS = ["proposed_time_utc", "proposed_time_by_player_id", "proposed_time_status"];

const text = (value) => String(value ?? "").trim();

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

function requestError(status, message) {
  return Object.assign(new Error(message), { status });
}

const actorPlayerId = (user) => text(user?.player_id ?? user?.bga_id);

function changedFields(before, after, fields) {
  const changes = {};
  fields.forEach((field) => {
    const oldValue = before?.[field] ?? null;
    const newValue = after?.[field] ?? null;
    if (oldValue !== newValue) changes[field] = { old: oldValue, new: newValue };
  });
  return changes;
}

async function loadMatch(db, duelId) {
  return dbGet(db, `${MATCH_SELECT_SQL} AND upper(trim(id)) = upper(trim(?)) LIMIT 1`, [text(duelId)]);
}

async function updateMatch(db, duelId, values, actorId) {
  const fields = Object.keys(values);
  await dbRun(
    db,
    `UPDATE duels SET ${fields.map((field) => `${field} = ?`).join(", ")}, updated_by = ?, updated_at = CURRENT_TIMESTAMP
     WHERE id = ? AND deleted_at IS NULL`,
    [...fields.map((field) => values[field]), actorId, duelId]
  );
}

// Tournament admin, or a player who holds a filled slot of the tournament.
async function resolveActor(db, user, tournament, slots) {
  const playerId = actorPlayerId(user);
  const isAdmin = await hasTournamentAdminAccess(db, user, tournament.id);
  const isParticipant = !!playerId && slots.some((slot) => text(slot.player_id) === playerId);
  return { isAdmin, isParticipant, playerId: playerId || null };
}

async function loadMatchContext(db, user, duelId) {
  const match = await loadMatch(db, duelId);
  if (!match) throw requestError(404, "Match not found");
  const tournament = await loadCclTournament(db, match.tournament_id);
  if (!tournament) throw requestError(404, "Match not found");
  const slots = await loadSlots(db, tournament.id);
  const actor = await resolveActor(db, user, tournament, slots);
  if (!actor.isAdmin && !getDuelSide(match, actor.playerId)) throw requestError(403, "Forbidden");
  return { match, tournament, slots, actor };
}

async function assertDuelFormat(db, format) {
  const row = await dbGet(db, "SELECT format FROM duel_formats WHERE lower(trim(format)) = lower(trim(?)) LIMIT 1", [format]);
  if (!row) throw requestError(400, `Unknown duel_format: ${format}`);
  return text(row.format);
}

// Players have no timezone of their own; like challenges, use the team of their association.
async function loadTeamTimezones(db) {
  const rows = await dbAll(db, "SELECT id, name, timezone FROM teams WHERE trim(COALESCE(timezone, '')) <> ''");
  const byKey = new Map();
  rows.forEach((row) => {
    byKey.set(`id:${text(row.id).toUpperCase()}`, text(row.timezone));
    if (text(row.name)) byKey.set(`name:${text(row.name).toLowerCase()}`, text(row.timezone));
  });
  return byKey;
}

function findTimezone(timezones, ...keys) {
  for (const key of keys.map(text).filter(Boolean)) {
    const timezone = timezones.get(`id:${key.toUpperCase()}`) || timezones.get(`name:${key.toLowerCase()}`);
    if (timezone) return timezone;
  }
  return null;
}

function formatPlayer(playerId, { slotsByPlayer, profilesById, associationsById, timezones }) {
  const id = text(playerId);
  if (!id) return null;
  const profile = profilesById.get(id) || null;
  const slot = slotsByPlayer.get(id) || null;
  const association = associationsById.get(text(slot?.association_id || profile?.association).toUpperCase()) || null;
  return {
    id,
    bga_nickname: profile?.bga_nickname || null,
    name: profile?.name || null,
    avatar: profile?.avatar || null,
    gg_elo: profile?.gg_elo ?? null,
    entry_stage: slot?.entry_stage || null,
    league_status: slot?.league_status || null,
    association_id: slot?.association_id || null,
    association_name: association?.name || null,
    association_flag: association?.flag || null,
    timezone: findTimezone(timezones, slot?.association_id, association?.name, profile?.association),
  };
}

function formatMatch(match, context, viewerPlayerId) {
  return {
    id: match.id,
    tournament_id: match.tournament_id,
    stage: match.stage,
    group_name: match.group_name || null,
    round_name: match.round_name || null,
    round_order: match.round_order ?? null,
    deadline_utc: match.deadline_utc || null,
    duel_format: match.duel_format || null,
    time_utc: match.time_utc || null,
    status: match.status,
    dw1: match.dw1 ?? null,
    dw2: match.dw2 ?? null,
    player_1: formatPlayer(match.player_1_id, context),
    player_2: formatPlayer(match.player_2_id, context),
    proposed_time_utc: match.proposed_time_utc || null,
    proposed_time_by_player_id: match.proposed_time_by_player_id || null,
    proposed_time_status: match.proposed_time_status || null,
    my_side: getDuelSide(match, viewerPlayerId),
  };
}

async function buildFormatContext(db, slots, playerIds) {
  const [profilesById, associationsById, timezones] = await Promise.all([
    loadProfilesById(db, playerIds),
    loadAssociationsById(db),
    loadTeamTimezones(db),
  ]);
  const slotsByPlayer = new Map(slots.filter((slot) => text(slot.player_id)).map((slot) => [text(slot.player_id), slot]));
  return { slotsByPlayer, profilesById, associationsById, timezones };
}

async function formatOne(db, match, slots, viewerPlayerId) {
  const context = await buildFormatContext(db, slots, [match.player_1_id, match.player_2_id]);
  return formatMatch(match, context, viewerPlayerId);
}

export function registerCclMatchRoutes(app, {
  db,
  requireAuthenticated,
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

  // Admins get every match and the players they can pair; players get their own matches.
  app.get("/ccl/:tournamentId/matches", requireAuthenticated, handle("Failed to load CCL matches", async (req, res) => {
    const tournament = await loadCclTournament(db, req.params.tournamentId);
    if (!tournament) throw requestError(404, "CCL tournament not found");
    const slots = await loadSlots(db, tournament.id);
    const actor = await resolveActor(db, req.user, tournament, slots);
    if (!actor.isAdmin && !actor.isParticipant) throw requestError(403, "Forbidden");

    const rows = await dbAll(
      db,
      `${MATCH_SELECT_SQL} AND upper(trim(tournament_id)) = upper(trim(?))
        ${actor.isAdmin ? "" : "AND (trim(player_1_id) = ? OR trim(player_2_id) = ?)"}`,
      actor.isAdmin ? [tournament.id] : [tournament.id, actor.playerId, actor.playerId]
    );
    const playerIds = actor.isAdmin
      ? slots.map((slot) => slot.player_id)
      : rows.flatMap((row) => [row.player_1_id, row.player_2_id]);
    const context = await buildFormatContext(db, slots, playerIds);
    const formats = actor.isAdmin
      ? (await dbAll(db, "SELECT format FROM duel_formats ORDER BY format")).map((row) => text(row.format))
      : [];

    return res.json({
      ok: true,
      tournament: { id: tournament.id, name: tournament.name, short_title: tournament.short_title || null },
      can_manage: actor.isAdmin,
      my_player_id: actor.playerId,
      stages: CCL_MATCH_STAGES,
      duel_formats: formats,
      players: actor.isAdmin
        ? CCL_MATCH_STAGES.map((stage) => ({
          stage,
          players: getCclEligiblePlayerIds(slots, stage).map((id) => formatPlayer(id, context)),
        }))
        : [],
      matches: rows.sort(compareCclMatches).map((row) => formatMatch(row, context, actor.playerId)),
    });
  }));

  app.post("/ccl/:tournamentId/matches", requireAuthenticated, handle("Failed to create CCL match", async (req, res) => {
    const tournament = await loadCclTournament(db, req.params.tournamentId);
    if (!tournament) throw requestError(404, "CCL tournament not found");
    if (!await hasTournamentAdminAccess(db, req.user, tournament.id)) throw requestError(403, "Forbidden");
    const input = normalizeCclMatchInput(req.body || {});
    if (input.error) throw requestError(400, input.error);
    const values = input.value;
    values.duel_format = await assertDuelFormat(db, values.duel_format);

    const actorId = actorPlayerId(req.user) || null;
    const duelId = `${tournament.id}-${randomUUID().slice(0, 8)}`;
    const created = await inTransaction(db, async () => {
      const slots = await loadSlots(db, tournament.id);
      const playersError = getCclMatchPlayersError({
        stage: values.stage, player1Id: values.player_1_id, player2Id: values.player_2_id, slots,
      });
      if (playersError) throw requestError(400, playersError);
      await dbRun(
        db,
        `
          INSERT INTO duels (
            id, tournament_id, match_id, duel_number, duel_format, time_utc,
            player_1_id, player_2_id, ranking, status, source_type, is_test,
            stage, group_name, round_name, round_order, deadline_utc, proposed_time_status,
            created_by, updated_by
          )
          VALUES (?, ?, NULL, 1, ?, ?, ?, ?, ?, 'Planned', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          duelId, tournament.id, values.duel_format, values.time_utc,
          values.player_1_id, values.player_2_id, Number(tournament.ranking ?? 1),
          CCL_MATCH_SOURCE_TYPE, Number(tournament.is_test) === 1 ? 1 : 0,
          values.stage, values.group_name, values.round_name, values.round_order, values.deadline_utc,
          values.time_utc ? DUEL_TIME_PROPOSAL_STATUSES.ACCEPTED : null,
          actorId, actorId,
        ]
      );
      return { match: await loadMatch(db, duelId), slots };
    });

    audit(req, {
      event_type: "ccl.match.created",
      entity_type: "duel",
      action: "create",
      record_id: duelId,
      changes: changedFields({}, created.match, EDITABLE_FIELDS),
      metadata: { tournament_id: tournament.id },
    });
    return res.status(201).json({ ok: true, match: await formatOne(db, created.match, created.slots, actorPlayerId(req.user)) });
  }));

  // Admin edit. Setting the time or the players directly replaces any pending proposal.
  app.patch("/ccl/matches/:id", requireAuthenticated, handle("Failed to update CCL match", async (req, res) => {
    const { match, tournament, slots, actor } = await loadMatchContext(db, req.user, req.params.id);
    if (!actor.isAdmin) throw requestError(403, "Forbidden");
    const input = normalizeCclMatchInput(req.body || {}, { partial: true });
    if (input.error) throw requestError(400, input.error);
    const values = input.value;
    if (!Object.keys(values).length) throw requestError(400, "Nothing to update");
    if (values.duel_format) values.duel_format = await assertDuelFormat(db, values.duel_format);

    const next = { ...match, ...values };
    if (["stage", "player_1_id", "player_2_id"].some((field) => field in values)) {
      if (!isPlannedDuel(match)) throw requestError(409, "Players can be changed only for a planned match");
      const playersError = getCclMatchPlayersError({
        stage: next.stage, player1Id: next.player_1_id, player2Id: next.player_2_id, slots,
      });
      if (playersError) throw requestError(400, playersError);
    }
    if (["time_utc", "player_1_id", "player_2_id"].some((field) => field in values)) {
      Object.assign(values, {
        proposed_time_utc: null,
        proposed_time_by_player_id: null,
        proposed_time_status: next.time_utc ? DUEL_TIME_PROPOSAL_STATUSES.ACCEPTED : null,
      });
    }

    await updateMatch(db, match.id, values, actorPlayerId(req.user) || null);
    const updated = await loadMatch(db, match.id);
    audit(req, {
      event_type: "ccl.match.updated",
      entity_type: "duel",
      action: "update",
      record_id: match.id,
      changes: changedFields(match, updated, [...EDITABLE_FIELDS, ...PROPOSAL_FIELDS]),
      metadata: { tournament_id: tournament.id },
    });
    return res.json({ ok: true, match: await formatOne(db, updated, slots, actor.playerId) });
  }));

  app.delete("/ccl/matches/:id", requireAuthenticated, handle("Failed to delete CCL match", async (req, res) => {
    const { match, tournament, actor } = await loadMatchContext(db, req.user, req.params.id);
    if (!actor.isAdmin) throw requestError(403, "Forbidden");
    await dbRun(
      db,
      "UPDATE duels SET deleted_at = CURRENT_TIMESTAMP, deleted_by = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted_at IS NULL",
      [actorPlayerId(req.user) || null, match.id]
    );
    audit(req, {
      event_type: "ccl.match.deleted",
      entity_type: "duel",
      action: "delete",
      record_id: match.id,
      changes: {},
      metadata: { tournament_id: tournament.id },
    });
    return res.json({ ok: true });
  }));

  app.post("/ccl/matches/:id/time-proposal", requireAuthenticated, handle("Failed to save time proposal", async (req, res) => {
    const payload = req.body || {};
    const result = await inTransaction(db, async () => {
      const { match, slots, actor } = await loadMatchContext(db, req.user, req.params.id);
      if (!isDuelProposalSnapshotCurrent(match, payload)) {
        throw requestError(409, "The time proposal has changed. Reload the match and try again");
      }
      const plan = planDuelTimeProposal({
        duel: match, actorPlayerId: actor.playerId, proposedTimeUtc: payload.proposed_time_utc, nowMs: now().getTime(),
      });
      if (plan.error) throw requestError(plan.status, plan.error);
      await updateMatch(db, match.id, plan.value, actor.playerId);
      return { before: match, after: await loadMatch(db, match.id), slots, actor, event: plan.event };
    });

    audit(req, {
      event_type: result.event,
      entity_type: "duel",
      action: "update",
      record_id: result.after.id,
      changes: changedFields(result.before, result.after, PROPOSAL_FIELDS),
      metadata: { tournament_id: result.after.tournament_id },
    });
    return res.json({ ok: true, match: await formatOne(db, result.after, result.slots, result.actor.playerId) });
  }));

  app.patch("/ccl/matches/:id/time-proposal/accept", requireAuthenticated, handle("Failed to accept time proposal", async (req, res) => {
    const payload = req.body || {};
    const result = await inTransaction(db, async () => {
      const { match, slots, actor } = await loadMatchContext(db, req.user, req.params.id);
      if (!isDuelProposalSnapshotCurrent(match, payload)) {
        throw requestError(409, "The time proposal has changed. Reload the match and review it again");
      }
      const plan = planDuelTimeAcceptance({ duel: match, actorPlayerId: actor.playerId, nowMs: now().getTime() });
      if (plan.error) throw requestError(plan.status, plan.error);
      await updateMatch(db, match.id, plan.value, actor.playerId);
      return { before: match, after: await loadMatch(db, match.id), slots, actor };
    });

    audit(req, {
      event_type: "duel.time_proposal.accepted",
      entity_type: "duel",
      action: "update",
      record_id: result.after.id,
      changes: changedFields(result.before, result.after, ["time_utc", "proposed_time_status"]),
      metadata: {
        tournament_id: result.after.tournament_id,
        proposed_time_by_player_id: result.before.proposed_time_by_player_id,
      },
    });
    return res.json({ ok: true, match: await formatOne(db, result.after, result.slots, result.actor.playerId) });
  }));
}
