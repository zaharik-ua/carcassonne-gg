import { randomUUID } from "node:crypto";

import {
  CCL_PHASES,
  CCL_STAGES,
  buildCclEloAssignments,
  buildCclPlayerLists,
  formCclPots,
  getCclLeagueResultError,
  getCclPhaseStatus,
  getCclPhasesAffectedByPlayerChange,
  getCclRatedElo,
  getCclSlotUpdateError,
  getCclStageDeadline,
  getCclStagePhase,
  isCclEntryEloSet,
  isCclStageRegistrationOpen,
  isCclTournament,
  normalizeCclManualElo,
  normalizeCclQualifiedBy,
  normalizeCclSlotCreateInput,
  normalizeCclStage,
} from "./ccl-slots.js";
import { hasTournamentAdminAccess } from "./tournament-admin-access.js";

// Same canonical association id as resolveAssociationId in server.js.
const ASSOCIATION_ID_SQL = "COALESCE(NULLIF(trim(a.code), ''), printf('ASSOCIATION_%s', a.rowid))";
const SLOT_FIELDS = ["association_id", "entry_stage", "player_id", "qualified_by_name", "qualified_by_link"];

const text = (value) => String(value ?? "").trim();
const idKey = (value) => text(value).toUpperCase();
const has = (object, field) => Object.prototype.hasOwnProperty.call(object || {}, field);

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

function requestError(status, message, extra = {}) {
  return Object.assign(new Error(message), { status, extra });
}

function actorLabel(user) {
  return text(user?.email)
    || text(user?.player_id ?? user?.bga_id)
    || (Number.isInteger(Number(user?.id)) ? `user:${Number(user.id)}` : null);
}

export async function loadCclTournament(db, tournamentId) {
  const id = text(tournamentId);
  if (!id) return null;
  const row = await dbGet(db, "SELECT * FROM tournaments WHERE upper(trim(id)) = upper(trim(?)) LIMIT 1", [id]);
  return row && isCclTournament(row) ? row : null;
}

// Accepts an association code, canonical id or name; returns { id, name, flag } or null.
export async function resolveCclAssociation(db, value) {
  const raw = text(value);
  if (!raw) return null;
  const row = await dbGet(
    db,
    `
      SELECT ${ASSOCIATION_ID_SQL} AS id, a.name, a.flag
      FROM associations a
      WHERE upper(trim(COALESCE(a.code, ''))) = upper(?)
         OR upper(${ASSOCIATION_ID_SQL}) = upper(?)
         OR lower(trim(COALESCE(a.name, ''))) = lower(?)
      LIMIT 1
    `,
    [raw, raw, raw]
  );
  return row ? { id: text(row.id), name: row.name, flag: row.flag || null } : null;
}

async function loadAssociationsById(db) {
  const rows = await dbAll(db, `SELECT ${ASSOCIATION_ID_SQL} AS id, a.name, a.flag FROM associations a`);
  return new Map(rows.map((row) => [idKey(row.id), { id: text(row.id), name: row.name, flag: row.flag || null }]));
}

// Admin of the tournament, or captain (profiles.team_captain = 1) of an association.
export async function resolveCclActor(db, user, tournament) {
  const isAdmin = await hasTournamentAdminAccess(db, user, tournament.id);
  const association = Number(user?.team_captain) === 1
    ? await resolveCclAssociation(db, user?.association)
    : null;
  return { isAdmin, captainAssociationId: association?.id || null };
}

async function loadSlots(db, tournamentId) {
  return dbAll(
    db,
    `
      SELECT id, tournament_id, association_id, entry_stage, player_id, league_status, created_at
      FROM tournament_players
      WHERE upper(trim(tournament_id)) = upper(trim(?))
        AND entry_stage IS NOT NULL
      ORDER BY created_at ASC, id ASC
    `,
    [tournamentId]
  );
}

async function loadSlot(db, slotId) {
  return dbGet(
    db,
    `
      SELECT id, tournament_id, association_id, entry_stage, player_id, league_status
      FROM tournament_players
      WHERE upper(trim(id)) = upper(trim(?))
        AND entry_stage IS NOT NULL
      LIMIT 1
    `,
    [text(slotId)]
  );
}

async function loadEntries(db, tournamentId) {
  return dbAll(
    db,
    "SELECT * FROM ccl_stage_entries WHERE upper(trim(tournament_id)) = upper(trim(?))",
    [tournamentId]
  );
}

async function loadProfilesById(db, playerIds) {
  const ids = [...new Set(playerIds.map(text).filter(Boolean))];
  if (!ids.length) return new Map();
  const rows = await dbAll(
    db,
    `
      SELECT trim(id) AS id, bga_nickname, name, avatar, association, gg_elo, gg_rating_position
      FROM profiles
      WHERE trim(id) IN (${ids.map(() => "?").join(", ")})
        AND deleted_at IS NULL
    `,
    ids
  );
  return new Map(rows.map((row) => [text(row.id), row]));
}

// Fresh pots are required after any change in a phase (design 4.3).
async function resetPhaseSeeds(db, tournamentId, phases) {
  for (const phase of new Set(phases)) {
    const stages = CCL_PHASES[phase];
    if (!stages) continue;
    await dbRun(
      db,
      `
        UPDATE ccl_stage_entries
        SET seed = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE upper(trim(tournament_id)) = upper(trim(?))
          AND stage IN (${stages.map(() => "?").join(", ")})
          AND seed IS NOT NULL
      `,
      [tournamentId, ...stages]
    );
  }
}

function formatEntry(entry, slot) {
  return {
    stage: entry.stage,
    qualified_by_name: entry.qualified_by_name,
    qualified_by_link: entry.qualified_by_link || null,
    player_id: entry.player_id || null,
    elo: entry.elo ?? null,
    elo_source: entry.elo_source || null,
    seed: entry.seed || null,
    fixed_at: entry.fixed_at || null,
    fixed_by: entry.fixed_by || null,
    elo_fixed: isCclEntryEloSet(entry, slot),
  };
}

function formatSlot(slot, { entries, profilesById, associationsById }) {
  const profile = profilesById.get(text(slot.player_id)) || null;
  const association = associationsById.get(idKey(slot.association_id)) || null;
  const stageOrder = (stage) => CCL_STAGES.indexOf(stage);
  return {
    id: slot.id,
    tournament_id: slot.tournament_id,
    association_id: slot.association_id || null,
    association_name: association?.name || null,
    association_flag: association?.flag || null,
    entry_stage: slot.entry_stage,
    player_id: slot.player_id || null,
    player: profile
      ? {
        id: text(profile.id),
        bga_nickname: profile.bga_nickname || null,
        name: profile.name || null,
        avatar: profile.avatar || null,
        gg_elo: profile.gg_elo ?? null,
      }
      : null,
    league_status: slot.league_status || null,
    entries: entries
      .filter((entry) => idKey(entry.slot_id) === idKey(slot.id))
      .sort((a, b) => stageOrder(a.stage) - stageOrder(b.stage))
      .map((entry) => formatEntry(entry, slot)),
  };
}

async function loadTournamentState(db, tournament) {
  const [slots, entries, associationsById] = await Promise.all([
    loadSlots(db, tournament.id),
    loadEntries(db, tournament.id),
    loadAssociationsById(db),
  ]);
  const profilesById = await loadProfilesById(db, slots.map((slot) => slot.player_id));
  return { slots, entries, associationsById, profilesById };
}

function stageDeadlines(tournament, now) {
  return CCL_STAGES.map((stage) => ({
    stage,
    registration_ends_at: getCclStageDeadline(tournament, stage),
    registration_open: isCclStageRegistrationOpen(tournament, stage, now),
  }));
}

async function loadSlotContext(db, user, slotId) {
  const slot = await loadSlot(db, slotId);
  if (!slot) throw requestError(404, "Slot not found");
  const tournament = await loadCclTournament(db, slot.tournament_id);
  if (!tournament) throw requestError(404, "Slot not found");
  const actor = await resolveCclActor(db, user, tournament);
  return { slot, tournament, actor };
}

async function requireCclAdmin(db, user, tournamentId) {
  const tournament = await loadCclTournament(db, tournamentId);
  if (!tournament) throw requestError(404, "CCL tournament not found");
  const actor = await resolveCclActor(db, user, tournament);
  if (!actor.isAdmin) throw requestError(403, "Forbidden");
  return { tournament, actor };
}

function phaseParam(value) {
  const phase = text(value).toLowerCase();
  if (!CCL_PHASES[phase]) throw requestError(400, "phase must be qualification or league");
  return phase;
}

function changedFields(before, after) {
  const changes = {};
  Object.keys(after).forEach((field) => {
    const oldValue = before?.[field] ?? null;
    const newValue = after[field] ?? null;
    if (oldValue !== newValue) changes[field] = { old: oldValue, new: newValue };
  });
  return changes;
}

export function registerCclRoutes(app, {
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
      return res.status(status).json({
        ok: false,
        message: status >= 500 ? failureMessage : error.message,
        ...(error?.extra || {}),
      });
    }
  };

  app.get("/ccl/:tournamentId/slots", requireAuthenticated, handle("Failed to load CCL slots", async (req, res) => {
    const tournament = await loadCclTournament(db, req.params.tournamentId);
    if (!tournament) throw requestError(404, "CCL tournament not found");
    const actor = await resolveCclActor(db, req.user, tournament);
    const state = await loadTournamentState(db, tournament);
    const visibleSlots = actor.isAdmin
      ? state.slots
      : state.slots.filter((slot) => (
        actor.captainAssociationId && idKey(slot.association_id) === idKey(actor.captainAssociationId)
      ));
    if (!actor.isAdmin && !visibleSlots.length) throw requestError(403, "Forbidden");

    const phases = Object.fromEntries(Object.keys(CCL_PHASES).map((phase) => {
      const status = getCclPhaseStatus({ slots: state.slots, entries: state.entries, phase });
      if (!actor.isAdmin) delete status.pots_blockers;
      return [phase, status];
    }));
    return res.json({
      ok: true,
      tournament: {
        id: tournament.id,
        name: tournament.name,
        stages: stageDeadlines(tournament, now().getTime()),
      },
      can_manage: actor.isAdmin,
      captain_association_id: actor.captainAssociationId,
      phases,
      slots: visibleSlots.map((slot) => formatSlot(slot, state)),
    });
  }));

  app.post("/ccl/:tournamentId/slots", requireAuthenticated, handle("Failed to create CCL slots", async (req, res) => {
    const { tournament } = await requireCclAdmin(db, req.user, req.params.tournamentId);
    const input = normalizeCclSlotCreateInput(req.body || {});
    if (input.error) throw requestError(400, input.error);
    let association = null;
    if (input.value.association_id) {
      association = await resolveCclAssociation(db, input.value.association_id);
      if (!association) throw requestError(400, `Association not found: ${input.value.association_id}`);
    }
    const actor = actorLabel(req.user);
    const slotIds = Array.from({ length: input.value.count }, () => randomUUID());

    await inTransaction(db, async () => {
      for (const slotId of slotIds) {
        await dbRun(
          db,
          `
            INSERT INTO tournament_players (
              id, tournament_id, team_id, player_id, captain, association_id, entry_stage,
              created_by, updated_by, created_at, updated_at
            )
            VALUES (?, ?, NULL, NULL, 0, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          `,
          [slotId, tournament.id, association?.id || null, input.value.entry_stage, actor, actor]
        );
        await dbRun(
          db,
          `
            INSERT INTO ccl_stage_entries (tournament_id, stage, slot_id, qualified_by_name, qualified_by_link)
            VALUES (?, ?, ?, ?, ?)
          `,
          [tournament.id, input.value.entry_stage, slotId, input.value.qualified_by_name, input.value.qualified_by_link]
        );
      }
      await resetPhaseSeeds(db, tournament.id, [getCclStagePhase(input.value.entry_stage)]);
    });

    audit(req, {
      event_type: "ccl_slots.created",
      entity_type: "ccl_slot",
      action: "create",
      record_id: tournament.id,
      changes: {
        association_id: { old: null, new: association?.id || null },
        entry_stage: { old: null, new: input.value.entry_stage },
        qualified_by_name: { old: null, new: input.value.qualified_by_name },
        qualified_by_link: { old: null, new: input.value.qualified_by_link },
      },
      metadata: { tournament_id: tournament.id, slot_ids: slotIds },
    });
    const state = await loadTournamentState(db, tournament);
    const created = new Set(slotIds.map(idKey));
    return res.status(201).json({
      ok: true,
      slots: state.slots.filter((slot) => created.has(idKey(slot.id))).map((slot) => formatSlot(slot, state)),
    });
  }));

  app.patch("/ccl/slots/:id", requireAuthenticated, handle("Failed to update CCL slot", async (req, res) => {
    const { slot, tournament, actor } = await loadSlotContext(db, req.user, req.params.id);
    if (!actor.isAdmin && (
      !actor.captainAssociationId || idKey(slot.association_id) !== idKey(actor.captainAssociationId)
    )) {
      throw requestError(403, "Forbidden");
    }

    const changes = { ...(req.body || {}) };
    if (has(changes, "association_id") && text(changes.association_id)) {
      const association = await resolveCclAssociation(db, changes.association_id);
      if (!association) throw requestError(400, `Association not found: ${text(changes.association_id)}`);
      changes.association_id = association.id;
    }
    if (has(changes, "player_id")) changes.player_id = text(changes.player_id) || null;

    const playerId = has(changes, "player_id") ? changes.player_id : text(slot.player_id) || null;
    const playerChanges = has(changes, "player_id") && changes.player_id !== (text(slot.player_id) || null);
    let player = null;
    let playerSlotId = null;
    if (playerId) {
      if (playerChanges || has(changes, "association_id")) {
        const profile = (await loadProfilesById(db, [playerId])).get(playerId);
        if (profile) {
          const playerAssociation = await resolveCclAssociation(db, profile.association);
          player = { id: playerId, association: playerAssociation?.id || null };
        }
      }
      const other = await dbGet(
        db,
        `
          SELECT id FROM tournament_players
          WHERE upper(trim(tournament_id)) = upper(trim(?))
            AND entry_stage IS NOT NULL
            AND trim(player_id) = ?
            AND upper(trim(id)) <> upper(trim(?))
          LIMIT 1
        `,
        [tournament.id, playerId, slot.id]
      );
      playerSlotId = other?.id || null;
    }

    const entries = await loadEntries(db, tournament.id);
    const entryPhase = getCclStagePhase(slot.entry_stage);
    const error = getCclSlotUpdateError({
      slot,
      changes,
      isAdmin: actor.isAdmin,
      captainAssociationId: actor.captainAssociationId,
      tournament,
      player,
      playerSlotId,
      now: now().getTime(),
    });
    if (error) throw requestError(400, error);

    const nextSlot = {
      association_id: has(changes, "association_id") ? text(changes.association_id) || null : slot.association_id || null,
      entry_stage: has(changes, "entry_stage") ? normalizeCclStage(changes.entry_stage) : slot.entry_stage,
      player_id: playerId,
    };
    const qualifiedBy = normalizeCclQualifiedBy(changes, { partial: true }).value;
    const stageChanges = nextSlot.entry_stage !== slot.entry_stage;
    const entryBefore = entries.find((entry) => (
      idKey(entry.slot_id) === idKey(slot.id) && entry.stage === slot.entry_stage
    ));
    const phasesToReset = [];
    if (playerChanges) phasesToReset.push(...getCclPhasesAffectedByPlayerChange(slot));
    if (stageChanges) phasesToReset.push(entryPhase, getCclStagePhase(nextSlot.entry_stage));
    // Elo already issued for this slot follows the new player (or is cleared with the player).
    const eloRefreshStages = playerChanges
      ? entries
        .filter((entry) => idKey(entry.slot_id) === idKey(slot.id) && entry.fixed_at)
        .filter((entry) => !stageChanges || entry.stage !== slot.entry_stage)
        .map((entry) => entry.stage)
      : [];
    const newPlayerElo = playerId && eloRefreshStages.length
      ? getCclRatedElo((await loadProfilesById(db, [playerId])).get(playerId)?.gg_elo)
      : null;

    await inTransaction(db, async () => {
      await dbRun(
        db,
        `
          UPDATE tournament_players
          SET association_id = ?, entry_stage = ?, player_id = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [nextSlot.association_id, nextSlot.entry_stage, nextSlot.player_id, actorLabel(req.user), slot.id]
      );
      if (stageChanges) {
        // Elo set for the old stage no longer applies.
        await dbRun(
          db,
          `
            UPDATE ccl_stage_entries
            SET stage = ?, player_id = NULL, elo = NULL, elo_source = NULL, seed = NULL,
                fixed_at = NULL, fixed_by = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE slot_id = ? AND stage = ?
          `,
          [nextSlot.entry_stage, slot.id, slot.entry_stage]
        );
      }
      for (const stage of eloRefreshStages) {
        await dbRun(
          db,
          playerId
            ? `
              UPDATE ccl_stage_entries
              SET player_id = ?, elo = ?, elo_source = 'gg_elo', seed = NULL, fixed_at = ?, fixed_by = ?,
                  updated_at = CURRENT_TIMESTAMP
              WHERE slot_id = ? AND stage = ?
            `
            : `
              UPDATE ccl_stage_entries
              SET player_id = NULL, elo = NULL, elo_source = NULL, seed = NULL, fixed_at = NULL, fixed_by = NULL,
                  updated_at = CURRENT_TIMESTAMP
              WHERE slot_id = ? AND stage = ?
            `,
          playerId
            ? [playerId, newPlayerElo, now().toISOString(), actorLabel(req.user), slot.id, stage]
            : [slot.id, stage]
        );
      }
      if (Object.keys(qualifiedBy).length) {
        const assignments = Object.keys(qualifiedBy).map((field) => `${field} = ?`).join(", ");
        await dbRun(
          db,
          `UPDATE ccl_stage_entries SET ${assignments}, updated_at = CURRENT_TIMESTAMP WHERE slot_id = ? AND stage = ?`,
          [...Object.values(qualifiedBy), slot.id, nextSlot.entry_stage]
        );
      }
      await resetPhaseSeeds(db, tournament.id, phasesToReset);
    });

    audit(req, {
      event_type: "ccl_slot.updated",
      entity_type: "ccl_slot",
      action: "update",
      record_id: slot.id,
      changes: {
        ...changedFields(slot, nextSlot),
        ...changedFields(entryBefore, qualifiedBy),
      },
      metadata: {
        tournament_id: tournament.id,
        reset_pots: [...new Set(phasesToReset)],
        elo_refreshed_stages: eloRefreshStages,
      },
    });
    const state = await loadTournamentState(db, tournament);
    const saved = state.slots.find((row) => idKey(row.id) === idKey(slot.id));
    return res.json({ ok: true, slot: formatSlot(saved, state) });
  }));

  app.delete("/ccl/slots/:id", requireAuthenticated, handle("Failed to delete CCL slot", async (req, res) => {
    const { slot, tournament, actor } = await loadSlotContext(db, req.user, req.params.id);
    if (!actor.isAdmin) throw requestError(403, "Forbidden");
    const phases = [getCclStagePhase(slot.entry_stage), ...getCclPhasesAffectedByPlayerChange(slot)];
    // Foreign keys are not enforced on the production connection; delete children explicitly.
    await inTransaction(db, async () => {
      await dbRun(db, "DELETE FROM ccl_stage_entries WHERE slot_id = ?", [slot.id]);
      await dbRun(db, "DELETE FROM tournament_players WHERE id = ?", [slot.id]);
      await resetPhaseSeeds(db, tournament.id, phases);
    });
    audit(req, {
      event_type: "ccl_slot.deleted",
      entity_type: "ccl_slot",
      action: "delete",
      record_id: slot.id,
      changes: changedFields(slot, { association_id: null, entry_stage: null, player_id: null, league_status: null }),
      metadata: { tournament_id: tournament.id },
    });
    return res.json({ ok: true, deleted_slot_id: slot.id });
  }));

  app.patch("/ccl/slots/:id/result", requireAuthenticated, handle("Failed to save qualification result", async (req, res) => {
    const { slot, tournament, actor } = await loadSlotContext(db, req.user, req.params.id);
    if (!actor.isAdmin) throw requestError(403, "Forbidden");
    const leagueStatus = text(req.body?.league_status).toLowerCase() || null;
    const qualifiedByInput = {
      qualified_by_name: req.body?.league_qualified_by_name,
      qualified_by_link: req.body?.league_qualified_by_link,
    };
    const error = getCclLeagueResultError({ slot, league_status: leagueStatus, ...qualifiedByInput });
    if (error) throw requestError(400, error);

    const mainEntry = await dbGet(db, "SELECT * FROM ccl_stage_entries WHERE slot_id = ? AND stage = 'Main'", [slot.id]);
    const qualifiedBy = leagueStatus === "qualified" ? normalizeCclQualifiedBy(qualifiedByInput).value : null;
    await inTransaction(db, async () => {
      await dbRun(
        db,
        "UPDATE tournament_players SET league_status = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        [leagueStatus, actorLabel(req.user), slot.id]
      );
      if (qualifiedBy && mainEntry) {
        await dbRun(
          db,
          `
            UPDATE ccl_stage_entries
            SET qualified_by_name = ?, qualified_by_link = ?, updated_at = CURRENT_TIMESTAMP
            WHERE slot_id = ? AND stage = 'Main'
          `,
          [qualifiedBy.qualified_by_name, qualifiedBy.qualified_by_link, slot.id]
        );
      } else if (qualifiedBy) {
        await dbRun(
          db,
          `
            INSERT INTO ccl_stage_entries (tournament_id, stage, slot_id, qualified_by_name, qualified_by_link)
            VALUES (?, 'Main', ?, ?, ?)
          `,
          [tournament.id, slot.id, qualifiedBy.qualified_by_name, qualifiedBy.qualified_by_link]
        );
        await resetPhaseSeeds(db, tournament.id, ["league"]);
      } else if (mainEntry) {
        await dbRun(db, "DELETE FROM ccl_stage_entries WHERE slot_id = ? AND stage = 'Main'", [slot.id]);
        await resetPhaseSeeds(db, tournament.id, ["league"]);
      }
    });

    audit(req, {
      event_type: "ccl_slot.result_updated",
      entity_type: "ccl_slot",
      action: "update",
      record_id: slot.id,
      changes: {
        ...changedFields(slot, { league_status: leagueStatus }),
        ...changedFields(
          { league_qualified_by_name: mainEntry?.qualified_by_name, league_qualified_by_link: mainEntry?.qualified_by_link },
          { league_qualified_by_name: qualifiedBy?.qualified_by_name, league_qualified_by_link: qualifiedBy?.qualified_by_link }
        ),
      },
      metadata: { tournament_id: tournament.id },
    });
    const state = await loadTournamentState(db, tournament);
    const saved = state.slots.find((row) => idKey(row.id) === idKey(slot.id));
    return res.json({ ok: true, slot: formatSlot(saved, state) });
  }));

  app.post("/ccl/:tournamentId/elo/:phase", requireAuthenticated, handle("Failed to set CCL Elo", async (req, res) => {
    const { tournament } = await requireCclAdmin(db, req.user, req.params.tournamentId);
    const phase = phaseParam(req.params.phase);
    const { slots, entries, profilesById } = await loadTournamentState(db, tournament);
    const { assignments, skipped } = buildCclEloAssignments({ slots, phase, profilesById });
    const entryKeys = new Set(entries.map((entry) => `${idKey(entry.slot_id)}|${entry.stage}`));
    const missing = assignments.filter((row) => !entryKeys.has(`${idKey(row.slot_id)}|${row.stage}`));
    if (missing.length) {
      throw requestError(409, "Some slots have no stage entry", {
        blockers: missing.map((row) => ({ slot_id: row.slot_id, reason: "stage_entry_missing" })),
      });
    }

    const fixedAt = now().toISOString();
    const fixedBy = actorLabel(req.user);
    await inTransaction(db, async () => {
      await resetPhaseSeeds(db, tournament.id, [phase]);
      for (const row of assignments) {
        await dbRun(
          db,
          `
            UPDATE ccl_stage_entries
            SET player_id = ?, elo = ?, elo_source = ?, seed = NULL, fixed_at = ?, fixed_by = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE slot_id = ? AND stage = ?
          `,
          [row.player_id, row.elo, row.elo_source, fixedAt, fixedBy, row.slot_id, row.stage]
        );
      }
    });
    audit(req, {
      event_type: "ccl_elo.set",
      entity_type: "ccl_phase",
      action: "update",
      record_id: `${tournament.id}|${phase}`,
      changes: Object.fromEntries(assignments.map((row) => [`${row.slot_id}|${row.stage}`, { new: row.elo }])),
      metadata: { tournament_id: tournament.id, phase, fixed_at: fixedAt, players: assignments.length, skipped },
    });
    const state = await loadTournamentState(db, tournament);
    return res.json({
      ok: true,
      phase: getCclPhaseStatus({ slots: state.slots, entries: state.entries, phase }),
      skipped,
      slots: state.slots.map((slot) => formatSlot(slot, state)),
    });
  }));

  app.patch("/ccl/slots/:id/stages/:stage/elo", requireAuthenticated, handle("Failed to save manual Elo", async (req, res) => {
    const { slot, tournament, actor } = await loadSlotContext(db, req.user, req.params.id);
    if (!actor.isAdmin) throw requestError(403, "Forbidden");
    const stage = normalizeCclStage(req.params.stage);
    if (!stage) throw requestError(400, `stage must be one of ${CCL_STAGES.join(", ")}`);
    if (!text(slot.player_id)) throw requestError(400, "The slot has no player");
    const elo = normalizeCclManualElo(req.body?.elo);
    if (elo.error) throw requestError(400, elo.error);
    const entry = await dbGet(db, "SELECT * FROM ccl_stage_entries WHERE slot_id = ? AND stage = ?", [slot.id, stage]);
    if (!entry) throw requestError(404, `The slot does not take part in ${stage}`);

    await inTransaction(db, async () => {
      await resetPhaseSeeds(db, tournament.id, [getCclStagePhase(stage)]);
      await dbRun(
        db,
        `
          UPDATE ccl_stage_entries
          SET player_id = ?, elo = ?, elo_source = 'manual', seed = NULL, fixed_at = ?, fixed_by = ?,
              updated_at = CURRENT_TIMESTAMP
          WHERE slot_id = ? AND stage = ?
        `,
        [text(slot.player_id), elo.value, now().toISOString(), actorLabel(req.user), slot.id, stage]
      );
    });
    audit(req, {
      event_type: "ccl_elo.manual",
      entity_type: "ccl_stage_entry",
      action: "update",
      record_id: `${slot.id}|${stage}`,
      changes: {
        elo: { old: entry.elo ?? null, new: elo.value },
        elo_source: { old: entry.elo_source || null, new: "manual" },
      },
      metadata: { tournament_id: tournament.id, player_id: text(slot.player_id) },
    });
    const state = await loadTournamentState(db, tournament);
    const saved = state.slots.find((row) => idKey(row.id) === idKey(slot.id));
    return res.json({ ok: true, slot: formatSlot(saved, state) });
  }));

  app.post("/ccl/:tournamentId/pots/:phase", requireAuthenticated, handle("Failed to form CCL pots", async (req, res) => {
    const { tournament } = await requireCclAdmin(db, req.user, req.params.tournamentId);
    const phase = phaseParam(req.params.phase);
    const { slots, entries, profilesById } = await loadTournamentState(db, tournament);
    const ratingPositionByPlayerId = new Map(
      [...profilesById.values()].map((profile) => [text(profile.id), profile.gg_rating_position])
    );
    // Pot sizes are fixed by the CCL format (CCL_QUALIFICATION_POTS, CCL_LEAGUE_POT_SIZE).
    const result = formCclPots({ slots, entries, phase, ratingPositionByPlayerId });
    if (result.error) {
      throw requestError(result.blockers ? 409 : 400, result.error, result.blockers ? { blockers: result.blockers } : {});
    }

    await inTransaction(db, async () => {
      await resetPhaseSeeds(db, tournament.id, [phase]);
      for (const row of result.seeds) {
        await dbRun(
          db,
          "UPDATE ccl_stage_entries SET seed = ?, updated_at = CURRENT_TIMESTAMP WHERE slot_id = ? AND stage = ?",
          [row.seed, row.slot_id, row.stage]
        );
      }
    });
    audit(req, {
      event_type: "ccl_pots.formed",
      entity_type: "ccl_phase",
      action: "update",
      record_id: `${tournament.id}|${phase}`,
      changes: Object.fromEntries(result.seeds.map((row) => [`${row.slot_id}|${row.stage}`, { new: row.seed }])),
      metadata: { tournament_id: tournament.id, phase },
    });
    return res.json({ ok: true, phase, seeds: result.seeds });
  }));

  app.get("/public/ccl/:tournamentId/players", handle("Failed to load CCL players", async (req, res) => {
    const tournament = await loadCclTournament(db, req.params.tournamentId);
    if (!tournament) throw requestError(404, "CCL tournament not found");
    const state = await loadTournamentState(db, tournament);
    const lists = buildCclPlayerLists(state);
    const phases = Object.fromEntries(Object.keys(CCL_PHASES).map((phase) => {
      const status = getCclPhaseStatus({ slots: state.slots, entries: state.entries, phase });
      return [phase, { elo_set: status.elo_set, elo_set_at: status.elo_set_at, pots_formed: status.pots_formed }];
    }));
    return res.json({
      ok: true,
      tournament: { id: tournament.id, name: tournament.name },
      ...lists,
      stages: stageDeadlines(tournament, now().getTime()),
      phases,
    });
  }));
}
