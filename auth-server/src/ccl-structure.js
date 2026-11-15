// CCL tournament structure: stages, groups and placeholder matches generated from the season format.
// Generated matches are duels rows (source_type 'ccl', status 'Draft', no players yet);
// the draw (Round 1) and the results (later rounds) fill their players.

import { randomUUID } from "node:crypto";

import { getCclFormat } from "./ccl-formats.js";
import { CCL_MATCH_SOURCE_TYPE } from "./ccl-matches.js";
import { loadCclTournament, loadProfilesById } from "./ccl-routes.js";
import { buildBracketLinks, buildDoubleEliminationGroup, describeSlotSource } from "./double-elimination.js";
import { hasTournamentAdminAccess } from "./tournament-admin-access.js";

const DAY_MS = 86400000;
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

function requestError(status, message, data = undefined) {
  return Object.assign(new Error(message), { status, data });
}

// "2026-11-16" → the same string when it is a valid Monday; null when empty; an error otherwise.
export function normalizeStageStartDate(value) {
  const raw = text(value);
  if (!raw) return { value: null };
  const parsed = /^\d{4}-\d{2}-\d{2}$/.test(raw) ? Date.parse(`${raw}T00:00:00Z`) : NaN;
  if (!Number.isFinite(parsed) || new Date(parsed).toISOString().slice(0, 10) !== raw) {
    return { error: "start_date must be a date in YYYY-MM-DD format" };
  }
  if (new Date(parsed).getUTCDay() !== 1) return { error: "start_date must be a Monday" };
  return { value: raw };
}

// Sunday 23:59 UTC of the given week of a path that starts on startDate (a Monday).
export function weekDeadlineUtc(startDate, week) {
  if (!startDate || !Number.isInteger(Number(week))) return null;
  const monday = Date.parse(`${startDate}T00:00:00Z`) + (Number(week) - 1) * 7 * DAY_MS;
  return new Date(monday + 6 * DAY_MS + (23 * 60 + 59) * 60000).toISOString();
}

const stageId = (tournamentId, stageKey) => `${tournamentId}|${stageKey}`;
const groupId = (tournamentId, stageKey, groupKey) => `${tournamentId}|${stageKey}|${groupKey}`;

// Label of the matches whose winner leaves the group, e.g. "Winner" for match 11 of Q1.
function advanceLabels(settings) {
  const labels = new Map();
  for (const { source, label } of settings.advance || []) {
    const match = /^winner:([\d|]+)$/.exec(text(source));
    if (match) match[1].split("|").forEach((no) => labels.set(Number(no), label));
  }
  return labels;
}

// Pure: every stage, group and placeholder match the format defines for this tournament.
export function buildCclStructurePlan({ tournamentId, format, startDates = {} }) {
  const stages = [];
  const groups = [];
  const matches = [];
  const addStage = (definition, parentId, sortOrder) => {
    const id = stageId(tournamentId, definition.key);
    const isPath = Array.isArray(definition.groups);
    const startDate = isPath ? startDates[definition.key] ?? null : null;
    stages.push({
      id,
      parent_id: parentId,
      stage_key: definition.key,
      name: definition.name,
      sort_order: sortOrder,
      format: definition.format ?? null,
      settings: definition.settings ?? null,
      start_date: startDate,
      is_path: isPath,
    });
    (definition.children || []).forEach((child, index) => addStage(child, id, index + 1));
    if (!isPath) return;
    if (definition.format !== "double_elimination") throw new Error(`Unsupported stage format: ${definition.format}`);
    const templates = buildDoubleEliminationGroup({
      groupSize: definition.settings.group_size,
      grandFinal: definition.settings.grand_final,
    });
    const labels = advanceLabels(definition.settings);
    definition.groups.forEach((groupKey, index) => {
      const gid = groupId(tournamentId, definition.key, groupKey);
      groups.push({ id: gid, stage_id: id, stage_key: definition.key, group_key: groupKey, name: `Group ${groupKey}`, sort_order: index + 1 });
      for (const template of templates) {
        matches.push({
          group_id: gid,
          stage_key: definition.key,
          group_key: groupKey,
          bracket_no: template.bracket_no,
          bracket_side: template.bracket_side,
          round_name: template.round_name,
          round_order: template.week,
          p1_source: template.p1_source,
          p2_source: template.p2_source,
          bracket_label: labels.get(template.bracket_no) || null,
          is_conditional: template.is_conditional,
          duel_format: definition.settings.duel_format,
          deadline_utc: weekDeadlineUtc(startDate, template.week),
        });
      }
    });
  };
  (format.stages || []).forEach((definition, index) => addStage(definition, null, index + 1));
  return { stages, groups, matches };
}

async function loadStages(db, tournamentId) {
  return dbAll(
    db,
    "SELECT * FROM tournament_stages WHERE upper(trim(tournament_id)) = upper(trim(?)) ORDER BY sort_order, stage_key",
    [tournamentId]
  );
}

async function upsertStage(db, tournamentId, stage, actorId) {
  await dbRun(
    db,
    `
      INSERT INTO tournament_stages (
        id, tournament_id, parent_id, stage_key, name, sort_order, format, settings_json, start_date,
        created_by, updated_by
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        parent_id = excluded.parent_id,
        name = excluded.name,
        sort_order = excluded.sort_order,
        format = excluded.format,
        settings_json = excluded.settings_json,
        start_date = excluded.start_date,
        updated_by = excluded.updated_by,
        updated_at = CURRENT_TIMESTAMP
    `,
    [
      stage.id, tournamentId, stage.parent_id, stage.stage_key, stage.name, stage.sort_order, stage.format,
      stage.settings ? JSON.stringify(stage.settings) : null, stage.start_date, actorId, actorId,
    ]
  );
}

// A path can be regenerated while its draw has not been made: every match is still an empty Draft.
async function findPathBlockers(db, stageRowId) {
  return dbAll(
    db,
    `
      SELECT d.id, d.group_name, d.bracket_no
      FROM duels d
      JOIN tournament_groups g ON upper(trim(g.id)) = upper(trim(d.group_id))
      WHERE upper(trim(g.stage_id)) = upper(trim(?))
        AND d.deleted_at IS NULL
        AND (
          COALESCE(trim(d.player_1_id), '') <> '' OR COALESCE(trim(d.player_2_id), '') <> ''
          OR COALESCE(trim(d.time_utc), '') <> '' OR COALESCE(trim(d.status), '') NOT IN ('', 'Draft')
        )
      ORDER BY d.group_name, d.bracket_no
    `,
    [stageRowId]
  );
}

async function replacePathContent(db, tournament, stage, plan, actorId) {
  await dbRun(
    db,
    `DELETE FROM duels WHERE upper(trim(group_id)) IN (
       SELECT upper(trim(id)) FROM tournament_groups WHERE upper(trim(stage_id)) = upper(trim(?))
     )`,
    [stage.id]
  );
  await dbRun(db, "DELETE FROM tournament_groups WHERE upper(trim(stage_id)) = upper(trim(?))", [stage.id]);
  for (const group of plan.groups.filter((item) => item.stage_id === stage.id)) {
    await dbRun(
      db,
      "INSERT INTO tournament_groups (id, stage_id, group_key, name, sort_order) VALUES (?, ?, ?, ?, ?)",
      [group.id, group.stage_id, group.group_key, group.name, group.sort_order]
    );
  }
  const isTest = Number(tournament.is_test) === 1 ? 1 : 0;
  const ranking = Number(tournament.ranking ?? 1) === 0 ? 0 : 1;
  for (const match of plan.matches.filter((item) => item.stage_key === stage.stage_key)) {
    await dbRun(
      db,
      `
        INSERT INTO duels (
          id, tournament_id, match_id, duel_number, duel_format, player_1_id, player_2_id, ranking, status,
          source_type, is_test, stage, group_name, round_name, round_order, deadline_utc,
          group_id, bracket_no, bracket_side, p1_source, p2_source, bracket_label, is_conditional,
          created_by, updated_by
        )
        VALUES (?, ?, NULL, 1, ?, NULL, NULL, ?, 'Draft', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        `${tournament.id}-${randomUUID().slice(0, 8)}`, tournament.id, match.duel_format, ranking,
        CCL_MATCH_SOURCE_TYPE, isTest, match.stage_key, match.group_key, match.round_name, match.round_order,
        match.deadline_utc, match.group_id, match.bracket_no, match.bracket_side, match.p1_source, match.p2_source,
        match.bracket_label, match.is_conditional ? 1 : 0, actorId, actorId,
      ]
    );
  }
}

// Creates the structure, or regenerates the paths that have not been drawn yet.
// startDates: { Q1: "2026-11-23", ... }; a path without a new date keeps its saved one.
export async function generateCclStructure(db, { tournament, startDates = {}, actorId = null }) {
  const format = getCclFormat(tournament.format_key);
  if (!format) throw requestError(400, "Choose the CCL format of the tournament first");
  if (!format.available || !format.stages) throw requestError(400, `${format.name} format is not available yet`);

  const dates = {};
  for (const [key, value] of Object.entries(startDates || {})) {
    const normalized = normalizeStageStartDate(value);
    if (normalized.error) throw requestError(400, `${key}: ${normalized.error}`);
    dates[key] = normalized.value;
  }

  return inTransaction(db, async () => {
    const existing = new Map((await loadStages(db, tournament.id)).map((row) => [text(row.stage_key).toUpperCase(), row]));
    const keepDates = {};
    existing.forEach((row) => { keepDates[row.stage_key] = row.start_date || null; });
    const plan = buildCclStructurePlan({ tournamentId: tournament.id, format, startDates: { ...keepDates, ...dates } });
    const unknown = Object.keys(dates).filter((key) => !plan.stages.some((stage) => stage.is_path && stage.stage_key === key));
    if (unknown.length) throw requestError(400, `Unknown stage in start_dates: ${unknown.join(", ")}`);

    const generated = [];
    const skipped = [];
    for (const stage of plan.stages) {
      const saved = existing.get(stage.stage_key.toUpperCase());
      if (!stage.is_path) {
        await upsertStage(db, tournament.id, stage, actorId);
        continue;
      }
      if (saved && text(saved.status) !== "structure") {
        skipped.push({ stage: stage.stage_key, status: saved.status });
        continue;
      }
      if (saved) {
        const blockers = await findPathBlockers(db, saved.id);
        if (blockers.length) {
          throw requestError(409, `${stage.stage_key} already has players, times or results in its matches`, { blockers });
        }
      }
      await upsertStage(db, tournament.id, stage, actorId);
      await replacePathContent(db, tournament, stage, plan, actorId);
      generated.push(stage.stage_key);
    }
    if (!generated.length) throw requestError(409, "Every stage is already drawn; nothing to regenerate", { skipped });
    return { format_key: format.key, generated, skipped };
  });
}

// New start date of a path: moves the deadlines of all its matches (drawn or not).
export async function updateCclStageStartDate(db, { tournament, stageKey, startDate, actorId = null }) {
  const normalized = normalizeStageStartDate(startDate);
  if (normalized.error) throw requestError(400, normalized.error);
  return inTransaction(db, async () => {
    const stage = (await loadStages(db, tournament.id)).find((row) => text(row.stage_key).toUpperCase() === text(stageKey).toUpperCase());
    if (!stage || !stage.format) throw requestError(404, "Stage not found");
    await dbRun(
      db,
      "UPDATE tournament_stages SET start_date = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
      [normalized.value, actorId, stage.id]
    );
    const duels = await dbAll(
      db,
      `SELECT d.id, d.round_order FROM duels d
       JOIN tournament_groups g ON upper(trim(g.id)) = upper(trim(d.group_id))
       WHERE upper(trim(g.stage_id)) = upper(trim(?)) AND d.deleted_at IS NULL`,
      [stage.id]
    );
    for (const duel of duels) {
      await dbRun(
        db,
        "UPDATE duels SET deadline_utc = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        [weekDeadlineUtc(normalized.value, duel.round_order), actorId, duel.id]
      );
    }
    return { stage: stage.stage_key, start_date: normalized.value, matches_updated: duels.length };
  });
}

function parseSettings(value) {
  try {
    return value ? JSON.parse(value) : null;
  } catch (_error) {
    return null;
  }
}

// The whole structure as a tree: stages → child stages → groups → matches with placeholders.
export async function loadCclStructure(db, tournamentId) {
  const stages = await loadStages(db, tournamentId);
  const groups = await dbAll(
    db,
    `SELECT g.* FROM tournament_groups g
     JOIN tournament_stages s ON upper(trim(s.id)) = upper(trim(g.stage_id))
     WHERE upper(trim(s.tournament_id)) = upper(trim(?))
     ORDER BY g.sort_order, g.group_key`,
    [tournamentId]
  );
  const duels = await dbAll(
    db,
    `SELECT id, group_id, bracket_no, bracket_side, round_name, round_order, deadline_utc, duel_format, time_utc,
            COALESCE(NULLIF(trim(status), ''), 'Draft') AS status, dw1, dw2, player_1_id, player_2_id,
            p1_source, p2_source, bracket_label, is_conditional
     FROM duels
     WHERE upper(trim(tournament_id)) = upper(trim(?)) AND group_id IS NOT NULL AND deleted_at IS NULL
     ORDER BY bracket_no`,
    [tournamentId]
  );
  const profiles = await loadProfilesById(db, duels.flatMap((duel) => [duel.player_1_id, duel.player_2_id]));
  const player = (id, source) => {
    const profile = profiles.get(text(id));
    return {
      id: text(id) || null,
      bga_nickname: profile?.bga_nickname || null,
      placeholder: describeSlotSource(source),
      source,
    };
  };

  const groupNode = (group) => {
    const rows = duels.filter((duel) => text(duel.group_id).toUpperCase() === text(group.id).toUpperCase());
    // The only conditional match is a Group Final Reset, which follows the Group Final right before it.
    const links = buildBracketLinks(rows.map((row) => ({
      ...row,
      is_conditional: Number(row.is_conditional) === 1,
      depends_on: row.bracket_no - 1,
    })));
    return {
      id: group.id,
      group_key: group.group_key,
      name: group.name,
      matches: rows.map((row) => ({
        id: row.id,
        bracket_no: row.bracket_no,
        bracket_side: row.bracket_side,
        round_name: row.round_name,
        round_order: row.round_order,
        deadline_utc: row.deadline_utc || null,
        duel_format: row.duel_format || null,
        time_utc: row.time_utc || null,
        status: row.status,
        dw1: row.dw1 ?? null,
        dw2: row.dw2 ?? null,
        bracket_label: row.bracket_label || null,
        is_conditional: Number(row.is_conditional) === 1,
        player_1: player(row.player_1_id, row.p1_source),
        player_2: player(row.player_2_id, row.p2_source),
        next_win: links.get(row.bracket_no)?.next_win ?? null,
        next_lose: links.get(row.bracket_no)?.next_lose ?? null,
      })),
    };
  };
  const stageNode = (stage) => ({
    id: stage.id,
    stage_key: stage.stage_key,
    name: stage.name,
    format: stage.format || null,
    status: stage.status,
    start_date: stage.start_date || null,
    settings: parseSettings(stage.settings_json),
    children: stages.filter((child) => text(child.parent_id).toUpperCase() === text(stage.id).toUpperCase()).map(stageNode),
    groups: groups.filter((group) => text(group.stage_id).toUpperCase() === text(stage.id).toUpperCase()).map(groupNode),
  });
  return stages.filter((stage) => !text(stage.parent_id)).map(stageNode);
}

export function registerCclStructureRoutes(app, {
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
  const loadAdminTournament = async (req) => {
    const tournament = await loadCclTournament(db, req.params.tournamentId);
    if (!tournament) throw requestError(404, "CCL tournament not found");
    if (!await hasTournamentAdminAccess(db, req.user, tournament.id)) throw requestError(403, "Forbidden");
    return tournament;
  };
  const actorId = (user) => text(user?.player_id ?? user?.bga_id) || null;

  app.get("/ccl/:tournamentId/structure", requireAuthenticated, handle("Failed to load CCL structure", async (req, res) => {
    const tournament = await loadAdminTournament(req);
    return res.json({
      ok: true,
      tournament: { id: tournament.id, name: tournament.name, format_key: tournament.format_key || null },
      stages: await loadCclStructure(db, tournament.id),
    });
  }));

  app.post("/ccl/:tournamentId/structure", requireAuthenticated, handle("Failed to generate CCL structure", async (req, res) => {
    const tournament = await loadAdminTournament(req);
    const result = await generateCclStructure(db, {
      tournament,
      startDates: req.body?.start_dates || {},
      actorId: actorId(req.user),
    });
    audit(req, {
      event_type: "ccl.structure.generated",
      entity_type: "tournament",
      action: "update",
      record_id: tournament.id,
      changes: {},
      metadata: { tournament_id: tournament.id, ...result },
    });
    return res.json({ ok: true, ...result, stages: await loadCclStructure(db, tournament.id) });
  }));

  app.patch("/ccl/:tournamentId/stages/:stageKey", requireAuthenticated, handle("Failed to update CCL stage", async (req, res) => {
    const tournament = await loadAdminTournament(req);
    if (!Object.prototype.hasOwnProperty.call(req.body || {}, "start_date")) throw requestError(400, "start_date is required");
    const result = await updateCclStageStartDate(db, {
      tournament,
      stageKey: req.params.stageKey,
      startDate: req.body.start_date,
      actorId: actorId(req.user),
    });
    audit(req, {
      event_type: "ccl.stage.start_date_updated",
      entity_type: "tournament",
      action: "update",
      record_id: tournament.id,
      changes: {},
      metadata: { tournament_id: tournament.id, ...result },
    });
    return res.json({ ok: true, ...result });
  }));
}

// Whether the tournament already has a generated structure (its format can no longer change).
export async function hasCclStructure(db, tournamentId) {
  const row = await dbGet(
    db,
    "SELECT 1 AS found FROM tournament_stages WHERE upper(trim(tournament_id)) = upper(trim(?)) LIMIT 1",
    [tournamentId]
  );
  return !!row;
}

// A path stage (Q1, Q2, Q3: a stage with groups) of the tournament, by key.
export async function loadCclPathStage(db, tournamentId, stageKey) {
  return dbGet(
    db,
    `SELECT * FROM tournament_stages
     WHERE upper(trim(tournament_id)) = upper(trim(?)) AND upper(trim(stage_key)) = upper(trim(?))
       AND format = 'double_elimination'
     LIMIT 1`,
    [tournamentId, text(stageKey)]
  );
}
