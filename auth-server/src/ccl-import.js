// One-time import of CCL slots from the Google Sheets export (tournaments-open.json)
// into tournament_players + ccl_stage_entries (design section 8).
// A dry-run builds a plan; apply re-checks the plan against the database, backs it up and writes it.
import { createHash, randomUUID } from "node:crypto";
import { mkdir } from "node:fs/promises";
import path from "node:path";

import { normalizeCclQualifiedBy, normalizeCclStage, isCclTournament } from "./ccl-slots.js";

export const CCL_IMPORT_VERSION = "ccl-slots-import-v1";
export const CCL_IMPORT_ACTOR = "import:ccl-slots";
const ASSOCIATION_ID_SQL = "COALESCE(NULLIF(trim(code), ''), printf('ASSOCIATION_%s', rowid))";

const text = (value) => String(value ?? "").trim();
const idKey = (value) => text(value).toUpperCase();

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function dbRun(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, (error) => (error ? reject(error) : resolve()));
  });
}

function failure(code, message) {
  return Object.assign(new Error(message), { code });
}

function hashOf(value) {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

// Everything that decides what apply writes; generated_at and the hash itself are excluded.
function planContent(plan) {
  const { generated_at: _generatedAt, plan_hash: _planHash, ...content } = plan;
  return content;
}

/**
 * Picks the rows of one tournament from the sheet export.
 * The export keeps `countries` ({ team, iso, flag }) used to match associations by ISO code.
 */
export function readCclSourceRows(data, sourceTournamentId) {
  const rows = (Array.isArray(data?.tournament_players) ? data.tournament_players : [])
    .filter((row) => idKey(row?.tournament_id) === idKey(sourceTournamentId))
    .map((row, index) => ({
      source_index: index,
      country: text(row.country),
      stage: text(row.stage),
      qualified_by_name: text(row.qualified_by_name),
      qualified_by_link: text(row.qualified_by_link),
      player_id: text(row.player_id),
      player: text(row.player),
    }));
  const isoByCountry = Object.fromEntries((Array.isArray(data?.countries) ? data.countries : [])
    .filter((country) => text(country?.team) && text(country?.iso))
    .map((country) => [text(country.team).toLowerCase(), text(country.iso)]));
  return { rows, isoByCountry };
}

// "Czech Republic=CZ" → { "czech republic": "CZ" }
export function parseCclAssociationMap(entries = []) {
  const map = {};
  for (const entry of entries) {
    const separator = String(entry).lastIndexOf("=");
    if (separator <= 0) throw failure("INVALID_MAP", `--map expects "Country=ASSOCIATION_CODE", got "${entry}"`);
    map[text(String(entry).slice(0, separator)).toLowerCase()] = text(String(entry).slice(separator + 1));
  }
  return map;
}

async function inspectDatabase(db, tournamentId) {
  const issues = [];
  const columns = (await dbAll(db, "PRAGMA table_info(tournament_players)")).map((column) => column.name);
  const entriesTable = await dbAll(db, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'ccl_stage_entries'");
  const schemaReady = columns.includes("entry_stage") && columns.includes("association_id") && entriesTable.length > 0;
  if (!schemaReady) {
    issues.push({ code: "SCHEMA_MISSING", message: "CCL schema is missing: deploy and restart the new auth-server first" });
  }
  const [tournament] = await dbAll(db, "SELECT * FROM tournaments WHERE upper(trim(id)) = upper(trim(?)) LIMIT 1", [tournamentId]);
  if (!tournament) {
    issues.push({ code: "TOURNAMENT_NOT_FOUND", message: `Tournament ${tournamentId} not found` });
  } else if (!isCclTournament(tournament)) {
    issues.push({ code: "TOURNAMENT_NOT_CCL", message: `Tournament ${tournament.id} must have type Individuals and category CCL` });
  }
  let existingSlots = 0;
  if (schemaReady && tournament) {
    const [row] = await dbAll(
      db,
      "SELECT COUNT(*) AS count FROM tournament_players WHERE upper(trim(tournament_id)) = upper(trim(?)) AND entry_stage IS NOT NULL",
      [tournament.id]
    );
    existingSlots = Number(row?.count) || 0;
    if (existingSlots) {
      issues.push({
        code: "TARGET_HAS_SLOTS",
        message: `Tournament ${tournament.id} already has ${existingSlots} CCL slots; the import runs only into an empty tournament`,
      });
    }
  }
  const associations = await dbAll(db, `SELECT ${ASSOCIATION_ID_SQL} AS id, code, name FROM associations`);
  return { issues, tournament: tournament || null, existingSlots, associations };
}

function createAssociationResolver(associations, { isoByCountry = {}, associationMap = {} } = {}) {
  const byId = new Map(associations.map((association) => [idKey(association.id), association]));
  const byName = new Map(associations.map((association) => [text(association.name).toLowerCase(), association]));
  // Order: explicit --map, association name, association code, the country's ISO code from the sheet.
  return (country) => {
    const key = text(country).toLowerCase();
    if (!key) return null;
    if (associationMap[key]) return byId.get(idKey(associationMap[key])) || null;
    return byName.get(key) || byId.get(idKey(country)) || byId.get(idKey(isoByCountry[key])) || null;
  };
}

async function loadProfiles(db, playerIds) {
  const ids = [...new Set(playerIds.filter(Boolean))];
  if (!ids.length) return new Map();
  const rows = await dbAll(
    db,
    `SELECT trim(id) AS id, bga_nickname, association FROM profiles
     WHERE trim(id) IN (${ids.map(() => "?").join(", ")}) AND deleted_at IS NULL`,
    ids
  );
  return new Map(rows.map((row) => [text(row.id), row]));
}

/**
 * Builds the import plan without writing anything.
 * - issues block the import (missing schema or tournament, slots already present, unmatched country, bad stage or link);
 * - warnings keep the slot but leave it without a player (unknown player, player of another association, duplicate).
 */
export async function buildCclImportPlan(db, {
  rows,
  isoByCountry = {},
  associationMap = {},
  tournamentId,
  sourceTournamentId,
  source = "",
}) {
  const inspected = await inspectDatabase(db, tournamentId);
  const issues = [...inspected.issues];
  const warnings = [];
  const resolveAssociation = createAssociationResolver(inspected.associations, { isoByCountry, associationMap });
  const profiles = await loadProfiles(db, rows.map((row) => row.player_id));
  const usedPlayers = new Map();

  const slots = rows.map((row) => {
    const rowRef = { source_index: row.source_index, country: row.country, stage: row.stage };
    const association = resolveAssociation(row.country);
    if (row.country && !association) {
      issues.push({ ...rowRef, code: "ASSOCIATION_NOT_FOUND", message: `No association matches "${row.country}"; add --map "${row.country}=CODE"` });
    }
    const entryStage = normalizeCclStage(row.stage);
    if (!entryStage) issues.push({ ...rowRef, code: "STAGE_INVALID", message: `Unknown stage "${row.stage}"` });
    const qualifiedBy = normalizeCclQualifiedBy(row);
    if (qualifiedBy.error) issues.push({ ...rowRef, code: "QUALIFIED_BY_INVALID", message: qualifiedBy.error });

    let playerId = null;
    if (row.player_id) {
      const profile = profiles.get(row.player_id);
      const playerAssociation = profile ? resolveAssociation(profile.association) : null;
      const other = usedPlayers.get(row.player_id);
      let warning = null;
      if (!profile) warning = ["PLAYER_NOT_FOUND", `Player ${row.player || row.player_id} (${row.player_id}) has no profile`];
      else if (!association) warning = ["PLAYER_WITHOUT_ASSOCIATION", "A slot without association cannot hold a player"];
      else if (idKey(playerAssociation?.id) !== idKey(association.id)) {
        warning = ["PLAYER_ASSOCIATION_MISMATCH", `Player ${profile.bga_nickname || row.player_id} belongs to "${profile.association || "no association"}"`];
      } else if (other !== undefined) {
        warning = ["PLAYER_DUPLICATE", `Player ${profile.bga_nickname || row.player_id} already takes the slot from source row ${other}`];
      }
      if (warning) {
        warnings.push({ ...rowRef, player_id: row.player_id, code: warning[0], message: `${warning[1]}; the slot is imported without a player` });
      } else {
        playerId = row.player_id;
        usedPlayers.set(row.player_id, row.source_index);
      }
    }

    return {
      source_index: row.source_index,
      association_id: association ? text(association.id) : null,
      association_name: association ? association.name : null,
      entry_stage: entryStage,
      qualified_by_name: qualifiedBy.value?.qualified_by_name ?? row.qualified_by_name,
      qualified_by_link: qualifiedBy.value?.qualified_by_link ?? null,
      player_id: playerId,
    };
  });

  const count = (predicate) => slots.filter(predicate).length;
  const plan = {
    format_version: 1,
    version: CCL_IMPORT_VERSION,
    generated_at: new Date().toISOString(),
    database_path: (await dbAll(db, "PRAGMA database_list")).find((item) => item.name === "main")?.file || "",
    source,
    source_tournament_id: sourceTournamentId,
    tournament_id: inspected.tournament?.id || tournamentId,
    association_map: associationMap,
    iso_by_country: isoByCountry,
    source_rows: rows,
    issues,
    warnings,
    slots,
    summary: {
      source_rows: rows.length,
      slots: slots.length,
      by_stage: Object.fromEntries(["Main", "Q1", "Q2", "Q3"].map((stage) => [stage, count((slot) => slot.entry_stage === stage)])),
      associations: new Set(slots.map((slot) => idKey(slot.association_id)).filter(Boolean)).size,
      without_association: count((slot) => !slot.association_id),
      with_player: count((slot) => slot.player_id),
      issues: issues.length,
      warnings: warnings.length,
      ready: issues.length === 0,
    },
  };
  plan.plan_hash = hashOf(planContent(plan));
  return plan;
}

// VACUUM INTO writes a consistent copy even while the server keeps its connection open.
async function backupDatabase(db, backupPath) {
  await mkdir(path.dirname(backupPath), { recursive: true });
  await dbRun(db, "VACUUM INTO ?", [backupPath]);
}

/**
 * Applies a reviewed dry-run plan. The plan is rebuilt from the current database and must match,
 * so anything changed since the review (associations, profiles, slots) stops the import.
 */
export async function applyCclImportPlan(db, plan, { backupDir } = {}) {
  if (plan?.format_version !== 1 || plan.version !== CCL_IMPORT_VERSION || plan.plan_hash !== hashOf(planContent(plan))) {
    throw failure("INVALID_PLAN", "Invalid or edited import report; generate a new one with a dry-run");
  }
  const rebuild = () => buildCclImportPlan(db, {
    rows: plan.source_rows,
    isoByCountry: plan.iso_by_country,
    associationMap: plan.association_map,
    tournamentId: plan.tournament_id,
    sourceTournamentId: plan.source_tournament_id,
    source: plan.source,
  });
  const assertSame = (current) => {
    if (current.issues.length) throw failure("IMPORT_BLOCKED", `Import has unresolved issues: ${current.issues.map((issue) => issue.code).join(", ")}`);
    if (current.plan_hash !== plan.plan_hash) {
      throw failure("STALE_PLAN", "The database changed after the dry-run; generate and review a new report");
    }
  };
  assertSame(await rebuild());

  const backupPath = path.join(
    backupDir || path.join(path.dirname(plan.database_path || "."), "backups"),
    `${CCL_IMPORT_VERSION}-${new Date().toISOString().replaceAll(":", "-")}-${randomUUID()}.sqlite`
  );
  await backupDatabase(db, backupPath);

  await dbRun(db, "BEGIN IMMEDIATE TRANSACTION");
  try {
    assertSame(await rebuild());
    const slotIds = [];
    for (const slot of plan.slots) {
      const slotId = randomUUID();
      slotIds.push(slotId);
      await dbRun(
        db,
        `
          INSERT INTO tournament_players (
            id, tournament_id, team_id, player_id, captain, association_id, entry_stage,
            created_by, updated_by, created_at, updated_at
          )
          VALUES (?, ?, NULL, ?, 0, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [slotId, plan.tournament_id, slot.player_id, slot.association_id, slot.entry_stage, CCL_IMPORT_ACTOR, CCL_IMPORT_ACTOR]
      );
      await dbRun(
        db,
        `
          INSERT INTO ccl_stage_entries (tournament_id, stage, slot_id, qualified_by_name, qualified_by_link)
          VALUES (?, ?, ?, ?, ?)
        `,
        [plan.tournament_id, slot.entry_stage, slotId, slot.qualified_by_name, slot.qualified_by_link]
      );
    }
    const [stored] = await dbAll(
      db,
      `
        SELECT COUNT(*) AS slots,
               (SELECT COUNT(*) FROM ccl_stage_entries e WHERE upper(trim(e.tournament_id)) = upper(trim(?))) AS entries
        FROM tournament_players
        WHERE upper(trim(tournament_id)) = upper(trim(?)) AND entry_stage IS NOT NULL
      `,
      [plan.tournament_id, plan.tournament_id]
    );
    if (Number(stored?.slots) !== plan.slots.length || Number(stored?.entries) !== plan.slots.length) {
      throw failure("VERIFICATION_FAILED", "Imported slot count does not match the plan; all changes were rolled back");
    }
    const auditTable = await dbAll(db, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'audit_trail'");
    if (auditTable.length) {
      await dbRun(
        db,
        `
          INSERT INTO audit_trail (event_type, entity_type, action, record_id, actor_email, changes, metadata)
          VALUES ('ccl_slots.imported', 'ccl_slot', 'create', ?, ?, ?, ?)
        `,
        [
          plan.tournament_id,
          CCL_IMPORT_ACTOR,
          JSON.stringify({ slots: { old: 0, new: plan.slots.length } }),
          JSON.stringify({
            tournament_id: plan.tournament_id,
            source: plan.source,
            source_tournament_id: plan.source_tournament_id,
            plan_hash: plan.plan_hash,
            backup_path: backupPath,
            slot_ids: slotIds,
          }),
        ]
      );
    }
    await dbRun(db, "COMMIT");
    return { applied: true, tournament_id: plan.tournament_id, slots: plan.slots.length, backup_path: backupPath };
  } catch (error) {
    await dbRun(db, "ROLLBACK").catch(() => {});
    throw error;
  }
}
