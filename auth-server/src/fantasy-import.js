// Import of Fantasy league prices from the organiser's CSV (Record ID, country, player, qualifyingGroup, stage, Price, ...).
// A dry-run builds a plan; apply re-checks the plan against the database, backs it up and writes it.
// Prices are a history: a new import adds rows with its own effective_from and never overwrites old ones;
// a player whose price did not change gets no new row.
import { createHash } from "node:crypto";
import { mkdir } from "node:fs/promises";
import path from "node:path";

import { loadAssociationsById } from "./ccl-routes.js";
import { loadCurrentPrices, loadLeague, loadLeagueSlots, loadPlayerGroups } from "./fantasy.js";

export const FANTASY_IMPORT_VERSION = "fantasy-prices-v1";
const IMPORT_ACTOR = "import:fantasy-prices";
const REQUIRED_COLUMNS = ["Record ID", "player", "stage", "Price"];

const text = (value) => String(value ?? "").trim().replace(/\s+/g, " ");
const idKey = (value) => text(value).toUpperCase();
const nameKey = (value) => text(value).toLowerCase();

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

function failure(code, message) {
  return Object.assign(new Error(message), { code });
}

const hashOf = (value) => createHash("sha256").update(JSON.stringify(value)).digest("hex");

// RFC 4180: quoted fields, "" inside quotes, CRLF or LF, an optional BOM. Returns objects keyed by the header.
export function parseCsv(source) {
  const input = String(source ?? "").replace(/^﻿/, "");
  const records = [];
  let record = [];
  let field = "";
  let quoted = false;
  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    if (quoted) {
      if (char === "\"" && input[index + 1] === "\"") {
        field += "\"";
        index += 1;
      } else if (char === "\"") {
        quoted = false;
      } else {
        field += char;
      }
    } else if (char === "\"") {
      quoted = true;
    } else if (char === ",") {
      record.push(field);
      field = "";
    } else if (char === "\n" || char === "\r") {
      if (char === "\r" && input[index + 1] === "\n") index += 1;
      record.push(field);
      records.push(record);
      record = [];
      field = "";
    } else {
      field += char;
    }
  }
  if (quoted) throw failure("INVALID_CSV", "The CSV has an unclosed quote");
  if (field !== "" || record.length) {
    record.push(field);
    records.push(record);
  }
  const [header = [], ...body] = records.filter((row) => row.some((value) => text(value) !== ""));
  const columns = header.map(text);
  const missing = REQUIRED_COLUMNS.filter((column) => !columns.includes(column));
  if (missing.length) throw failure("INVALID_CSV", `The CSV has no column ${missing.join(", ")}`);
  return body.map((row) => Object.fromEntries(columns.map((column, index) => [column, row[index] ?? ""])));
}

// The CSV columns the plan keeps, so apply can rebuild it without the file.
export function readPriceRows(csvRows) {
  return csvRows.map((row, index) => ({
    line: index + 2,
    record_id: text(row["Record ID"]),
    player: text(row.player),
    stage: text(row.stage),
    price: text(row.Price),
    country: text(row.country),
    group: text(row.qualifyingGroup),
  }));
}

function parsePrice(value) {
  const price = Number(value);
  if (!text(value) || !Number.isFinite(price) || price <= 0 || !Number.isInteger(price * 2)) return null;
  return price;
}

function planContent(plan) {
  return {
    format_version: plan.format_version,
    version: plan.version,
    league_id: plan.league_id,
    effective_from: plan.effective_from,
    source_rows: plan.source_rows,
    prices: plan.prices,
    unchanged: plan.unchanged,
    issues: plan.issues,
  };
}

/**
 * Dry-run. rows: readPriceRows(...). Matches each row of a league stage to a filled CCL slot of that stage
 * by the player's BGA nickname (case-insensitive) and compares its price with the price at effectiveFrom.
 * issues block the import; warnings (country or group differ from the database) do not.
 */
export async function buildFantasyPricePlan(db, { rows, leagueId, effectiveFrom, source = null }) {
  const league = await loadLeague(db, leagueId);
  if (!league) throw failure("NO_LEAGUE", `No Fantasy league ${leagueId}`);
  const at = new Date(effectiveFrom);
  if (Number.isNaN(at.getTime())) throw failure("INVALID_DATE", `Invalid effective_from: ${effectiveFrom}`);
  const effective = at.toISOString();
  const stageKeys = new Set(league.stages.map(idKey));

  const [slots, associations, groups, current] = await Promise.all([
    loadLeagueSlots(db, league),
    loadAssociationsById(db),
    loadPlayerGroups(db, league.tournament_id),
    loadCurrentPrices(db, league.id, effective),
  ]);
  const currentPrice = new Map([...current].map(([key, row]) => [key, Number(row.price)]));

  const issues = [];
  const warnings = [];
  const prices = [];
  const unchanged = [];
  const skipped = {};
  const usedSlots = new Map();
  for (const row of rows) {
    const where = { line: row.line, record_id: row.record_id, player: row.player, stage: row.stage };
    if (!stageKeys.has(idKey(row.stage))) {
      skipped[row.stage || "(empty)"] = (skipped[row.stage || "(empty)"] || 0) + 1;
      continue;
    }
    const price = parsePrice(row.price);
    if (price === null) {
      issues.push({ code: "INVALID_PRICE", message: `Price must be above 0 in steps of 0.5: "${row.price}"`, ...where });
      continue;
    }
    const matches = slots.filter((slot) => idKey(slot.stage) === idKey(row.stage) && nameKey(slot.bga_nickname) === nameKey(row.player));
    if (matches.length !== 1) {
      issues.push({
        code: matches.length ? "AMBIGUOUS_PLAYER" : "UNMATCHED_PLAYER",
        message: matches.length
          ? `${matches.length} ${row.stage} slots have the nickname "${row.player}"`
          : `No ${row.stage} slot has a player with the BGA nickname "${row.player}"`,
        ...where,
      });
      continue;
    }
    const slot = matches[0];
    if (usedSlots.has(idKey(slot.slot_id))) {
      issues.push({ code: "DUPLICATE_PLAYER", message: `Same player as line ${usedSlots.get(idKey(slot.slot_id))}`, ...where });
      continue;
    }
    usedSlots.set(idKey(slot.slot_id), row.line);

    const association = associations.get(idKey(slot.association_id));
    if (row.country && association && ![association.name, association.id].map(nameKey).includes(nameKey(row.country))) {
      warnings.push({ code: "COUNTRY_MISMATCH", message: `CSV: ${row.country}, site: ${association.name}`, ...where });
    }
    const group = groups.get(`${idKey(slot.stage)}|${slot.player_id}`);
    if (row.group && group && idKey(row.group) !== idKey(group)) {
      warnings.push({ code: "GROUP_MISMATCH", message: `CSV: group ${row.group}, site: group ${group}`, ...where });
    }
    const entry = { slot_id: slot.slot_id, stage: slot.stage, player: slot.bga_nickname, price, source_record_id: row.record_id || null };
    if (currentPrice.get(idKey(slot.slot_id)) === price) unchanged.push(entry);
    else prices.push({ ...entry, old_price: currentPrice.get(idKey(slot.slot_id)) ?? null });
  }
  for (const slot of slots) {
    if (!usedSlots.has(idKey(slot.slot_id)) && !currentPrice.has(idKey(slot.slot_id))) {
      issues.push({
        code: "MISSING_PRICE",
        message: `${slot.stage} player ${slot.bga_nickname || slot.player_id} has no price`,
        slot_id: slot.slot_id,
        stage: slot.stage,
        player: slot.bga_nickname || slot.player_id,
      });
    }
  }

  const plan = {
    format_version: 1,
    version: FANTASY_IMPORT_VERSION,
    league_id: league.id,
    effective_from: effective,
    source,
    source_rows: rows,
    prices,
    unchanged,
    issues,
    warnings,
    summary: {
      csv_rows: rows.length,
      skipped_by_stage: skipped,
      slots_in_stages: slots.length,
      new_prices: prices.length,
      unchanged_prices: unchanged.length,
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
 * so a slot or price changed since the review stops the import.
 */
export async function applyFantasyPricePlan(db, plan, { backupDir, databasePath = "." } = {}) {
  if (plan?.format_version !== 1 || plan.version !== FANTASY_IMPORT_VERSION || plan.plan_hash !== hashOf(planContent(plan))) {
    throw failure("INVALID_PLAN", "Invalid or edited import report; generate a new one with a dry-run");
  }
  const rebuild = () => buildFantasyPricePlan(db, {
    rows: plan.source_rows,
    leagueId: plan.league_id,
    effectiveFrom: plan.effective_from,
    source: plan.source,
  });
  const assertSame = (current) => {
    if (current.issues.length) throw failure("IMPORT_BLOCKED", `Import has unresolved issues: ${[...new Set(current.issues.map((issue) => issue.code))].join(", ")}`);
    if (current.plan_hash !== plan.plan_hash) throw failure("STALE_PLAN", "The database changed after the dry-run; generate and review a new report");
  };
  assertSame(await rebuild());
  if (!plan.prices.length) return { applied: false, league_id: plan.league_id, prices: 0, backup_path: null };

  const backupPath = path.join(
    backupDir || path.join(path.dirname(databasePath), "backups"),
    `${FANTASY_IMPORT_VERSION}-${new Date().toISOString().replaceAll(":", "-")}.sqlite`
  );
  await backupDatabase(db, backupPath);

  await dbRun(db, "BEGIN IMMEDIATE TRANSACTION");
  try {
    assertSame(await rebuild());
    for (const row of plan.prices) {
      await dbRun(
        db,
        `
          INSERT INTO fantasy_prices (league_id, slot_id, stage, price, effective_from, source_record_id, imported_by)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        `,
        [plan.league_id, row.slot_id, row.stage, row.price, plan.effective_from, row.source_record_id, IMPORT_ACTOR]
      );
    }
    const auditTable = await dbAll(db, "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'audit_trail'");
    if (auditTable.length) {
      await dbRun(
        db,
        `
          INSERT INTO audit_trail (event_type, entity_type, action, record_id, actor_email, changes, metadata)
          VALUES ('fantasy_prices.imported', 'fantasy_price', 'create', ?, ?, ?, ?)
        `,
        [
          plan.league_id,
          IMPORT_ACTOR,
          JSON.stringify({ prices: { new: plan.prices.length } }),
          JSON.stringify({
            league_id: plan.league_id,
            effective_from: plan.effective_from,
            source: plan.source,
            plan_hash: plan.plan_hash,
            backup_path: backupPath,
          }),
        ]
      );
    }
    await dbRun(db, "COMMIT");
    return { applied: true, league_id: plan.league_id, prices: plan.prices.length, backup_path: backupPath };
  } catch (error) {
    await dbRun(db, "ROLLBACK").catch(() => {});
    throw error;
  }
}
