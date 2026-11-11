// Rules for CCL player-vs-player matches. A match is a duels row with source_type = 'ccl';
// stage, group, round, deadline and the time proposal are the generic duels columns.
import { normalizeUtc } from "./duel-time-proposals.js";

export const CCL_MATCH_SOURCE_TYPE = "ccl";
export const CCL_MATCH_STAGES = Object.freeze(["Q1", "Q2", "Q3", "Main", "Playoffs"]);
const MAX_TEXT_LENGTH = 80;

const text = (value) => String(value ?? "").trim();
const nullableText = (value) => text(value) || null;
const has = (object, field) => Object.prototype.hasOwnProperty.call(object || {}, field);

export function normalizeCclMatchStage(value) {
  const key = text(value).toLowerCase();
  if (key === "league" || key === "league stage") return "Main";
  return CCL_MATCH_STAGES.find((stage) => stage.toLowerCase() === key) || null;
}

// Admin form input. With `partial`, only the fields present in the body are returned.
export function normalizeCclMatchInput(body, { partial = false } = {}) {
  const input = body && typeof body === "object" ? body : {};
  const value = {};
  const wants = (field) => !partial || has(input, field);

  if (wants("stage")) {
    value.stage = normalizeCclMatchStage(input.stage);
    if (!value.stage) return { error: `stage must be one of ${CCL_MATCH_STAGES.join(", ")}` };
  }
  for (const field of ["group_name", "round_name"]) {
    if (!wants(field)) continue;
    value[field] = nullableText(input[field]);
    if (value[field] && value[field].length > MAX_TEXT_LENGTH) {
      return { error: `${field} must be at most ${MAX_TEXT_LENGTH} characters` };
    }
  }
  if (wants("round_order")) {
    const raw = input.round_order;
    if (raw === undefined || raw === null || raw === "") value.round_order = null;
    else if (!Number.isInteger(Number(raw)) || Number(raw) < 1) return { error: "round_order must be a positive integer" };
    else value.round_order = Number(raw);
  }
  for (const field of ["player_1_id", "player_2_id"]) {
    if (!wants(field)) continue;
    value[field] = nullableText(input[field]);
    if (!value[field]) return { error: `${field} is required` };
  }
  if (wants("duel_format")) {
    value.duel_format = nullableText(input.duel_format);
    if (!value.duel_format) return { error: "duel_format is required" };
  }
  for (const field of ["time_utc", "deadline_utc"]) {
    if (!wants(field)) continue;
    const raw = text(input[field]);
    value[field] = raw ? normalizeUtc(raw) : null;
    if (raw && !value[field]) return { error: `${field} must be a valid UTC timestamp` };
  }
  return { value };
}

// Players of a CCL slot who may meet in a match of this stage.
// Q1-Q3: slots of that stage. Main and Playoffs: Main slots and qualified Q slots.
export function isCclSlotEligibleForStage(slot, stage) {
  if (!slot || !text(slot.player_id)) return false;
  const entryStage = text(slot.entry_stage);
  if (stage === "Main" || stage === "Playoffs") {
    return entryStage === "Main" || text(slot.league_status) === "qualified";
  }
  return entryStage === stage;
}

export function getCclEligiblePlayerIds(slots, stage) {
  return (slots || []).filter((slot) => isCclSlotEligibleForStage(slot, stage)).map((slot) => text(slot.player_id));
}

export function getCclMatchPlayersError({ stage, player1Id, player2Id, slots }) {
  const player1 = text(player1Id);
  const player2 = text(player2Id);
  if (!player1 || !player2) return "Both players are required";
  if (player1 === player2) return "A player cannot play against themselves";
  const eligible = new Set(getCclEligiblePlayerIds(slots, stage));
  const missing = [player1, player2].filter((id) => !eligible.has(id));
  if (!missing.length) return null;
  return stage === "Main" || stage === "Playoffs"
    ? `Not a League Stage player of this tournament: ${missing.join(", ")}`
    : `Not registered for ${stage} in this tournament: ${missing.join(", ")}`;
}

const stageOrder = (stage) => {
  const index = CCL_MATCH_STAGES.indexOf(stage);
  return index < 0 ? CCL_MATCH_STAGES.length : index;
};

// Stage → round → group → time (matches without a time last) → id.
export function compareCclMatches(a, b) {
  const timeOf = (match) => {
    const ts = Date.parse(text(match?.time_utc));
    return Number.isFinite(ts) ? ts : Number.MAX_SAFE_INTEGER;
  };
  return stageOrder(text(a?.stage)) - stageOrder(text(b?.stage))
    || (a?.round_order ?? Number.MAX_SAFE_INTEGER) - (b?.round_order ?? Number.MAX_SAFE_INTEGER)
    || text(a?.round_name).localeCompare(text(b?.round_name))
    || text(a?.group_name).localeCompare(text(b?.group_name))
    || timeOf(a) - timeOf(b)
    || text(a?.id).localeCompare(text(b?.id));
}
