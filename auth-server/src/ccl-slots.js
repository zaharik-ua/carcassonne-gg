import { readTournamentClassification } from "./tournament-classification.js";

export const CCL_STAGES = Object.freeze(["Main", "Q1", "Q2", "Q3"]);
export const CCL_QUALIFICATION_STAGES = Object.freeze(["Q1", "Q2", "Q3"]);
export const CCL_PHASES = Object.freeze({
  qualification: CCL_QUALIFICATION_STAGES,
  league: Object.freeze(["Main"]),
});
export const CCL_LEAGUE_STATUSES = Object.freeze(["qualified", "eliminated"]);
export const CCL_QUALIFICATION_SEEDS = Object.freeze(["Seeded", "Unseeded"]);
export const CCL_LEAGUE_POTS = Object.freeze(["Pot 1", "Pot 2", "Pot 3", "Pot 4", "Pot 5"]);
// Every profile starts at this GG Elo; it means "no official matches yet".
export const CCL_GG_ELO_INITIAL = 1500;
// Each qualification path has its own pots: a fixed number of Seeded players, the rest Unseeded.
export const CCL_QUALIFICATION_POTS = Object.freeze({
  Q1: Object.freeze({ seeded: 20, max_players: 40 }),
  Q2: Object.freeze({ seeded: 16, max_players: 32 }),
  Q3: Object.freeze({ seeded: 12, max_players: 24 }),
});
// League Stage: five pots of 12 players (60 in total).
export const CCL_LEAGUE_POT_SIZE = 12;
// One request may create up to the largest stage (the full League Stage).
const MAX_SLOTS_PER_REQUEST = CCL_LEAGUE_POT_SIZE * CCL_LEAGUE_POTS.length;

const DEADLINE_COLUMNS = Object.freeze({
  Main: "ccl_main_registration_ends_at",
  Q1: "ccl_q12_registration_ends_at",
  Q2: "ccl_q12_registration_ends_at",
  Q3: "ccl_q3_registration_ends_at",
});
// Tournament form fields holding the stage deadlines (Q1 and Q2 share one).
export const CCL_DEADLINE_FIELDS = Object.freeze([...new Set(Object.values(DEADLINE_COLUMNS))]);
const CAPTAIN_SLOT_FIELDS = new Set(["player_id", "qualified_by_name", "qualified_by_link"]);
const ADMIN_SLOT_FIELDS = new Set([...CAPTAIN_SLOT_FIELDS, "association_id", "entry_stage"]);

const text = (value) => String(value ?? "").trim();
const idKey = (value) => text(value).toUpperCase();
const has = (object, field) => Object.prototype.hasOwnProperty.call(object || {}, field);
const nullableText = (value) => text(value) || null;
const compareText = (a, b) => text(a).localeCompare(text(b), undefined, { sensitivity: "base" });

export function isCclTournament(tournament) {
  const { type, category } = readTournamentClassification(tournament || {});
  return type === "Individuals" && category === "CCL";
}

export function normalizeCclStage(value) {
  const key = text(value).toLowerCase();
  return CCL_STAGES.find((stage) => stage.toLowerCase() === key) || null;
}

export function getCclStagePhase(stage) {
  const normalized = normalizeCclStage(stage);
  if (!normalized) return null;
  return normalized === "Main" ? "league" : "qualification";
}

export function getCclStageDeadline(tournament, stage) {
  const column = DEADLINE_COLUMNS[normalizeCclStage(stage)];
  return column ? nullableText(tournament?.[column]) : null;
}

// Same rule as team rosters: no deadline set means registration is open.
export function isCclStageRegistrationOpen(tournament, stage, now = Date.now()) {
  const deadline = Date.parse(getCclStageDeadline(tournament, stage) || "");
  return !Number.isFinite(deadline) || deadline > now;
}

function normalizeLink(value) {
  const link = text(value);
  if (!link) return { value: null };
  try {
    const url = new URL(link);
    if (url.protocol === "http:" || url.protocol === "https:") return { value: url.href };
  } catch {
    // Reported below.
  }
  return { error: "qualified_by_link must be an http(s) URL" };
}

// Returns { value } or { error }; both fields are optional when partial = true.
export function normalizeCclQualifiedBy(input = {}, { partial = false } = {}) {
  const value = {};
  if (!partial || has(input, "qualified_by_name")) {
    const name = text(input.qualified_by_name);
    if (!name) return { error: "qualified_by_name is required" };
    value.qualified_by_name = name;
  }
  if (!partial || has(input, "qualified_by_link")) {
    const link = normalizeLink(input.qualified_by_link);
    if (link.error) return { error: link.error };
    value.qualified_by_link = link.value;
  }
  return { value };
}

// Admin "Add slot" form: association is optional (e.g. Fans Choice slots assigned later).
export function normalizeCclSlotCreateInput(input = {}) {
  const entryStage = normalizeCclStage(input.entry_stage);
  if (!entryStage) return { error: `entry_stage must be one of ${CCL_STAGES.join(", ")}` };
  const qualifiedBy = normalizeCclQualifiedBy(input);
  if (qualifiedBy.error) return qualifiedBy;
  const count = input.count === undefined || input.count === null || input.count === "" ? 1 : Number(input.count);
  if (!Number.isInteger(count) || count < 1 || count > MAX_SLOTS_PER_REQUEST) {
    return { error: `count must be an integer from 1 to ${MAX_SLOTS_PER_REQUEST}` };
  }
  return {
    value: {
      association_id: nullableText(input.association_id),
      entry_stage: entryStage,
      ...qualifiedBy.value,
      count,
    },
  };
}

/**
 * Validates one slot change. Returns an error message or null.
 * - slot: { id, association_id, entry_stage, player_id, league_status }
 * - changes: subset of association_id, entry_stage, player_id, qualified_by_name, qualified_by_link
 * - player: profile { id, association } of the new player_id, when it changes
 * - playerSlotId: another slot of this tournament that already holds that player
 */
export function getCclSlotUpdateError({
  slot,
  changes = {},
  isAdmin = false,
  captainAssociationId = null,
  tournament = null,
  player = null,
  playerSlotId = null,
  now = Date.now(),
}) {
  const allowedFields = isAdmin ? ADMIN_SLOT_FIELDS : CAPTAIN_SLOT_FIELDS;
  const forbiddenField = Object.keys(changes).find((field) => !allowedFields.has(field));
  if (forbiddenField) {
    return isAdmin ? `Unknown slot field: ${forbiddenField}` : `Captains cannot change ${forbiddenField}`;
  }

  const playerChanges = has(changes, "player_id") && text(changes.player_id) !== text(slot.player_id);
  if (!isAdmin) {
    if (!text(slot.association_id) || idKey(slot.association_id) !== idKey(captainAssociationId)) {
      return "Captains can change only slots of their association";
    }
    // Elo issued early does not lock the slot; only the stage deadline does.
    if (!isCclStageRegistrationOpen(tournament, slot.entry_stage, now)) {
      return "Registration for this stage is closed";
    }
  }

  const qualifiedBy = normalizeCclQualifiedBy(changes, { partial: true });
  if (qualifiedBy.error) return qualifiedBy.error;

  const entryStage = has(changes, "entry_stage") ? normalizeCclStage(changes.entry_stage) : slot.entry_stage;
  if (!entryStage) return `entry_stage must be one of ${CCL_STAGES.join(", ")}`;
  if (entryStage === "Main" && slot.league_status) {
    return "Clear the qualification result before moving the slot to Main";
  }

  const associationId = has(changes, "association_id") ? nullableText(changes.association_id) : nullableText(slot.association_id);
  const playerId = has(changes, "player_id") ? nullableText(changes.player_id) : nullableText(slot.player_id);
  if (!playerId) return null;
  if (!associationId) return "A slot without an association cannot have a player";
  if (playerChanges || has(changes, "association_id")) {
    if (!player || text(player.id) !== playerId) return `Player not found: ${playerId}`;
    if (idKey(player.association) !== idKey(associationId)) {
      return `Player ${playerId} does not belong to association ${associationId}`;
    }
  }
  if (playerSlotId && idKey(playerSlotId) !== idKey(slot.id)) {
    return `Player ${playerId} already has a slot in this tournament`;
  }
  return null;
}

// Phases whose pots must be reset by a player change (4.3): the entry phase, and the league if the slot is in it.
export function getCclPhasesAffectedByPlayerChange(slot) {
  const phases = [getCclStagePhase(slot?.entry_stage)].filter(Boolean);
  if (slot?.league_status === "qualified") phases.push("league");
  return phases;
}

export function getCclLeagueResultError({ slot, league_status: leagueStatus, ...qualifiedByInput }) {
  if (!CCL_QUALIFICATION_STAGES.includes(slot?.entry_stage)) {
    return "Qualification results apply only to Q1, Q2 and Q3 slots";
  }
  if (leagueStatus === null || leagueStatus === undefined || leagueStatus === "") return null;
  if (!CCL_LEAGUE_STATUSES.includes(leagueStatus)) {
    return `league_status must be one of ${CCL_LEAGUE_STATUSES.join(", ")} or empty`;
  }
  if (leagueStatus !== "qualified") return null;
  if (!text(slot.player_id)) return "An empty slot cannot qualify for the League Stage";
  return normalizeCclQualifiedBy(qualifiedByInput).error || null;
}

// Slots taking part in a phase: Q slots for qualification; Main slots + qualified Q slots for the league.
export function getCclPhaseSlots(slots, phase) {
  if (phase === "qualification") {
    return (slots || []).filter((slot) => CCL_QUALIFICATION_STAGES.includes(slot.entry_stage));
  }
  if (phase === "league") {
    return (slots || []).filter((slot) => slot.entry_stage === "Main" || slot.league_status === "qualified");
  }
  return [];
}

// Reasons pots cannot be formed yet; for the league every Q slot needs a result.
export function getCclPotsBlockers(slots, phase) {
  const blockers = [];
  if (phase === "league") {
    (slots || [])
      .filter((slot) => CCL_QUALIFICATION_STAGES.includes(slot.entry_stage) && !slot.league_status)
      .forEach((slot) => blockers.push({ slot_id: slot.id, reason: "league_status_pending" }));
  }
  getCclPhaseSlots(slots, phase).forEach((slot) => {
    if (!text(slot.association_id)) blockers.push({ slot_id: slot.id, reason: "association_missing" });
    else if (!text(slot.player_id)) blockers.push({ slot_id: slot.id, reason: "player_missing" });
  });
  return blockers;
}

export function getCclRatedElo(ggElo) {
  if (ggElo === null || ggElo === undefined || text(ggElo) === "") return null;
  const elo = Number(ggElo);
  return Number.isFinite(elo) && elo !== CCL_GG_ELO_INITIAL ? elo : null;
}

/**
 * Rows to write into ccl_stage_entries when the admin sets (or refreshes) Elo; seeds are cleared.
 * Elo may be issued early, before registration ends: slots without a player are skipped.
 */
export function buildCclEloAssignments({ slots, phase, profilesById }) {
  const assignments = [];
  const skipped = [];
  getCclPhaseSlots(slots, phase).forEach((slot) => {
    const playerId = text(slot.player_id);
    if (!playerId) {
      skipped.push({ slot_id: slot.id, reason: "player_missing" });
      return;
    }
    assignments.push({
      slot_id: slot.id,
      stage: phase === "league" ? "Main" : slot.entry_stage,
      player_id: playerId,
      elo: getCclRatedElo(profilesById?.get(playerId)?.gg_elo),
      elo_source: "gg_elo",
      seed: null,
    });
  });
  return { assignments, skipped };
}

// Empty value = clear the Elo of the stage (not set); otherwise a positive number.
export function normalizeCclManualElo(value) {
  if (value === null || value === undefined || text(value) === "") return { value: null };
  const elo = Number(value);
  if (!Number.isFinite(elo) || elo <= 0) {
    return { error: "Manual Elo must be a positive number" };
  }
  return { value: elo };
}

// Elo of an entry counts only while it was set for the player who holds the slot now.
export function isCclEntryEloSet(entry, slot) {
  return !!entry?.fixed_at && !!text(entry.player_id) && text(entry.player_id) === text(slot?.player_id);
}

/**
 * Phase state for admin screens:
 * - elo_set: every phase slot has Elo set for its current player;
 * - pots_formed: in addition, every phase slot has a seed;
 * - pots_blockers: why pots cannot be formed yet.
 */
export function getCclPhaseStatus({ slots, entries, phase }) {
  const stages = CCL_PHASES[phase] || [];
  const phaseEntries = (entries || []).filter((entry) => stages.includes(entry.stage));
  const entriesBySlot = new Map(phaseEntries.map((entry) => [idKey(entry.slot_id), entry]));
  const phaseSlots = getCclPhaseSlots(slots, phase);
  const fixedAt = phaseEntries.map((entry) => text(entry.fixed_at)).filter(Boolean).sort();
  const eloSet = phaseSlots.length > 0
    && phaseSlots.every((slot) => isCclEntryEloSet(entriesBySlot.get(idKey(slot.id)), slot));
  return {
    phase,
    slots: phaseSlots.length,
    filled: phaseSlots.filter((slot) => text(slot.player_id)).length,
    elo_set: eloSet,
    elo_set_at: fixedAt.at(-1) || null,
    pots_formed: eloSet && phaseSlots.every((slot) => !!entriesBySlot.get(idKey(slot.id))?.seed),
    pots_blockers: getCclPotsBlockers(slots, phase),
  };
}

function sortByRating(entries, ratingPositionByPlayerId) {
  const position = (entry) => {
    const value = Number(ratingPositionByPlayerId?.get(text(entry.player_id)));
    return Number.isFinite(value) && value > 0 ? value : Number.POSITIVE_INFINITY;
  };
  return [...entries].sort((a, b) => (
    (b.elo - a.elo)
    || (position(a) - position(b))
    || compareText(a.player_id, b.player_id)
  ));
}

function splitRated(entries) {
  return {
    rated: entries.filter((entry) => entry.elo !== null && entry.elo !== undefined),
    unrated: entries.filter((entry) => entry.elo === null || entry.elo === undefined),
  };
}

function formQualificationSeeds(entries, ratingPositionByPlayerId) {
  const seeds = [];
  for (const stage of CCL_QUALIFICATION_STAGES) {
    const stageEntries = entries.filter((entry) => entry.stage === stage);
    if (!stageEntries.length) continue;
    const { seeded: seededCount, max_players: maxPlayers } = CCL_QUALIFICATION_POTS[stage];
    if (stageEntries.length > maxPlayers) {
      return { error: `${stage} has ${stageEntries.length} players; at most ${maxPlayers} are allowed` };
    }
    const { rated, unrated } = splitRated(stageEntries);
    if (seededCount > rated.length) {
      return { error: `${stage} needs ${seededCount} rated players for Seeded, but has ${rated.length}` };
    }
    sortByRating(rated, ratingPositionByPlayerId).forEach((entry, index) => {
      seeds.push({ slot_id: entry.slot_id, stage, seed: index < seededCount ? "Seeded" : "Unseeded" });
    });
    unrated.forEach((entry) => seeds.push({ slot_id: entry.slot_id, stage, seed: "Unseeded" }));
  }
  return { seeds };
}

function formLeaguePots(entries, ratingPositionByPlayerId) {
  const maxPlayers = CCL_LEAGUE_POT_SIZE * CCL_LEAGUE_POTS.length;
  if (entries.length > maxPlayers) {
    return { error: `League Stage has ${entries.length} players; at most ${maxPlayers} are allowed` };
  }
  const { rated, unrated } = splitRated(entries);
  const lastPot = CCL_LEAGUE_POTS.length - 1;
  // Pots 1-4 are full and rated; Pot 5 takes the rest, including every player without rating.
  const ratedNeeded = CCL_LEAGUE_POT_SIZE * lastPot;
  if (rated.length < ratedNeeded) {
    return { error: `League Stage needs ${ratedNeeded} rated players for Pot 1-${lastPot}, but has ${rated.length}` };
  }
  const seeds = sortByRating(rated, ratingPositionByPlayerId).map((entry, index) => ({
    slot_id: entry.slot_id,
    stage: "Main",
    seed: CCL_LEAGUE_POTS[Math.min(Math.floor(index / CCL_LEAGUE_POT_SIZE), lastPot)],
  }));
  unrated.forEach((entry) => seeds.push({ slot_id: entry.slot_id, stage: "Main", seed: CCL_LEAGUE_POTS[lastPot] }));
  return { seeds };
}

/**
 * Forms pots from Elo already set for the phase. Players without rating always land in the
 * last pot (Unseeded / Pot 5). Qualification uses CCL_QUALIFICATION_POTS per path;
 * the league has five pots of CCL_LEAGUE_POT_SIZE.
 * Returns { seeds: [{ slot_id, stage, seed }] } or { error, blockers? }.
 */
export function formCclPots({ slots, entries, phase, ratingPositionByPlayerId }) {
  const phaseSlots = getCclPhaseSlots(slots, phase);
  const stages = CCL_PHASES[phase];
  if (!stages) return { error: "phase must be qualification or league" };
  const entriesBySlot = new Map(
    (entries || [])
      .filter((entry) => stages.includes(entry.stage))
      .map((entry) => [idKey(entry.slot_id), entry])
  );
  const blockers = getCclPotsBlockers(slots, phase);
  phaseSlots.forEach((slot) => {
    if (text(slot.player_id) && !isCclEntryEloSet(entriesBySlot.get(idKey(slot.id)), slot)) {
      blockers.push({ slot_id: slot.id, reason: "elo_not_set" });
    }
  });
  if (blockers.length) {
    const onlyElo = blockers.every((blocker) => blocker.reason === "elo_not_set");
    return { error: onlyElo ? "Elo must be set for every player of the phase" : "Pots cannot be formed yet", blockers };
  }

  const phaseEntries = phaseSlots.map((slot) => {
    const entry = entriesBySlot.get(idKey(slot.id));
    return { slot_id: slot.id, stage: entry.stage, player_id: entry.player_id, elo: entry.elo ?? null };
  });
  return phase === "league"
    ? formLeaguePots(phaseEntries, ratingPositionByPlayerId)
    : formQualificationSeeds(phaseEntries, ratingPositionByPlayerId);
}

function buildPlayerRow({ slot, entry, profile, association }) {
  const eloSet = isCclEntryEloSet(entry, slot);
  const elo = eloSet ? entry.elo ?? null : null;
  return {
    slot_id: slot.id,
    association_id: nullableText(slot.association_id),
    country: association?.name || "",
    flag: association?.flag || null,
    player_id: nullableText(slot.player_id),
    player: profile ? text(profile.bga_nickname) || text(profile.name) : "",
    avatar: profile?.avatar || null,
    stage: slot.entry_stage,
    league_status: slot.league_status || null,
    qualified_by_name: entry?.qualified_by_name || null,
    qualified_by_link: entry?.qualified_by_link || null,
    elo_fixed: eloSet,
    has_rating: elo !== null,
    elo,
    seed: eloSet ? entry.seed || null : null,
  };
}

const STAGE_ORDER = new Map(CCL_STAGES.map((stage, index) => [stage, index]));

function compareRows(a, b, { byStage }) {
  const unassigned = Number(!a.association_id) - Number(!b.association_id);
  if (unassigned) return unassigned;
  return compareText(a.country || a.association_id, b.country || b.association_id)
    || (byStage ? STAGE_ORDER.get(a.stage) - STAGE_ORDER.get(b.stage) : 0)
    || (Number(!a.player) - Number(!b.player))
    || compareText(a.player, b.player)
    || compareText(a.slot_id, b.slot_id);
}

/**
 * Builds both public lists (section 5 of the design):
 * - all_players: every slot; Elo and seeding only for Q slots, from their entry stage;
 * - league_players: Main slots + qualified Q slots; Qualified by, Elo and pot from the Main entry.
 * Row fields keep the legacy JSON names (country, player, stage, elo, seeding, qualified_by_*).
 * Slots without an association go last (TBD group).
 */
export function buildCclPlayerLists({ slots, entries, profilesById, associationsById }) {
  const entriesBySlotStage = new Map(
    (entries || []).map((entry) => [`${idKey(entry.slot_id)}|${entry.stage}`, entry])
  );
  const rowFor = (slot, stage) => buildPlayerRow({
    slot,
    entry: entriesBySlotStage.get(`${idKey(slot.id)}|${stage}`) || null,
    profile: profilesById?.get(text(slot.player_id)) || null,
    association: associationsById?.get(idKey(slot.association_id)) || null,
  });

  const allPlayers = (slots || []).map((slot) => {
    const { seed, ...row } = rowFor(slot, slot.entry_stage);
    // Main slots show no Elo in this list; seeding = Seeded / Unseeded.
    return slot.entry_stage === "Main"
      ? { ...row, elo_fixed: false, has_rating: false, elo: null, seeding: null }
      : { ...row, seeding: seed };
  });
  const leaguePlayers = getCclPhaseSlots(slots, "league").map((slot) => {
    const { seed, ...row } = rowFor(slot, "Main");
    return { ...row, pot: seed };
  });

  return {
    all_players: allPlayers.sort((a, b) => compareRows(a, b, { byStage: true })),
    league_players: leaguePlayers.sort((a, b) => compareRows(a, b, { byStage: false })),
  };
}
