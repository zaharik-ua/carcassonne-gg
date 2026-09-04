const ROUND_LABELS = Object.freeze({
  round_of_32: "Round of 32",
  round_of_16: "Round of 16",
  quarter_final: "Quarter-final",
  semi_final: "Semi-final",
  bronze_medal_match: "Bronze medal match",
  final: "Final",
});

const MAIN_ROUND_KEYS = Object.freeze([
  "round_of_32",
  "round_of_16",
  "quarter_final",
  "semi_final",
  "final",
]);

const FIRST_ROUND_PARTICIPANTS = Object.freeze({
  round_of_32: 32,
  round_of_16: 16,
  quarter_final: 8,
  semi_final: 4,
});

export class InPersonPlayoffError extends Error {
  constructor(code, message, details = null) {
    super(message);
    this.name = "InPersonPlayoffError";
    this.code = code;
    this.details = details;
  }
}

function playoffError(code, message, details = null) {
  throw new InPersonPlayoffError(code, message, details);
}

function normalizeParticipantId(value) {
  return String(value ?? "").trim();
}

export function getPlayoffRoundLabel(roundKey) {
  return ROUND_LABELS[String(roundKey || "")] || String(roundKey || "").replace(/_/g, " ");
}

export function getPlayoffRoundKeys(firstRound) {
  const normalizedFirstRound = String(firstRound || "").trim().toLowerCase();
  const firstIndex = MAIN_ROUND_KEYS.indexOf(normalizedFirstRound);
  if (firstIndex < 0 || normalizedFirstRound === "final") {
    playoffError(
      "INVALID_PLAYOFF_FIRST_ROUND",
      "playoff_first_round must be round_of_32, round_of_16, quarter_final or semi_final",
      { field: "playoff_first_round" }
    );
  }
  const mainRounds = MAIN_ROUND_KEYS.slice(firstIndex);
  return [...mainRounds.slice(0, -1), "bronze_medal_match", "final"];
}

function validateParticipantSlots(firstRound, participantIds) {
  const required = FIRST_ROUND_PARTICIPANTS[firstRound];
  if (!required) {
    playoffError("INVALID_PLAYOFF_FIRST_ROUND", "The configured playoff first round is invalid");
  }
  if (!Array.isArray(participantIds)) {
    playoffError("PLAYOFF_SLOTS_REQUIRED", "participant_ids must be an array", {
      field: "participant_ids",
      required,
    });
  }
  if (participantIds.length !== required) {
    playoffError(
      "PLAYOFF_SLOTS_INCOMPLETE",
      `Exactly ${required} participant slots must be filled`,
      { field: "participant_ids", required, actual: participantIds.length }
    );
  }
  const normalized = participantIds.map(normalizeParticipantId);
  const missingSlots = normalized
    .map((participantId, index) => (participantId ? null : index + 1))
    .filter(Boolean);
  if (missingSlots.length) {
    playoffError("PLAYOFF_SLOTS_INCOMPLETE", "Every first-round playoff slot must be filled", {
      field: "participant_ids",
      missing_slots: missingSlots,
    });
  }
  const positionsByParticipant = new Map();
  normalized.forEach((participantId, index) => {
    const positions = positionsByParticipant.get(participantId) || [];
    positions.push(index + 1);
    positionsByParticipant.set(participantId, positions);
  });
  const duplicates = Array.from(positionsByParticipant.entries())
    .filter(([, positions]) => positions.length > 1)
    .map(([participantId, positions]) => ({ participant_id: participantId, slots: positions }));
  if (duplicates.length) {
    playoffError(
      "DUPLICATE_PLAYOFF_PARTICIPANT",
      "A participant cannot occupy more than one first-round playoff slot",
      { field: "participant_ids", duplicates }
    );
  }
  return normalized;
}

function matchKey(roundKey, bracketPosition) {
  return `${roundKey}:${bracketPosition}`;
}

/**
 * Builds the complete deterministic single-elimination structure, including
 * Final and the mandatory Bronze medal match. Stable database IDs are assigned
 * by the service; this pure plan links matches by deterministic keys and slots.
 */
export function buildPlayoffBracket({ first_round: firstRound, participant_ids: participantIds } = {}) {
  const normalizedFirstRound = String(firstRound || "").trim().toLowerCase();
  const normalizedParticipantIds = validateParticipantSlots(
    normalizedFirstRound,
    participantIds
  );
  const firstMainIndex = MAIN_ROUND_KEYS.indexOf(normalizedFirstRound);
  const mainRoundKeys = MAIN_ROUND_KEYS.slice(firstMainIndex);
  const orderedRoundKeys = getPlayoffRoundKeys(normalizedFirstRound);
  const roundsByKey = new Map();

  orderedRoundKeys.forEach((roundKey, index) => {
    let matchCount = 1;
    const mainIndex = mainRoundKeys.indexOf(roundKey);
    if (mainIndex >= 0) {
      matchCount = normalizedParticipantIds.length / (2 ** (mainIndex + 1));
    }
    const matches = Array.from({ length: matchCount }, (_, matchIndex) => ({
      key: matchKey(roundKey, matchIndex + 1),
      round_key: roundKey,
      bracket_position: matchIndex + 1,
      table_number: matchIndex + 1,
      participant_a_id: null,
      participant_b_id: null,
      next_match_for_winner_key: null,
      next_match_for_winner_slot: null,
      next_match_for_loser_key: null,
      next_match_for_loser_slot: null,
    }));
    roundsByKey.set(roundKey, {
      round_key: roundKey,
      round_label: getPlayoffRoundLabel(roundKey),
      round_order: index + 1,
      matches,
    });
  });

  const firstRoundPlan = roundsByKey.get(normalizedFirstRound);
  firstRoundPlan.matches.forEach((match, index) => {
    match.participant_a_id = normalizedParticipantIds[index * 2];
    match.participant_b_id = normalizedParticipantIds[(index * 2) + 1];
  });

  mainRoundKeys.slice(0, -1).forEach((roundKey, mainIndex) => {
    const round = roundsByKey.get(roundKey);
    const nextRoundKey = mainRoundKeys[mainIndex + 1];
    round.matches.forEach((match, index) => {
      const nextPosition = Math.floor(index / 2) + 1;
      const nextSlot = index % 2 === 0 ? "participant_a" : "participant_b";
      match.next_match_for_winner_key = matchKey(nextRoundKey, nextPosition);
      match.next_match_for_winner_slot = nextSlot;
      if (roundKey === "semi_final") {
        match.next_match_for_loser_key = matchKey("bronze_medal_match", 1);
        match.next_match_for_loser_slot = nextSlot;
      }
    });
  });

  return {
    first_round: normalizedFirstRound,
    participant_count: normalizedParticipantIds.length,
    participant_ids: normalizedParticipantIds,
    rounds: orderedRoundKeys.map((roundKey) => roundsByKey.get(roundKey)),
  };
}
