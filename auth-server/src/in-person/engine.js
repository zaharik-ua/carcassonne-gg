const RESULT_TYPES = new Set(["points", "simple", "time_forfeit", "technical", "bye"]);
const TECHNICAL_REASONS = new Set([
  "withdrawal",
  "disqualification",
  "no_show",
  "admin_decision",
]);

export class InPersonEngineError extends Error {
  constructor(code, message, details = null) {
    super(message);
    this.name = "InPersonEngineError";
    this.code = code;
    this.details = details;
  }
}

function engineError(code, message, details = null) {
  throw new InPersonEngineError(code, message, details);
}

function hasValue(value) {
  return value !== undefined && value !== null && value !== "";
}

function participantId(value) {
  const id = String(value ?? "").trim();
  return id || null;
}

function requireParticipantId(value, field) {
  const id = participantId(value);
  if (!id) engineError("PARTICIPANT_ID_REQUIRED", `${field} is required`, { field });
  return id;
}

function compareCodePoints(left, right) {
  const leftText = String(left);
  const rightText = String(right);
  if (/^(?:0|[1-9]\d*)$/.test(leftText) && /^(?:0|[1-9]\d*)$/.test(rightText)) {
    const leftNumber = BigInt(leftText);
    const rightNumber = BigInt(rightText);
    if (leftNumber < rightNumber) return -1;
    if (leftNumber > rightNumber) return 1;
    return 0;
  }
  return leftText < rightText ? -1 : leftText > rightText ? 1 : 0;
}

export function compareStableParticipantIds(left, right) {
  return compareCodePoints(participantId(left) || "", participantId(right) || "");
}

function requireNormalMatch(match) {
  const participantAId = requireParticipantId(match?.participant_a_id, "participant_a_id");
  const participantBId = requireParticipantId(match?.participant_b_id, "participant_b_id");
  if (participantAId === participantBId) {
    engineError("SAME_PARTICIPANT", "A participant cannot play against themselves");
  }
  const startingParticipantId = requireParticipantId(
    match?.starting_participant_id,
    "starting_participant_id"
  );
  if (![participantAId, participantBId].includes(startingParticipantId)) {
    engineError(
      "INVALID_STARTING_PARTICIPANT",
      "starting_participant_id must belong to this match",
      { field: "starting_participant_id" }
    );
  }
  return { participantAId, participantBId, startingParticipantId };
}

function requireWinner(input, participantAId, participantBId) {
  const winnerParticipantId = requireParticipantId(
    input?.winner_participant_id,
    "winner_participant_id"
  );
  if (![participantAId, participantBId].includes(winnerParticipantId)) {
    engineError("INVALID_WINNER", "winner_participant_id must belong to this match", {
      field: "winner_participant_id",
    });
  }
  return winnerParticipantId;
}

function validateOptionalLoser(input, expectedLoserId) {
  if (!hasValue(input?.loser_participant_id)) return;
  if (participantId(input.loser_participant_id) !== expectedLoserId) {
    engineError("INVALID_LOSER", "loser_participant_id does not match the result", {
      field: "loser_participant_id",
    });
  }
}

function rejectFields(input, fields, resultType) {
  const populated = fields.filter((field) => hasValue(input?.[field]));
  if (populated.length) {
    engineError(
      "MUTUALLY_EXCLUSIVE_RESULT_FIELDS",
      `${populated.join(", ")} cannot be used with result_type ${resultType}`,
      { fields: populated, result_type: resultType }
    );
  }
}

function normalizePoints(value, field) {
  const number = Number(value);
  if (!Number.isInteger(number) || number < 0) {
    engineError("INVALID_POINTS", `${field} must be a non-negative integer`, { field });
  }
  return number;
}

/**
 * Validates one complete result and returns the canonical fields stored on
 * in_person_matches. The function never mutates match or input.
 */
export function validateMatchResult(match, input = {}) {
  const resultType = String(input?.result_type || "").trim().toLowerCase();
  if (!RESULT_TYPES.has(resultType)) {
    engineError("INVALID_RESULT_TYPE", "result_type is invalid", { field: "result_type" });
  }

  if (resultType === "bye") {
    const participantAId = requireParticipantId(match?.participant_a_id, "participant_a_id");
    if (participantId(match?.participant_b_id)) {
      engineError("INVALID_BYE_MATCH", "A bye cannot have a second participant");
    }
    if (!(match?.is_bye === true || Number(match?.is_bye) === 1)) {
      engineError("INVALID_BYE_MATCH", "Only a system bye match can use result_type bye");
    }
    rejectFields(
      input,
      ["points_a", "points_b", "loser_participant_id", "finish_reason"],
      resultType
    );
    if (
      hasValue(input?.winner_participant_id)
      && participantId(input.winner_participant_id) !== participantAId
    ) {
      engineError("INVALID_WINNER", "The bye participant must be the winner");
    }
    return {
      status: "completed",
      is_bye: true,
      result_type: "bye",
      points_a: null,
      points_b: null,
      winner_participant_id: participantAId,
      loser_participant_id: null,
      finish_reason: null,
      admin_note: null,
    };
  }

  const { participantAId, participantBId, startingParticipantId } = requireNormalMatch(match);
  let winnerParticipantId;
  let pointsA = null;
  let pointsB = null;
  let finishReason = null;

  if (resultType === "points") {
    if (!hasValue(input?.points_a) || !hasValue(input?.points_b)) {
      engineError("POINTS_REQUIRED", "points_a and points_b are required for a points result", {
        fields: ["points_a", "points_b"],
      });
    }
    pointsA = normalizePoints(input.points_a, "points_a");
    pointsB = normalizePoints(input.points_b, "points_b");
    rejectFields(input, ["finish_reason"], resultType);
    if (pointsA > pointsB) winnerParticipantId = participantAId;
    else if (pointsB > pointsA) winnerParticipantId = participantBId;
    else {
      winnerParticipantId = startingParticipantId === participantAId
        ? participantBId
        : participantAId;
    }
    if (
      hasValue(input?.winner_participant_id)
      && participantId(input.winner_participant_id) !== winnerParticipantId
    ) {
      engineError(
        "RESULT_WINNER_MISMATCH",
        "winner_participant_id contradicts the points result or second-player tie rule",
        { field: "winner_participant_id", expected: winnerParticipantId }
      );
    }
  } else {
    rejectFields(input, ["points_a", "points_b"], resultType);
    winnerParticipantId = requireWinner(input, participantAId, participantBId);
    if (resultType === "time_forfeit") {
      const suppliedReason = String(input?.finish_reason || "").trim().toLowerCase();
      if (suppliedReason && suppliedReason !== "time_forfeit") {
        engineError("INVALID_FINISH_REASON", "A time forfeit must use finish_reason time_forfeit", {
          field: "finish_reason",
        });
      }
      finishReason = "time_forfeit";
    } else if (resultType === "technical") {
      finishReason = String(input?.finish_reason || "").trim().toLowerCase();
      if (!TECHNICAL_REASONS.has(finishReason)) {
        engineError(
          "INVALID_FINISH_REASON",
          "A technical result requires withdrawal, disqualification, no_show or admin_decision",
          { field: "finish_reason" }
        );
      }
    } else {
      rejectFields(input, ["finish_reason"], resultType);
    }
  }

  const loserParticipantId = winnerParticipantId === participantAId
    ? participantBId
    : participantAId;
  validateOptionalLoser(input, loserParticipantId);
  const adminNote = String(input?.admin_note || "").trim() || null;

  return {
    status: "completed",
    is_bye: false,
    result_type: resultType,
    points_a: pointsA,
    points_b: pointsB,
    winner_participant_id: winnerParticipantId,
    loser_participant_id: loserParticipantId,
    finish_reason: finishReason,
    admin_note: adminNote,
  };
}

function isCompletedSwissRound(round) {
  return String(round?.stage || "swiss") === "swiss"
    && String(round?.status || "") === "completed";
}

function orderedCompletedSwissRounds(rounds) {
  return (Array.isArray(rounds) ? rounds : [])
    .filter(isCompletedSwissRound)
    .slice()
    .sort((left, right) => {
      const numberDifference = Number(left?.round_number || 0) - Number(right?.round_number || 0);
      if (numberDifference) return numberDifference;
      return compareCodePoints(left?.id || "", right?.id || "");
    });
}

function orderedRoundMatches(round) {
  return (Array.isArray(round?.matches) ? round.matches : [])
    .filter((match) => String(match?.status || "") !== "cancelled")
    .slice()
    .sort((left, right) => {
      const tableDifference = Number(left?.table_number || Number.MAX_SAFE_INTEGER)
        - Number(right?.table_number || Number.MAX_SAFE_INTEGER);
      if (tableDifference) return tableDifference;
      return compareCodePoints(left?.id || "", right?.id || "");
    });
}

function createParticipantState(participant) {
  return {
    participant,
    participant_id: requireParticipantId(participant?.id ?? participant?.participant_id, "participant_id"),
    wins: 0,
    opponents: [],
    vp_difference: 0,
    sonneborn_berger: 0,
    bye_count: 0,
    start_sequence: [],
  };
}

function createParticipantStateMap(participants) {
  const stateById = new Map();
  (Array.isArray(participants) ? participants : []).forEach((participant) => {
    const state = createParticipantState(participant);
    if (stateById.has(state.participant_id)) {
      engineError("DUPLICATE_PARTICIPANT_ID", "Participant IDs must be unique", {
        participant_id: state.participant_id,
      });
    }
    stateById.set(state.participant_id, state);
  });
  return stateById;
}

function requireKnownParticipant(stateById, id, field) {
  const normalizedId = requireParticipantId(id, field);
  const state = stateById.get(normalizedId);
  if (!state) {
    engineError("UNKNOWN_PARTICIPANT", `${field} does not reference a known participant`, {
      field,
      participant_id: normalizedId,
    });
  }
  return state;
}

export function compareSwissStandings(left, right) {
  const descendingFields = [
    "wins",
    "solkoff1",
    "solkoff2",
    "vp_difference",
    "sonneborn_berger",
  ];
  for (const field of descendingFields) {
    const difference = Number(right?.[field] || 0) - Number(left?.[field] || 0);
    if (difference) return difference;
  }
  const leftHasBye = Number(left?.bye_count || 0) > 0;
  const rightHasBye = Number(right?.bye_count || 0) > 0;
  if (leftHasBye !== rightHasBye) return leftHasBye ? 1 : -1;
  return compareStableParticipantIds(
    left?.participant_id ?? left?.id,
    right?.participant_id ?? right?.id
  );
}

/**
 * Rebuilds swiss_standard_v1 from completed, non-cancelled Swiss rounds.
 * Sonneborn-Berger intentionally preserves the CHU-2025 rule: for every win,
 * add the opponent's number of wins before that match.
 */
export function calculateSwissStandings({ participants = [], rounds = [] } = {}) {
  const stateById = createParticipantStateMap(participants);
  const startedParticipantIds = new Set();

  for (const round of orderedCompletedSwissRounds(rounds)) {
    for (const match of orderedRoundMatches(round)) {
      if (String(match?.status || "") !== "completed") {
        engineError(
          "INCOMPLETE_MATCH_IN_COMPLETED_ROUND",
          "Every active match in a completed round must be completed",
          { round_id: round?.id || null, match_id: match?.id || null }
        );
      }
      const normalized = validateMatchResult(match, match);
      const participantA = requireKnownParticipant(
        stateById,
        match.participant_a_id,
        "participant_a_id"
      );
      startedParticipantIds.add(participantA.participant_id);

      if (normalized.is_bye) {
        participantA.wins += 1;
        participantA.bye_count += 1;
        participantA.start_sequence.push("B");
        continue;
      }

      const participantB = requireKnownParticipant(
        stateById,
        match.participant_b_id,
        "participant_b_id"
      );
      startedParticipantIds.add(participantB.participant_id);
      participantA.opponents.push(participantB.participant_id);
      participantB.opponents.push(participantA.participant_id);

      const winner = stateById.get(normalized.winner_participant_id);
      const loser = stateById.get(normalized.loser_participant_id);
      winner.sonneborn_berger += loser.wins;
      winner.wins += 1;

      if (normalized.result_type === "points") {
        participantA.vp_difference += normalized.points_a - normalized.points_b;
        participantB.vp_difference += normalized.points_b - normalized.points_a;
      }

      if (participantA.participant_id === participantId(match.starting_participant_id)) {
        participantA.start_sequence.push("F");
        participantB.start_sequence.push("S");
      } else {
        participantA.start_sequence.push("S");
        participantB.start_sequence.push("F");
      }
    }
  }

  const rows = [];
  for (const participantIdValue of startedParticipantIds) {
    const state = stateById.get(participantIdValue);
    const opponentWins = state.opponents.map((opponentId) => stateById.get(opponentId).wins);
    for (let index = 0; index < state.bye_count; index += 1) opponentWins.push(0);
    opponentWins.sort((left, right) => left - right);
    const buchholz = opponentWins.reduce((total, value) => total + value, 0);
    const solkoff1 = opponentWins.length >= 2
      ? opponentWins.slice(1).reduce((total, value) => total + value, 0)
      : 0;
    const solkoff2 = opponentWins.length >= 3
      ? opponentWins.slice(1, -1).reduce((total, value) => total + value, 0)
      : 0;
    rows.push({
      participant_id: state.participant_id,
      wins: state.wins,
      buchholz,
      solkoff1,
      solkoff2,
      vp_difference: state.vp_difference,
      sonneborn_berger: state.sonneborn_berger,
      bye_count: state.bye_count,
      start_sequence: state.start_sequence.join(""),
    });
  }

  rows.sort(compareSwissStandings);
  return rows.map((row, index) => ({ ...row, position: index + 1 }));
}

function isPairingEligible(participant) {
  const status = String(participant?.status || "").trim().toLowerCase();
  return !status || status === "checked_in";
}

function pairingParticipants(participants) {
  const byId = createParticipantStateMap(participants);
  return Array.from(byId.values())
    .map((state) => state.participant)
    .filter(isPairingEligible);
}

function pairingMatch(participantAId, participantBId, startingParticipantId, tableNumber, warnings = []) {
  return {
    table_number: tableNumber,
    participant_a_id: participantAId,
    participant_b_id: participantBId,
    starting_participant_id: startingParticipantId,
    status: "scheduled",
    is_bye: false,
    result_type: null,
    winner_participant_id: null,
    loser_participant_id: null,
    warnings: [...warnings],
  };
}

function pairingBye(participantIdValue) {
  return {
    table_number: null,
    participant_a_id: participantIdValue,
    participant_b_id: null,
    starting_participant_id: null,
    status: "completed",
    is_bye: true,
    result_type: "bye",
    winner_participant_id: participantIdValue,
    loser_participant_id: null,
    warnings: [],
  };
}

function pairingResponse(matches) {
  const warnings = [];
  matches.forEach((match) => {
    match.warnings.forEach((code) => {
      warnings.push({
        code,
        table_number: match.table_number,
        participant_ids: [match.participant_a_id, match.participant_b_id].filter(Boolean),
      });
    });
  });
  return {
    matches,
    bye_participant_id: matches.find((match) => match.is_bye)?.participant_a_id || null,
    warnings,
  };
}

/** Uses only checked-in participants and their actually issued draw numbers. */
export function pairFirstSwissRound({ participants = [] } = {}) {
  const eligible = pairingParticipants(participants);
  if (eligible.length < 2) {
    engineError("NOT_ENOUGH_PARTICIPANTS", "At least two checked-in participants are required");
  }
  const drawNumbers = new Set();
  eligible.forEach((participant) => {
    const drawNumber = Number(participant?.draw_number);
    if (!Number.isInteger(drawNumber) || drawNumber <= 0) {
      engineError("INVALID_DRAW_NUMBER", "Every checked-in participant needs a positive draw number", {
        participant_id: participantId(participant?.id ?? participant?.participant_id),
      });
    }
    if (drawNumbers.has(drawNumber)) {
      engineError("DUPLICATE_DRAW_NUMBER", "Draw numbers must be unique", { draw_number: drawNumber });
    }
    drawNumbers.add(drawNumber);
  });

  const ordered = eligible.slice().sort((left, right) => {
    const difference = Number(left.draw_number) - Number(right.draw_number);
    if (difference) return difference;
    return compareStableParticipantIds(left?.id ?? left?.participant_id, right?.id ?? right?.participant_id);
  });
  let bye = null;
  if (ordered.length % 2 === 1) bye = ordered.pop();
  const half = ordered.length / 2;
  const matches = [];
  for (let index = 0; index < half; index += 1) {
    const participantAId = participantId(ordered[index]?.id ?? ordered[index]?.participant_id);
    const participantBId = participantId(ordered[half + index]?.id ?? ordered[half + index]?.participant_id);
    matches.push(pairingMatch(participantAId, participantBId, participantAId, index + 1));
  }
  if (bye) matches.push(pairingBye(participantId(bye?.id ?? bye?.participant_id)));
  return pairingResponse(matches);
}

function normalPairKey(left, right) {
  const leftId = participantId(left) || "";
  const rightId = participantId(right) || "";
  return compareStableParticipantIds(leftId, rightId) <= 0
    ? `${leftId}\u0000${rightId}`
    : `${rightId}\u0000${leftId}`;
}

function completedMatchHistory(rounds) {
  const playedPairs = new Set();
  const starterStats = new Map();
  const lastRoundByes = new Set();
  const completedRounds = orderedCompletedSwissRounds(rounds);
  const latestRoundNumber = completedRounds.length
    ? Number(completedRounds[completedRounds.length - 1]?.round_number || 0)
    : 0;

  function statsFor(id) {
    if (!starterStats.has(id)) {
      starterStats.set(id, { first: 0, second: 0, last: null, streak: 0 });
    }
    return starterStats.get(id);
  }

  function addRole(id, role) {
    const stats = statsFor(id);
    if (role === "first") stats.first += 1;
    else stats.second += 1;
    stats.streak = stats.last === role ? stats.streak + 1 : 1;
    stats.last = role;
  }

  completedRounds.forEach((round) => {
    orderedRoundMatches(round).forEach((match) => {
      if (String(match?.status || "") !== "completed") return;
      const participantAId = participantId(match?.participant_a_id);
      const participantBId = participantId(match?.participant_b_id);
      if (!participantAId || !participantBId) {
        if (Number(round?.round_number || 0) === latestRoundNumber && participantAId) {
          lastRoundByes.add(participantAId);
        }
        return;
      }
      playedPairs.add(normalPairKey(participantAId, participantBId));
      const starterId = participantId(match?.starting_participant_id);
      if (starterId === participantAId) {
        addRole(participantAId, "first");
        addRole(participantBId, "second");
      } else if (starterId === participantBId) {
        addRole(participantAId, "second");
        addRole(participantBId, "first");
      }
    });
  });
  return { playedPairs, starterStats, lastRoundByes };
}

function roleResult(stats, role) {
  const current = stats || { first: 0, second: 0, last: null, streak: 0 };
  const first = current.first + (role === "first" ? 1 : 0);
  const second = current.second + (role === "second" ? 1 : 0);
  return {
    triple: current.last === role && current.streak >= 2,
    imbalance: Math.abs(first - second) > 2,
    absoluteDifference: Math.abs(first - second),
  };
}

function orientationOptions(left, right, starterStats) {
  const options = [
    {
      starter_id: left.id,
      left: roleResult(starterStats.get(left.id), "first"),
      right: roleResult(starterStats.get(right.id), "second"),
    },
    {
      starter_id: right.id,
      left: roleResult(starterStats.get(left.id), "second"),
      right: roleResult(starterStats.get(right.id), "first"),
    },
  ];
  options.forEach((option) => {
    option.tripleCount = Number(option.left.triple) + Number(option.right.triple);
    option.imbalanceCount = Number(option.left.imbalance) + Number(option.right.imbalance);
    option.totalDifference = option.left.absoluteDifference + option.right.absoluteDifference;
  });
  return options.sort((first, second) => (
    first.tripleCount - second.tripleCount
    || first.imbalanceCount - second.imbalanceCount
    || first.totalDifference - second.totalDifference
    || compareStableParticipantIds(first.starter_id, second.starter_id)
  ));
}

function orientationAllowed(left, right, starterStats, pass) {
  return orientationOptions(left, right, starterStats).some((option) => (
    (pass.allowTriple || option.tripleCount === 0)
    && (pass.allowImbalance || option.imbalanceCount === 0)
  ));
}

function selectOrientation(left, right, starterStats) {
  return orientationOptions(left, right, starterStats)[0];
}

function edgePreference(left, right, starterStats) {
  const orientation = selectOrientation(left, right, starterStats);
  return [
    Math.abs(left.wins - right.wins),
    orientation.tripleCount,
    orientation.imbalanceCount,
    orientation.totalDifference,
    Math.abs(left.rank_index - right.rank_index),
    right.rank_index,
  ];
}

function comparePreference(left, right) {
  const size = Math.max(left.length, right.length);
  for (let index = 0; index < size; index += 1) {
    const difference = Number(left[index] || 0) - Number(right[index] || 0);
    if (difference) return difference;
  }
  return 0;
}

function greedyPerfectMatching(players, adjacency) {
  const unpaired = new Set(players.map((_, index) => index));
  const pairs = [];
  while (unpaired.size) {
    const leftIndex = unpaired.values().next().value;
    const rightIndex = adjacency[leftIndex].find((candidate) => unpaired.has(candidate));
    if (rightIndex === undefined) return null;
    pairs.push([leftIndex, rightIndex]);
    unpaired.delete(leftIndex);
    unpaired.delete(rightIndex);
  }
  return pairs;
}

// Edmonds' blossom algorithm. It is used only when the preferred greedy path
// cannot complete the round, so dense, normal tournament rounds stay cheap.
function blossomMaximumMatching(adjacency) {
  const size = adjacency.length;
  const match = Array(size).fill(-1);
  const parent = Array(size).fill(-1);
  const base = Array.from({ length: size }, (_, index) => index);
  const used = Array(size).fill(false);
  const blossom = Array(size).fill(false);

  function lowestCommonAncestor(first, second) {
    const path = Array(size).fill(false);
    let current = first;
    while (true) {
      current = base[current];
      path[current] = true;
      if (match[current] === -1) break;
      current = parent[match[current]];
    }
    current = second;
    while (true) {
      current = base[current];
      if (path[current]) return current;
      current = parent[match[current]];
    }
  }

  function markPath(vertex, blossomBase, child) {
    let current = vertex;
    let nextChild = child;
    while (base[current] !== blossomBase) {
      blossom[base[current]] = true;
      blossom[base[match[current]]] = true;
      parent[current] = nextChild;
      nextChild = match[current];
      current = parent[match[current]];
    }
  }

  function findAugmentingPath(root) {
    used.fill(false);
    parent.fill(-1);
    for (let index = 0; index < size; index += 1) base[index] = index;
    const queue = [root];
    used[root] = true;
    for (let head = 0; head < queue.length; head += 1) {
      const vertex = queue[head];
      for (const candidate of adjacency[vertex]) {
        if (base[vertex] === base[candidate] || match[vertex] === candidate) continue;
        if (
          candidate === root
          || (match[candidate] !== -1 && parent[match[candidate]] !== -1)
        ) {
          const blossomBase = lowestCommonAncestor(vertex, candidate);
          blossom.fill(false);
          markPath(vertex, blossomBase, candidate);
          markPath(candidate, blossomBase, vertex);
          for (let index = 0; index < size; index += 1) {
            if (!blossom[base[index]]) continue;
            base[index] = blossomBase;
            if (!used[index]) {
              used[index] = true;
              queue.push(index);
            }
          }
        } else if (parent[candidate] === -1) {
          parent[candidate] = vertex;
          if (match[candidate] === -1) {
            let current = candidate;
            while (current !== -1) {
              const previous = parent[current];
              const next = previous === -1 ? -1 : match[previous];
              match[current] = previous;
              if (previous !== -1) match[previous] = current;
              current = next;
            }
            return true;
          }
          const matched = match[candidate];
          used[matched] = true;
          queue.push(matched);
        }
      }
    }
    return false;
  }

  for (let vertex = 0; vertex < size; vertex += 1) {
    if (match[vertex] === -1) findAugmentingPath(vertex);
  }
  return match;
}

function perfectMatchingForPass(players, canPair, starterStats) {
  const adjacency = players.map((left, leftIndex) => players
    .map((right, rightIndex) => ({ right, rightIndex }))
    .filter(({ right, rightIndex }) => rightIndex !== leftIndex && canPair(left, right))
    .sort((first, second) => (
      comparePreference(
        edgePreference(left, first.right, starterStats),
        edgePreference(left, second.right, starterStats)
      )
      || compareStableParticipantIds(first.right.id, second.right.id)
    ))
    .map(({ rightIndex }) => rightIndex));

  const greedy = greedyPerfectMatching(players, adjacency);
  if (greedy) return greedy;
  const match = blossomMaximumMatching(adjacency);
  if (match.some((value) => value === -1)) return null;
  const pairs = [];
  match.forEach((rightIndex, leftIndex) => {
    if (leftIndex < rightIndex) pairs.push([leftIndex, rightIndex]);
  });
  return pairs.length * 2 === players.length ? pairs : null;
}

function standingParticipantId(row) {
  return participantId(row?.participant_id ?? row?.id);
}

function selectNextRoundBye(players, lastRoundByes) {
  const withoutPreviousBye = players.filter((player) => player.bye_count === 0);
  if (!withoutPreviousBye.length) {
    engineError(
      "NO_BYE_ELIGIBLE_PARTICIPANT",
      "Every active participant has already received a bye; this policy is outside MVP"
    );
  }
  const withoutRecentBye = withoutPreviousBye.filter((player) => !lastRoundByes.has(player.id));
  const pool = withoutRecentBye.length ? withoutRecentBye : withoutPreviousBye;
  return pool.slice().sort((left, right) => (
    right.rank_index - left.rank_index
    || compareStableParticipantIds(right.id, left.id)
  ))[0];
}

/**
 * Pairs a subsequent Swiss round. Constraints are relaxed deterministically:
 * starter imbalance, third identical start, and only then rematches.
 */
export function pairNextSwissRound({ participants = [], standings = [], rounds = [] } = {}) {
  const eligible = pairingParticipants(participants);
  if (!eligible.length) {
    engineError("NOT_ENOUGH_PARTICIPANTS", "At least one checked-in participant is required");
  }
  const participantById = new Map(
    eligible.map((participant) => [
      participantId(participant?.id ?? participant?.participant_id),
      participant,
    ])
  );
  const orderedStandings = (Array.isArray(standings) ? standings : [])
    .slice()
    .sort((left, right) => {
      const leftPosition = Number(left?.position);
      const rightPosition = Number(right?.position);
      if (
        Number.isInteger(leftPosition)
        && leftPosition > 0
        && Number.isInteger(rightPosition)
        && rightPosition > 0
        && leftPosition !== rightPosition
      ) {
        return leftPosition - rightPosition;
      }
      return compareSwissStandings(left, right);
    });
  const standingById = new Map();
  orderedStandings.forEach((row, index) => {
    const id = standingParticipantId(row);
    if (!id) {
      engineError("PARTICIPANT_ID_REQUIRED", "Every standings row needs a participant_id", {
        field: "participant_id",
      });
    }
    if (standingById.has(id)) {
      engineError("DUPLICATE_STANDINGS_PARTICIPANT", "Standings participant IDs must be unique", {
        participant_id: id,
      });
    }
    standingById.set(id, { row, index });
  });
  const players = [];
  for (const [id] of participantById) {
    const standing = standingById.get(id);
    if (!standing) {
      engineError("MISSING_STANDINGS_PARTICIPANT", "Every active participant needs a standings row", {
        participant_id: id,
      });
    }
    players.push({
      id,
      wins: Number(standing.row?.wins || 0),
      bye_count: Number(standing.row?.bye_count || 0),
      rank_index: standing.index,
    });
  }
  players.sort((left, right) => left.rank_index - right.rank_index);

  const { playedPairs, starterStats, lastRoundByes } = completedMatchHistory(rounds);
  let bye = null;
  let pairingPool = players;
  if (pairingPool.length % 2 === 1) {
    bye = selectNextRoundBye(pairingPool, lastRoundByes);
    pairingPool = pairingPool.filter((player) => player.id !== bye.id);
  }

  const passes = [
    { allowRematch: false, allowTriple: false, allowImbalance: false },
    { allowRematch: false, allowTriple: false, allowImbalance: true },
    { allowRematch: false, allowTriple: true, allowImbalance: true },
    { allowRematch: true, allowTriple: false, allowImbalance: false },
    { allowRematch: true, allowTriple: false, allowImbalance: true },
    { allowRematch: true, allowTriple: true, allowImbalance: true },
  ];
  let indexPairs = null;
  for (const pass of passes) {
    indexPairs = perfectMatchingForPass(
      pairingPool,
      (left, right) => (
        (pass.allowRematch || !playedPairs.has(normalPairKey(left.id, right.id)))
        && orientationAllowed(left, right, starterStats, pass)
      ),
      starterStats
    );
    if (indexPairs) break;
  }
  if (!indexPairs) engineError("PAIRING_FAILED", "Could not create a complete Swiss pairing");

  const paired = indexPairs.map(([leftIndex, rightIndex]) => {
    const left = pairingPool[leftIndex];
    const right = pairingPool[rightIndex];
    return [left, right].sort((first, second) => (
      first.rank_index - second.rank_index || compareStableParticipantIds(first.id, second.id)
    ));
  }).sort((first, second) => (
    first[0].rank_index - second[0].rank_index
    || first[1].rank_index - second[1].rank_index
  ));

  const matches = paired.map(([left, right], index) => {
    const orientation = selectOrientation(left, right, starterStats);
    const warnings = [];
    if (playedPairs.has(normalPairKey(left.id, right.id))) warnings.push("rematch");
    if (orientation.tripleCount > 0) warnings.push("starter_3_in_a_row");
    if (orientation.imbalanceCount > 0) warnings.push("starter_imbalance");
    return pairingMatch(left.id, right.id, orientation.starter_id, index + 1, warnings);
  });
  if (bye) matches.push(pairingBye(bye.id));
  return pairingResponse(matches);
}
