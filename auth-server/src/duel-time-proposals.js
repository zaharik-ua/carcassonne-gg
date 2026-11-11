// Time agreement between the two players of a duel (duels.proposed_time_*).
// Not tied to a tournament format: CCL uses it first, other individual events can follow.

export const DUEL_TIME_PROPOSAL_STATUSES = Object.freeze({
  PENDING: "pending",
  ACCEPTED: "accepted",
  CHANGE_PENDING: "change_pending",
});
const ACTIVE_PROPOSAL_STATUSES = [DUEL_TIME_PROPOSAL_STATUSES.PENDING, DUEL_TIME_PROPOSAL_STATUSES.CHANGE_PENDING];

const text = (value) => String(value ?? "").trim();
const has = (object, field) => Object.prototype.hasOwnProperty.call(object || {}, field);

export function normalizeUtc(value) {
  const raw = text(value);
  if (!raw) return null;
  const date = new Date(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2})?$/.test(raw) ? `${raw.replace(" ", "T")}Z` : raw);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

export function isPlannedDuel(duel) {
  return text(duel?.status || "Planned").toLowerCase() === "planned";
}

export function isActiveDuelTimeProposal(duel) {
  return ACTIVE_PROPOSAL_STATUSES.includes(text(duel?.proposed_time_status).toLowerCase())
    && !!normalizeUtc(duel?.proposed_time_utc)
    && !!text(duel?.proposed_time_by_player_id);
}

// 1 or 2 for a player of the duel, 0 otherwise.
export function getDuelSide(duel, playerId) {
  const id = text(playerId);
  if (!id) return 0;
  if (text(duel?.player_1_id) === id) return 1;
  if (text(duel?.player_2_id) === id) return 2;
  return 0;
}

// The client sends the proposal it saw; a mismatch means the other player acted meanwhile.
export function isDuelProposalSnapshotCurrent(duel, payload) {
  const input = payload && typeof payload === "object" ? payload : {};
  if (!has(input, "expected_proposed_time_utc")) return true;
  return normalizeUtc(input.expected_proposed_time_utc) === normalizeUtc(duel?.proposed_time_utc)
    && text(input.expected_proposed_time_by_player_id) === text(duel?.proposed_time_by_player_id)
    && text(input.expected_proposed_time_status).toLowerCase() === text(duel?.proposed_time_status).toLowerCase();
}

function denied(status, error) {
  return { status, error };
}

// Proposal or counter-proposal by one of the two players; deadline_utc caps the time.
export function planDuelTimeProposal({ duel, actorPlayerId, proposedTimeUtc, nowMs }) {
  const side = getDuelSide(duel, actorPlayerId);
  if (!side) return denied(403, "Only the players of this match can propose its time");
  if (!isPlannedDuel(duel)) return denied(409, "The time can be changed only for a planned match");
  const proposed = normalizeUtc(proposedTimeUtc);
  if (!proposed) return denied(400, "A valid proposed_time_utc is required");
  if (Date.parse(proposed) <= nowMs) return denied(400, "Proposed match time must be in the future");
  const deadline = normalizeUtc(duel?.deadline_utc);
  if (deadline && Date.parse(proposed) > Date.parse(deadline)) {
    return denied(400, "Proposed match time must be before the round deadline");
  }

  const active = isActiveDuelTimeProposal(duel);
  const ownProposal = active && text(duel.proposed_time_by_player_id) === text(actorPlayerId);
  if (active && normalizeUtc(duel.proposed_time_utc) === proposed) {
    return denied(400, ownProposal ? "Choose a time different from the current proposal" : "Choose a different time for the counter-proposal");
  }
  const currentTime = normalizeUtc(duel?.time_utc);
  if (currentTime === proposed) return denied(400, "Choose a time different from the current match time");

  return {
    value: {
      proposed_time_utc: proposed,
      proposed_time_by_player_id: text(actorPlayerId),
      proposed_time_status: currentTime ? DUEL_TIME_PROPOSAL_STATUSES.CHANGE_PENDING : DUEL_TIME_PROPOSAL_STATUSES.PENDING,
    },
    event: !active
      ? (currentTime ? "duel.time_change_proposed" : "duel.time_proposal.created")
      : (ownProposal ? "duel.time_proposal.updated" : "duel.time_proposal.countered"),
  };
}

// Acceptance by the player who did not make the proposal.
export function planDuelTimeAcceptance({ duel, actorPlayerId, nowMs }) {
  const side = getDuelSide(duel, actorPlayerId);
  if (!side) return denied(403, "Only the players of this match can accept its time");
  if (!isPlannedDuel(duel)) return denied(409, "The time can be changed only for a planned match");
  if (!isActiveDuelTimeProposal(duel)) return denied(409, "There is no pending time proposal for this match");
  if (text(duel.proposed_time_by_player_id) === text(actorPlayerId)) {
    return denied(403, "Only the opponent can accept this proposal");
  }
  const proposed = normalizeUtc(duel.proposed_time_utc);
  if (Date.parse(proposed) <= nowMs) return denied(409, "The proposed match time is no longer available");
  return { value: { time_utc: proposed, proposed_time_status: DUEL_TIME_PROPOSAL_STATUSES.ACCEPTED } };
}
