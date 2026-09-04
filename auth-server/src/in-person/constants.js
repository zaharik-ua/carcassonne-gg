export const IN_PERSON_SCOPES = Object.freeze(["international", "local"]);

export const IN_PERSON_LOCAL_SUBTYPES = Object.freeze(["final", "qualifier"]);

export const IN_PERSON_PLAYOFF_FIRST_ROUNDS = Object.freeze([
  "round_of_32",
  "round_of_16",
  "quarter_final",
  "semi_final",
]);

export const IN_PERSON_TOURNAMENT_STATUSES = Object.freeze([
  "draft",
  "registration",
  "check_in",
  "swiss",
  "playoff",
  "completed",
  "cancelled",
]);

export const IN_PERSON_DRAW_MODE = "manual_draw_numbers";
export const IN_PERSON_TIEBREAK_PROFILE = "swiss_standard_v1";

const PLAYOFF_PREVIEWS = Object.freeze({
  round_of_32: {
    participant_count: 32,
    rounds: ["Round of 32", "Round of 16", "Quarter-final", "Semi-final", "Bronze medal match", "Final"],
  },
  round_of_16: {
    participant_count: 16,
    rounds: ["Round of 16", "Quarter-final", "Semi-final", "Bronze medal match", "Final"],
  },
  quarter_final: {
    participant_count: 8,
    rounds: ["Quarter-final", "Semi-final", "Bronze medal match", "Final"],
  },
  semi_final: {
    participant_count: 4,
    rounds: ["Semi-final", "Bronze medal match", "Final"],
  },
});

export function getPlayoffPreview(firstRound) {
  const preview = PLAYOFF_PREVIEWS[firstRound];
  if (!preview) return null;
  return {
    participant_count: preview.participant_count,
    rounds: [...preview.rounds],
    includes_bronze_match: true,
  };
}
