import { getPlayoffRoundLabel } from "./playoff.js";

export function serializePublicTournament(row) {
  if (!row) return null;
  return {
    id: row.id,
    slug: row.slug,
    name_en: row.name_en,
    name_local: row.name_local || null,
    scope: row.scope,
    association_id: row.association_id || null,
    association_name: row.association_name || null,
    association_flag: row.association_flag || null,
    local_subtype: row.local_subtype || null,
    qualifier_city_id: row.qualifier_city_id || null,
    qualifier_city_name_en: row.qualifier_city_name_en || null,
    qualifier_city_name_local: row.qualifier_city_name_local || null,
    start_date: row.start_date,
    end_date: row.end_date,
    organizer_name: row.organizer_name,
    organizer_url: row.organizer_url || null,
    rules_url: row.rules_url || null,
    swiss_rounds_count: Number(row.swiss_rounds_count),
    playoff_first_round: row.playoff_first_round,
    status: row.status,
    published_at: row.published_at || null,
    completed_at: row.completed_at || null,
  };
}

export function serializePublicParticipant(row) {
  if (!row) return null;
  return {
    id: row.id,
    name_en: row.name_en,
    name_local: row.name_local || null,
    bga_nickname: row.bga_nickname || null,
    association_id: row.association_id || row.city_association_id || null,
    association_name: row.association_name || null,
    association_flag: row.association_flag || null,
    city_id: row.city_id || null,
    city_name_en: row.city_name_en || null,
    city_name_local: row.city_name_local || null,
    city_icon_url: row.city_icon_url || null,
    status: row.status,
  };
}

export function serializePublicStanding(row) {
  if (!row) return null;
  return {
    participant_id: row.participant_id,
    participant_name_en: row.participant_name_en || null,
    participant_name_local: row.participant_name_local || null,
    position: Number(row.position),
    wins: Number(row.wins),
    solkoff1: Number(row.solkoff1),
    solkoff2: Number(row.solkoff2),
    vp_difference: Number(row.vp_difference),
  };
}

export function serializePublicMatch(row) {
  if (!row) return null;
  return {
    id: row.id,
    round_id: row.round_id,
    bracket_position: row.bracket_position == null ? null : Number(row.bracket_position),
    table_number: row.table_number == null ? null : Number(row.table_number),
    participant_a_id: row.participant_a_id || null,
    participant_a_name_en: row.participant_a_name_en || null,
    participant_a_name_local: row.participant_a_name_local || null,
    participant_b_id: row.participant_b_id || null,
    participant_b_name_en: row.participant_b_name_en || null,
    participant_b_name_local: row.participant_b_name_local || null,
    starting_participant_id: row.starting_participant_id || null,
    status: row.status,
    is_bye: Number(row.is_bye) === 1,
    result_type: row.result_type || null,
    points_a: row.points_a == null ? null : Number(row.points_a),
    points_b: row.points_b == null ? null : Number(row.points_b),
    winner_participant_id: row.winner_participant_id || null,
    loser_participant_id: row.loser_participant_id || null,
    finish_reason: row.finish_reason || null,
    next_match_for_winner_id: row.next_match_for_winner_id || null,
    next_match_for_winner_slot: row.next_match_for_winner_slot || null,
    next_match_for_loser_id: row.next_match_for_loser_id || null,
    next_match_for_loser_slot: row.next_match_for_loser_slot || null,
  };
}

export function serializePublicRound(row, matches = []) {
  if (!row) return null;
  return {
    id: row.id,
    stage: row.stage,
    round_number: row.round_number == null ? null : Number(row.round_number),
    round_key: row.round_key || null,
    round_label: row.stage === "playoff" ? getPlayoffRoundLabel(row.round_key) : null,
    round_order: row.round_order == null ? null : Number(row.round_order),
    status: row.status,
    published_at: row.published_at || null,
    completed_at: row.completed_at || null,
    matches,
  };
}

export function publicPlayoffPlacements(rounds = []) {
  const finalMatch = rounds.find((round) => round.round_key === "final")?.matches?.[0];
  const bronzeMatch = rounds.find((round) => (
    round.round_key === "bronze_medal_match"
  ))?.matches?.[0];
  if (finalMatch?.status !== "completed" || bronzeMatch?.status !== "completed") return null;
  return {
    first: finalMatch.winner_participant_id,
    second: finalMatch.loser_participant_id,
    third: bronzeMatch.winner_participant_id,
    fourth: bronzeMatch.loser_participant_id,
  };
}
