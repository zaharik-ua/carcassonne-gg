from __future__ import annotations

import math
import re
from dataclasses import dataclass

from .sqlite_repository import (
    GgEloDuelRatingUpdate,
    SqliteProfileGgEloRepository,
)


INITIAL_ELO = 1500.0
K_FACTOR = 32.0
ELO_SCALE = 400.0


@dataclass
class PlayerRatingState:
    profile_id: str
    base_elo: float
    base_elo_was_missing: bool
    elo: float
    elo_at_delta_start: float
    duels: int = 0
    period_duels: int = 0


class ProfileGgEloUpdateService:
    def __init__(self, *, repository: SqliteProfileGgEloRepository) -> None:
        self.repository = repository

    def run(self, *, dry_run: bool = False) -> dict:
        settings = self.repository.load_rating_settings()
        profiles = self.repository.load_profiles()
        states = {
            profile.profile_id: PlayerRatingState(
                profile_id=profile.profile_id,
                base_elo=_base_elo_or_default(profile.gg_base_elo),
                base_elo_was_missing=profile.gg_base_elo is None,
                elo=_base_elo_or_default(profile.gg_base_elo),
                elo_at_delta_start=_base_elo_or_default(profile.gg_base_elo),
            )
            for profile in profiles
        }

        skipped_unknown_players = 0
        skipped_invalid_scores = 0
        processed_duels = 0
        period_duels = 0
        duel_rating_updates: list[GgEloDuelRatingUpdate] = []

        duels = self.repository.load_duels_after(settings.base_date)
        for duel in duels:
            player_a = states.get(duel.player_1_id)
            player_b = states.get(duel.player_2_id)
            if player_a is None or player_b is None:
                skipped_unknown_players += 1
                continue

            result = infer_result_from_scores(duel.dw1, duel.dw2)
            if result is None:
                skipped_invalid_scores += 1
                continue

            k_multiplier = kyrylo_k(
                wins_winner=result["wins_winner"],
                wins_loser=result["wins_loser"],
                best_of_n=parse_best_of_n(duel.duel_format, duel.dw1, duel.dw2),
            )
            elo_a_before = player_a.elo
            elo_b_before = player_b.elo
            expected_a = expected_score(elo_a_before, elo_b_before)
            expected_b = 1.0 - expected_a

            player_a.elo = elo_a_before + K_FACTOR * (result["score_a"] - expected_a) * k_multiplier
            player_b.elo = elo_b_before + K_FACTOR * (result["score_b"] - expected_b) * k_multiplier
            duel_rating_updates.append(
                GgEloDuelRatingUpdate(
                    duel_id=duel.duel_id,
                    player1_elo_before=round_rating(elo_a_before),
                    player1_elo_after=round_rating(player_a.elo),
                    player2_elo_before=round_rating(elo_b_before),
                    player2_elo_after=round_rating(player_b.elo),
                )
            )
            player_a.duels += 1
            player_b.duels += 1
            processed_duels += 1

            if duel.time_utc <= settings.delta_start_date:
                player_a.elo_at_delta_start = player_a.elo
                player_b.elo_at_delta_start = player_b.elo
            else:
                player_a.period_duels += 1
                player_b.period_duels += 1
                period_duels += 1

        ratings_by_id = {
            profile_id: round_rating(state.elo)
            for profile_id, state in states.items()
        }
        deltas_by_id = {
            profile_id: round_rating(state.elo - state.elo_at_delta_start)
            for profile_id, state in states.items()
        }
        active_profile_ids = {profile.profile_id for profile in profiles if profile.is_active}
        positions_by_id = _build_positions(ratings_by_id, active_profile_ids)
        base_elo_backfills_by_id = {
            profile_id: INITIAL_ELO
            for profile_id, state in states.items()
            if state.base_elo_was_missing and state.duels > 0
        }

        updated = 0 if dry_run else self.repository.update_profile_ratings(
            ratings_by_id=ratings_by_id,
            deltas_by_id=deltas_by_id,
            positions_by_id=positions_by_id,
            base_elo_backfills_by_id=base_elo_backfills_by_id,
            duel_rating_updates=duel_rating_updates,
        )

        return {
            "ok": True,
            "dry_run": bool(dry_run),
            "base_date": settings.base_date.isoformat(),
            "delta_start_date": settings.delta_start_date.isoformat(),
            "profiles": len(profiles),
            "active_profiles_ranked": len(positions_by_id),
            "selected_duels": len(duels),
            "processed_duels": processed_duels,
            "period_duels": period_duels,
            "skipped_duels_unknown_players": skipped_unknown_players,
            "skipped_duels_invalid_scores": skipped_invalid_scores,
            "base_elo_backfills": len(base_elo_backfills_by_id),
            "updated_duel_elo_snapshots": len(duel_rating_updates),
            "updated_profiles": updated,
        }


def infer_result_from_scores(dw1: int | float | None, dw2: int | float | None) -> dict | None:
    score_a = _number_or_none(dw1)
    score_b = _number_or_none(dw2)
    if score_a is None or score_b is None:
        return None

    if score_a > score_b:
        return {"score_a": 1.0, "score_b": 0.0, "wins_winner": score_a, "wins_loser": score_b}
    if score_a < score_b:
        return {"score_a": 0.0, "score_b": 1.0, "wins_winner": score_b, "wins_loser": score_a}
    return {"score_a": 0.5, "score_b": 0.5, "wins_winner": score_a, "wins_loser": score_b}


def expected_score(rating_a: float, rating_b: float) -> float:
    return 1.0 / (1.0 + math.pow(10.0, (rating_b - rating_a) / ELO_SCALE))


def parse_best_of_n(duel_format: str | None, dw1: int | float | None, dw2: int | float | None) -> int:
    raw_format = str(duel_format or "").lower()
    match = re.search(r"\d+", raw_format)
    if match:
        parsed = int(match.group(0))
        if parsed > 0:
            return parsed

    max_wins = max(_number_or_none(dw1) or 0, _number_or_none(dw2) or 0)
    if max_wins == 1:
        return 1
    if max_wins == 2:
        return 3
    if max_wins == 3:
        return 5
    return 3


def kyrylo_k(*, wins_winner: float, wins_loser: float, best_of_n: int) -> float:
    return 1.0 + (wins_winner - wins_loser - 1.0) / best_of_n


def round_rating(value: float) -> float:
    return round(float(value) + 1e-9, 2)


def _base_elo_or_default(value: float | None) -> float:
    numeric = _number_or_none(value)
    return numeric if numeric is not None else INITIAL_ELO


def _number_or_none(value: int | float | str | None) -> float | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        numeric = float(raw)
    except ValueError:
        return None
    return numeric if math.isfinite(numeric) else None


def _build_positions(ratings_by_id: dict[str, float], active_profile_ids: set[str]) -> dict[str, int]:
    ranked_profile_ids = sorted(
        (profile_id for profile_id in ratings_by_id if profile_id in active_profile_ids),
        key=lambda profile_id: (-ratings_by_id[profile_id], profile_id.lower()),
    )
    return {profile_id: index for index, profile_id in enumerate(ranked_profile_ids, start=1)}
