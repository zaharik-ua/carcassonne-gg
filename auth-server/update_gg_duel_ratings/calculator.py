from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class GgRatingAnchors:
    low: float
    high: float


def percentile_inclusive(sorted_values: list[float], percentile: float) -> float:
    if not sorted_values:
        raise ValueError("At least one GG Elo value is required")
    position = (len(sorted_values) - 1) * percentile
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    return lower_value + (upper_value - lower_value) * (position - lower_index)


def build_anchors(ratings: list[float]) -> GgRatingAnchors:
    numeric_ratings = sorted(float(value) for value in ratings if math.isfinite(float(value)))
    if not numeric_ratings:
        raise ValueError("No numeric profiles.gg_elo values were found")
    return GgRatingAnchors(
        low=percentile_inclusive(numeric_ratings, 0.15),
        high=percentile_inclusive(numeric_ratings, 0.95),
    )


def calculate_gg_rating_full(
    player_rating_a: float | None,
    player_rating_b: float | None,
    anchors: GgRatingAnchors,
) -> float | None:
    if player_rating_a is None or player_rating_b is None:
        return None
    player_rating_a = float(player_rating_a)
    player_rating_b = float(player_rating_b)
    if not math.isfinite(player_rating_a) or not math.isfinite(player_rating_b):
        return None

    rating_span = max(anchors.high - anchors.low, 1)
    difference_scale = rating_span * 0.4375
    maximum_score = 5.49
    curve_power = 0.8
    closeness_bonus = 0.15
    match_average = (player_rating_a + player_rating_b) / 2
    normalized_strength = min(1, max(0, (match_average - anchors.low) / rating_span))
    closeness = 1 - min(1, abs(player_rating_a - player_rating_b) / difference_scale)
    calculated_score = min(
        maximum_score,
        maximum_score * normalized_strength**curve_power
        + closeness_bonus * normalized_strength * closeness**curve_power,
    )
    if player_rating_a >= anchors.high and player_rating_b >= anchors.high:
        return 6.0
    return calculated_score


def round_gg_rating(rating_full: float | None) -> int | None:
    if rating_full is None:
        return None
    return math.floor(rating_full + 0.5)
