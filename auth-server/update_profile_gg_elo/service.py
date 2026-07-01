from __future__ import annotations

import math

from .client import PlayersJsonClient
from .sqlite_repository import SqliteProfileGgEloRepository


class ProfileGgEloUpdateService:
    def __init__(
        self,
        *,
        repository: SqliteProfileGgEloRepository,
        client: PlayersJsonClient,
    ) -> None:
        self.repository = repository
        self.client = client

    def run(self, *, dry_run: bool = False) -> dict:
        payload = self.client.fetch()
        source_rows = payload.get("gg_profiles")
        if not isinstance(source_rows, list):
            raise ValueError("Players JSON must contain a gg_profiles array")

        ratings_by_id: dict[str, float] = {}
        source_order_by_id: dict[str, int] = {}
        skipped_without_id = 0
        skipped_without_rating = 0
        for row in source_rows:
            if not isinstance(row, dict):
                skipped_without_id += 1
                continue
            profile_id = str(row.get("id") or row.get("profile_id") or "").strip()
            if not profile_id:
                skipped_without_id += 1
                continue
            raw_rating = row.get("gg_elo")
            if raw_rating is None or str(raw_rating).strip() == "":
                skipped_without_rating += 1
                continue
            try:
                rating = float(raw_rating)
            except (TypeError, ValueError):
                skipped_without_rating += 1
                continue
            if not math.isfinite(rating):
                skipped_without_rating += 1
                continue
            if profile_id in ratings_by_id:
                raise ValueError(f"Duplicate GG profile id in players JSON: {profile_id}")
            ratings_by_id[profile_id] = rating
            source_order_by_id[profile_id] = len(source_order_by_id)

        profile_ids = self.repository.load_profile_ids()
        active_profile_ids = self.repository.load_active_profile_ids()
        applicable_ratings = {
            profile_id: rating
            for profile_id, rating in ratings_by_id.items()
            if profile_id in profile_ids
        }
        ranked_profile_ids = sorted(
            (profile_id for profile_id in applicable_ratings if profile_id in active_profile_ids),
            key=lambda profile_id: (
                -applicable_ratings[profile_id],
                source_order_by_id[profile_id],
            ),
        )
        positions_by_id = {
            profile_id: index
            for index, profile_id in enumerate(ranked_profile_ids, start=1)
        }
        updated = (
            0
            if dry_run
            else self.repository.update_gg_elos(applicable_ratings, positions_by_id)
        )

        return {
            "ok": True,
            "dry_run": bool(dry_run),
            "source_profiles": len(source_rows),
            "source_profiles_with_rating": len(ratings_by_id),
            "database_profiles": len(profile_ids),
            "matched_profiles": len(applicable_ratings),
            "ranked_active_profiles": len(positions_by_id),
            "updated_profiles": updated,
            "source_profiles_without_database_match": len(set(ratings_by_id) - profile_ids),
            "database_profiles_without_source_rating": len(profile_ids - set(ratings_by_id)),
            "skipped_source_rows_without_id": skipped_without_id,
            "skipped_source_rows_without_rating": skipped_without_rating,
        }
