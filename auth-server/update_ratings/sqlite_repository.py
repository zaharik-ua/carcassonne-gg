from __future__ import annotations

import sqlite3
from pathlib import Path


class SqliteRatingsRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())
        self._ensure_schema()

    def update_duel_rating(self, duel_id: str) -> dict:
        normalized_duel_id = str(duel_id).strip()
        if not normalized_duel_id:
            return {"found": False, "updated": 0, "duel_id": duel_id, "match_id": None}

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                  d.id,
                  d.match_id,
                  p1.bga_elo AS elo1,
                  p2.bga_elo AS elo2
                FROM duels d
                LEFT JOIN profiles p1
                  ON trim(COALESCE(p1.id, '')) = trim(COALESCE(d.player_1_id, ''))
                LEFT JOIN profiles p2
                  ON trim(COALESCE(p2.id, '')) = trim(COALESCE(d.player_2_id, ''))
                WHERE trim(COALESCE(d.id, '')) = trim(?)
                  AND d.deleted_at IS NULL
                LIMIT 1
                """,
                (normalized_duel_id,),
            ).fetchone()
            if row is None:
                return {"found": False, "updated": 0, "duel_id": normalized_duel_id, "match_id": None}

            rating_full = self._calculate_duel_rating_full(
                self._to_int_or_none(row["elo1"]),
                self._to_int_or_none(row["elo2"]),
            )
            rating = self._calculate_duel_rating(rating_full)
            cursor = conn.execute(
                """
                UPDATE duels
                SET
                  rating_full = ?,
                  rating = ?,
                  updated_at = CURRENT_TIMESTAMP
                WHERE trim(COALESCE(id, '')) = trim(?)
                  AND deleted_at IS NULL
                """,
                (rating_full, rating, normalized_duel_id),
            )
            conn.commit()
            return {
                "found": True,
                "updated": int(cursor.rowcount or 0),
                "duel_id": normalized_duel_id,
                "match_id": str(row["match_id"]).strip() if row["match_id"] is not None else None,
                "rating_full": rating_full,
                "rating": rating,
            }

    def update_match_rating(self, match_id: str) -> dict:
        normalized_match_id = str(match_id).strip()
        if not normalized_match_id:
            return {"found": False, "updated": 0, "match_id": match_id, "rating": None}

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id
                FROM matches
                WHERE trim(COALESCE(id, '')) = trim(?)
                  AND deleted_at IS NULL
                LIMIT 1
                """,
                (normalized_match_id,),
            ).fetchone()
            if row is None:
                return {"found": False, "updated": 0, "match_id": normalized_match_id, "rating": None}

            rating = self._calculate_match_rating(conn, match_id=normalized_match_id)
            cursor = conn.execute(
                """
                UPDATE matches
                SET
                  rating = ?,
                  updated_at = CURRENT_TIMESTAMP
                WHERE trim(COALESCE(id, '')) = trim(?)
                  AND deleted_at IS NULL
                """,
                (rating, normalized_match_id),
            )
            conn.commit()
            return {
                "found": True,
                "updated": int(cursor.rowcount or 0),
                "match_id": normalized_match_id,
                "rating": rating,
            }

    def update_match_with_duels(self, match_id: str) -> dict:
        normalized_match_id = str(match_id).strip()
        duel_ids = self.fetch_duel_ids_for_match(match_id=normalized_match_id)
        duel_results = [self.update_duel_rating(duel_id) for duel_id in duel_ids]
        match_result = self.update_match_rating(normalized_match_id)
        return {
            "match": match_result,
            "duels": duel_results,
        }

    def update_planned(self) -> dict:
        planned_duel_ids = self.fetch_planned_duel_ids()
        duel_results = [self.update_duel_rating(duel_id) for duel_id in planned_duel_ids]

        planned_match_ids = self.fetch_planned_match_ids()
        match_results = [self.update_match_rating(match_id) for match_id in planned_match_ids]

        return {
            "planned_duels": duel_results,
            "planned_matches": match_results,
        }

    def update_planned_missing_ratings(self) -> dict:
        planned_duel_ids = self.fetch_planned_duel_ids_missing_ratings()
        duel_results = [self.update_duel_rating(duel_id) for duel_id in planned_duel_ids]

        planned_match_ids = []
        seen_match_ids: set[str] = set()
        for duel_result in duel_results:
            match_id = str(duel_result.get("match_id") or "").strip()
            if not match_id or match_id in seen_match_ids:
                continue
            seen_match_ids.add(match_id)
            planned_match_ids.append(match_id)

        match_results = [self.update_match_rating(match_id) for match_id in planned_match_ids]

        return {
            "planned_duels_missing_ratings": duel_results,
            "planned_matches_for_missing_ratings": match_results,
        }

    def fetch_duel_ids_for_match(self, *, match_id: str) -> list[str]:
        normalized_match_id = str(match_id).strip()
        if not normalized_match_id:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM duels
                WHERE trim(COALESCE(match_id, '')) = trim(?)
                  AND deleted_at IS NULL
                ORDER BY
                  CASE WHEN duel_number IS NULL THEN 1 ELSE 0 END ASC,
                  duel_number ASC,
                  id ASC
                """,
                (normalized_match_id,),
            ).fetchall()
        return [str(row["id"]).strip() for row in rows if row["id"] is not None and str(row["id"]).strip()]

    def fetch_planned_duel_ids(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM duels
                WHERE deleted_at IS NULL
                  AND COALESCE(status, 'Planned') = 'Planned'
                ORDER BY
                  datetime(COALESCE(time_utc, '1970-01-01 00:00:00')) ASC,
                  CASE WHEN duel_number IS NULL THEN 1 ELSE 0 END ASC,
                  duel_number ASC,
                  id ASC
                """
            ).fetchall()
        return [str(row["id"]).strip() for row in rows if row["id"] is not None and str(row["id"]).strip()]

    def fetch_planned_duel_ids_missing_ratings(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT d.id
                FROM duels d
                LEFT JOIN profiles p1
                  ON trim(COALESCE(p1.id, '')) = trim(COALESCE(d.player_1_id, ''))
                LEFT JOIN profiles p2
                  ON trim(COALESCE(p2.id, '')) = trim(COALESCE(d.player_2_id, ''))
                WHERE d.deleted_at IS NULL
                  AND trim(COALESCE(d.player_1_id, '')) <> ''
                  AND trim(COALESCE(d.player_2_id, '')) <> ''
                  AND COALESCE(status, 'Planned') = 'Planned'
                  AND (
                    rating_full IS NULL
                    OR trim(COALESCE(CAST(rating_full AS TEXT), '')) = ''
                    OR rating IS NULL
                    OR trim(COALESCE(CAST(rating AS TEXT), '')) = ''
                  )
                  AND p1.bga_elo IS NOT NULL
                  AND p2.bga_elo IS NOT NULL
                ORDER BY
                  datetime(COALESCE(d.time_utc, '1970-01-01 00:00:00')) ASC,
                  CASE WHEN d.duel_number IS NULL THEN 1 ELSE 0 END ASC,
                  d.duel_number ASC,
                  d.id ASC
                """
            ).fetchall()
        return [str(row["id"]).strip() for row in rows if row["id"] is not None and str(row["id"]).strip()]

    def fetch_planned_match_ids(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM matches
                WHERE deleted_at IS NULL
                  AND COALESCE(status, 'Planned') = 'Planned'
                ORDER BY
                  datetime(COALESCE(time_utc, '1970-01-01 00:00:00')) ASC,
                  id ASC
                """
            ).fetchall()
        return [str(row["id"]).strip() for row in rows if row["id"] is not None and str(row["id"]).strip()]

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            duel_columns = {
                str(row["name"]).strip().lower()
                for row in conn.execute("PRAGMA table_info(duels)").fetchall()
                if row["name"] is not None
            }
            match_columns = {
                str(row["name"]).strip().lower()
                for row in conn.execute("PRAGMA table_info(matches)").fetchall()
                if row["name"] is not None
            }
            if "rating_full" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN rating_full REAL")
            if "rating" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN rating INTEGER")
            if "rating" not in match_columns:
                conn.execute("ALTER TABLE matches ADD COLUMN rating INTEGER")
            conn.commit()

    @staticmethod
    def _calculate_duel_rating_full(elo1: int | None, elo2: int | None) -> float | None:
        if elo1 is None or elo2 is None:
            return None

        pow_value = 1.5
        max_rating = 5.49
        max_elo = 700
        min_elo = 300
        avg = (elo1 + elo2) / 2
        norm_avg = min(max((avg - min_elo) / (max_elo - min_elo), 0), 1)
        delta = abs(elo1 - elo2) / 175
        score = min(
            max_rating,
            (norm_avg**pow_value) * max_rating + norm_avg * ((1 - min(1, delta)) ** pow_value),
        )
        if elo1 >= 700 and elo2 >= 700:
            return 6.0
        return score

    @staticmethod
    def _calculate_duel_rating(duel_rating_full: float | None) -> int | None:
        if duel_rating_full is None:
            return None
        return round(duel_rating_full)

    @staticmethod
    def _calculate_match_rating(conn: sqlite3.Connection, *, match_id: str) -> int | None:
        total_duels_row = conn.execute(
            """
            SELECT COUNT(*) AS total_duels
            FROM duels
            WHERE trim(COALESCE(match_id, '')) = trim(?)
              AND deleted_at IS NULL
            """,
            (match_id,),
        ).fetchone()
        total_duels = int(total_duels_row["total_duels"] or 0) if total_duels_row is not None else 0

        rows = conn.execute(
            """
            SELECT rating_full
            FROM duels
            WHERE trim(COALESCE(match_id, '')) = trim(?)
              AND deleted_at IS NULL
              AND rating_full IS NOT NULL
            """,
            (match_id,),
        ).fetchall()
        duel_ratings = [
            float(row["rating_full"])
            for row in rows
            if row["rating_full"] is not None and str(row["rating_full"]).strip() != ""
        ]
        if not duel_ratings or len(duel_ratings) != total_duels:
            return None
        mean_square = sum(rating * rating for rating in duel_ratings) / len(duel_ratings)
        return round(mean_square**0.5)

    @staticmethod
    def _to_int_or_none(value) -> int | None:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw or raw == "?":
            return None
        return int(float(raw))
