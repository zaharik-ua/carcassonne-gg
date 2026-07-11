from __future__ import annotations

import sqlite3
from pathlib import Path


class SqliteProfileGgEloRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())
        self._ensure_schema()

    def load_profile_ids(self) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT trim(COALESCE(id, '')) AS id
                FROM profiles
                WHERE trim(COALESCE(id, '')) <> ''
                  AND deleted_at IS NULL
                """
            ).fetchall()
        return {str(row["id"]).strip() for row in rows}

    def load_active_profile_ids(self) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT trim(COALESCE(id, '')) AS id
                FROM profiles
                WHERE trim(COALESCE(id, '')) <> ''
                  AND deleted_at IS NULL
                  AND lower(trim(COALESCE(status, ''))) = 'active'
                """
            ).fetchall()
        return {str(row["id"]).strip() for row in rows}

    def update_gg_elos(
        self,
        ratings_by_id: dict[str, float],
        positions_by_id: dict[str, int],
    ) -> int:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE TRANSACTION")
            try:
                conn.execute(
                    """
                    UPDATE profiles
                    SET gg_rating_position = NULL
                    WHERE deleted_at IS NULL
                    """
                )
                updated = 0
                for profile_id, gg_elo in ratings_by_id.items():
                    cursor = conn.execute(
                        """
                        UPDATE profiles
                        SET
                          gg_elo = ?,
                          gg_base_elo = COALESCE(gg_base_elo, ?),
                          gg_elo_updated_at = CURRENT_TIMESTAMP,
                          gg_rating_position = ?
                        WHERE trim(COALESCE(id, '')) = ?
                          AND deleted_at IS NULL
                        """,
                        (gg_elo, gg_elo, positions_by_id.get(profile_id), profile_id),
                    )
                    updated += max(0, int(cursor.rowcount))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return updated

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'profiles'"
            ).fetchone()
            if table is None:
                raise RuntimeError(f"profiles table was not found in SQLite DB: {self.db_path}")

            columns = {
                str(row["name"]).strip()
                for row in conn.execute("PRAGMA table_info(profiles)").fetchall()
            }
            required_columns = {"id", "status", "deleted_at"}
            missing = sorted(required_columns - columns)
            if missing:
                raise RuntimeError(f"profiles table is missing required columns: {', '.join(missing)}")
            if "gg_elo" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN gg_elo REAL")
            if "gg_base_elo" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN gg_base_elo REAL")
            if "gg_elo_period_delta" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN gg_elo_period_delta REAL")
            if "gg_elo_updated_at" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN gg_elo_updated_at TEXT")
            if "gg_rating_position" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN gg_rating_position INTEGER")
            conn.execute(
                """
                UPDATE profiles
                SET gg_base_elo = gg_elo
                WHERE gg_base_elo IS NULL
                  AND gg_elo IS NOT NULL
                  AND trim(CAST(gg_elo AS TEXT)) <> ''
                """
            )
            conn.commit()
