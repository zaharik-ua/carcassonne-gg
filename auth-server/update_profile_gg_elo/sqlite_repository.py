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

    def update_gg_elos(self, ratings_by_id: dict[str, float]) -> int:
        if not ratings_by_id:
            return 0

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE TRANSACTION")
            try:
                updated = 0
                for profile_id, gg_elo in ratings_by_id.items():
                    cursor = conn.execute(
                        """
                        UPDATE profiles
                        SET
                          gg_elo = ?,
                          gg_elo_updated_at = CURRENT_TIMESTAMP
                        WHERE trim(COALESCE(id, '')) = ?
                          AND deleted_at IS NULL
                        """,
                        (gg_elo, profile_id),
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
            required_columns = {"id", "deleted_at"}
            missing = sorted(required_columns - columns)
            if missing:
                raise RuntimeError(f"profiles table is missing required columns: {', '.join(missing)}")
            if "gg_elo" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN gg_elo REAL")
            if "gg_elo_updated_at" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN gg_elo_updated_at TEXT")
            conn.commit()
