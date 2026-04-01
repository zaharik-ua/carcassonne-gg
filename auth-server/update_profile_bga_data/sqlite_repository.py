from __future__ import annotations

import sqlite3
from pathlib import Path

from .models import ProfileBgaDataUpdateRequest, ProfileBgaDataUpdateResult


class SqliteProfileBgaDataRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())
        self._ensure_schema()

    def fetch_player(self, player_id: str) -> ProfileBgaDataUpdateRequest | None:
        normalized_player_id = str(player_id or "").strip()
        if not normalized_player_id:
            return None

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                  trim(id) AS player_id,
                  CAST(trim(id) AS INTEGER) AS bga_player_id,
                  trim(COALESCE(bga_nickname, '')) AS bga_nickname,
                  NULLIF(trim(COALESCE(avatar, '')), '') AS avatar
                FROM profiles
                WHERE trim(COALESCE(id, '')) = trim(?)
                  AND deleted_at IS NULL
                LIMIT 1
                """,
                (normalized_player_id,),
            ).fetchone()

        if row is None:
            return None

        bga_nickname = str(row["bga_nickname"] or "").strip()
        if not bga_nickname:
            raise RuntimeError(f"Profile {normalized_player_id} has empty bga_nickname")

        return ProfileBgaDataUpdateRequest(
            player_id=str(row["player_id"]),
            bga_player_id=int(row["bga_player_id"]),
            bga_nickname=bga_nickname,
            avatar=row["avatar"],
        )

    def save_player_result(self, player: ProfileBgaDataUpdateRequest, result: ProfileBgaDataUpdateResult) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE profiles
                SET
                  bga_nickname = ?,
                  avatar = ?,
                  updated_at = CURRENT_TIMESTAMP
                WHERE trim(COALESCE(id, '')) = trim(?)
                  AND deleted_at IS NULL
                """,
                (result.bga_nickname, result.avatar, str(player.player_id)),
            )
            conn.commit()

    def load_profile_snapshot(self, player_id: str) -> dict | None:
        normalized_player_id = str(player_id or "").strip()
        if not normalized_player_id:
            return None

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                  trim(id) AS id,
                  NULLIF(trim(COALESCE(bga_nickname, '')), '') AS bga_nickname,
                  NULLIF(trim(COALESCE(avatar, '')), '') AS avatar
                FROM profiles
                WHERE trim(COALESCE(id, '')) = trim(?)
                  AND deleted_at IS NULL
                LIMIT 1
                """,
                (normalized_player_id,),
            ).fetchone()

        return dict(row) if row is not None else None

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            table_exists = conn.execute(
                """
                SELECT 1
                FROM sqlite_master
                WHERE type = 'table' AND name = 'profiles'
                LIMIT 1
                """
            ).fetchone()
            if table_exists is None:
                raise RuntimeError(f"profiles table was not found in SQLite DB: {self.db_path}")

            columns = {
                str(row["name"]).strip()
                for row in conn.execute("PRAGMA table_info(profiles)").fetchall()
            }
            if "avatar" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN avatar TEXT")
            conn.commit()
