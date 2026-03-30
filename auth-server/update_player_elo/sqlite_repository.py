from __future__ import annotations

import sqlite3
from pathlib import Path

from .models import PlayerEloUpdateRequest, PlayerEloUpdateResult
from .repository import PlayerEloRepository


class SqlitePlayerEloRepository(PlayerEloRepository):
    def __init__(self, db_path: str, *, player_id: str | None = None) -> None:
        self.db_path = str(Path(db_path).resolve())
        self.player_id = str(player_id).strip() if player_id is not None else None
        self._profiles_columns: set[str] = set()
        self._ensure_schema()

    def fetch_players_to_update(
        self,
        *,
        limit: int,
        selection_mode: str = "stale_first",
        exclude_player_ids: set[str] | None = None,
    ) -> list[PlayerEloUpdateRequest]:
        if selection_mode not in {"stale_first", "only_null"}:
            raise ValueError(f"Unknown player Elo selection mode: {selection_mode}")

        exclude_player_ids = {str(player_id).strip() for player_id in (exclude_player_ids or set()) if str(player_id).strip()}

        params: list[object] = []
        where_parts = [
            "deleted_at IS NULL",
            "trim(COALESCE(id, '')) <> ''",
            "trim(COALESCE(id, '')) GLOB '[0-9]*'",
        ]

        if selection_mode == "only_null":
            where_parts.append("bga_elo IS NULL")

        if self.player_id is not None:
            where_parts.append("trim(COALESCE(id, '')) = trim(?)")
            params.append(self.player_id)

        if exclude_player_ids:
            placeholders = ",".join("?" for _ in exclude_player_ids)
            where_parts.append(f"trim(COALESCE(id, '')) NOT IN ({placeholders})")
            params.extend(sorted(exclude_player_ids))

        params.append(int(limit))
        stable_order_column = "rowid"
        order_by_sql = f"""
              CASE WHEN bga_elo_updated_at IS NULL OR trim(bga_elo_updated_at) = '' THEN 0 ELSE 1 END ASC,
              datetime(COALESCE(bga_elo_updated_at, '1970-01-01 00:00:00')) ASC,
              {stable_order_column} ASC
        """
        if selection_mode == "only_null":
            order_by_sql = f"""
              CASE WHEN bga_elo_updated_at IS NULL OR trim(bga_elo_updated_at) = '' THEN 0 ELSE 1 END ASC,
              {stable_order_column} ASC
            """

        sql = f"""
            SELECT
              trim(id) AS player_id,
              CAST(trim(id) AS INTEGER) AS bga_player_id
            FROM profiles
            WHERE {' AND '.join(where_parts)}
            ORDER BY
              {order_by_sql}
            LIMIT ?
        """

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        return [
            PlayerEloUpdateRequest(
                player_id=str(row["player_id"]),
                bga_player_id=int(row["bga_player_id"]),
            )
            for row in rows
        ]

    def save_player_result(self, player: PlayerEloUpdateRequest, result: PlayerEloUpdateResult) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE profiles
                SET
                  bga_elo = ?,
                  bga_elo_updated_at = CURRENT_TIMESTAMP,
                  updated_at = CURRENT_TIMESTAMP
                WHERE trim(COALESCE(id, '')) = trim(?)
                """,
                (result.elo, str(player.player_id)),
            )
            conn.commit()

    def save_player_error(self, player: PlayerEloUpdateRequest, message: str) -> None:
        print(f"⚠️ Player Elo update failed for profile {player.player_id}: {message}", flush=True)

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
            self._profiles_columns = columns
            if "bga_elo" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN bga_elo INTEGER")
            if "bga_elo_updated_at" not in columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN bga_elo_updated_at TEXT")
            duel_columns = {
                str(row["name"]).strip()
                for row in conn.execute("PRAGMA table_info(duels)").fetchall()
            }
            match_columns = {
                str(row["name"]).strip()
                for row in conn.execute("PRAGMA table_info(matches)").fetchall()
            }
            if "rating_full" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN rating_full REAL")
            if "rating" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN rating INTEGER")
            if "rating" not in match_columns:
                conn.execute("ALTER TABLE matches ADD COLUMN rating INTEGER")
            conn.commit()
