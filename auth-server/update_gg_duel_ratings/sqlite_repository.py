from __future__ import annotations

import math
import sqlite3
from pathlib import Path

from .calculator import build_anchors, calculate_gg_rating_full, round_gg_rating


class SqliteGgDuelRatingsRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())
        self._ensure_schema()

    def recalculate(
        self,
        *,
        duel_ids: list[str] | None = None,
        status: str | None = None,
        source_type: str | None = None,
        tournament_id: str | None = None,
        missing_only: bool = False,
        dry_run: bool = False,
    ) -> dict:
        with self._connect() as conn:
            anchors = build_anchors(self._load_all_gg_elos(conn))
            rows = self._load_duels(
                conn,
                duel_ids=duel_ids,
                status=status,
                source_type=source_type,
                tournament_id=tournament_id,
                missing_only=missing_only,
            )
            results = []
            for row in rows:
                rating_full = calculate_gg_rating_full(row["elo1"], row["elo2"], anchors)
                results.append({
                    "duel_id": str(row["id"]),
                    "gg_rating_full": rating_full,
                    "gg_rating": round_gg_rating(rating_full),
                })

            if not dry_run:
                conn.execute("BEGIN IMMEDIATE TRANSACTION")
                try:
                    conn.executemany(
                        """
                        UPDATE duels
                        SET gg_rating_full = ?, gg_rating = ?
                        WHERE id = ? AND deleted_at IS NULL
                        """,
                        [
                            (item["gg_rating_full"], item["gg_rating"], item["duel_id"])
                            for item in results
                        ],
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

        return {
            "ok": True,
            "dry_run": bool(dry_run),
            "selected_duels": len(results),
            "calculated_duels": sum(item["gg_rating_full"] is not None for item in results),
            "duels_without_player_gg_elo": sum(item["gg_rating_full"] is None for item in results),
            "anchors": {"low": anchors.low, "high": anchors.high},
            "results": results,
        }

    @staticmethod
    def _load_all_gg_elos(conn: sqlite3.Connection) -> list[float]:
        rows = conn.execute(
            """
            SELECT gg_elo
            FROM profiles
            WHERE deleted_at IS NULL AND gg_elo IS NOT NULL
            """
        ).fetchall()
        ratings = []
        for row in rows:
            try:
                value = float(row["gg_elo"])
            except (TypeError, ValueError):
                continue
            if math.isfinite(value):
                ratings.append(value)
        return ratings

    @staticmethod
    def _load_duels(
        conn: sqlite3.Connection,
        *,
        duel_ids: list[str] | None,
        status: str | None,
        source_type: str | None,
        tournament_id: str | None,
        missing_only: bool,
    ) -> list[sqlite3.Row]:
        where_parts = ["d.deleted_at IS NULL"]
        params: list[object] = []
        normalized_ids = list(dict.fromkeys(
            str(duel_id).strip() for duel_id in (duel_ids or []) if str(duel_id).strip()
        ))
        if normalized_ids:
            where_parts.append(f"trim(COALESCE(d.id, '')) IN ({', '.join('?' for _ in normalized_ids)})")
            params.extend(normalized_ids)
        if status is not None:
            where_parts.append("lower(trim(COALESCE(d.status, ''))) = lower(trim(?))")
            params.append(status)
        if source_type is not None:
            where_parts.append("lower(trim(COALESCE(d.source_type, ''))) = lower(trim(?))")
            params.append(source_type)
        if tournament_id is not None:
            where_parts.append("trim(COALESCE(d.tournament_id, '')) = trim(?)")
            params.append(tournament_id)
        if missing_only:
            where_parts.append(
                "(d.gg_rating_full IS NULL OR trim(CAST(d.gg_rating_full AS TEXT)) = '')"
            )
        return conn.execute(
            f"""
            SELECT d.id, p1.gg_elo AS elo1, p2.gg_elo AS elo2
            FROM duels d
            LEFT JOIN profiles p1
              ON trim(COALESCE(p1.id, '')) = trim(COALESCE(d.player_1_id, ''))
             AND p1.deleted_at IS NULL
            LEFT JOIN profiles p2
              ON trim(COALESCE(p2.id, '')) = trim(COALESCE(d.player_2_id, ''))
             AND p2.deleted_at IS NULL
            WHERE {' AND '.join(where_parts)}
            ORDER BY d.id COLLATE NOCASE ASC
            """,
            params,
        ).fetchall()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            tables = {
                str(row["name"])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            missing_tables = sorted({"profiles", "duels"} - tables)
            if missing_tables:
                raise RuntimeError(f"SQLite DB is missing required tables: {', '.join(missing_tables)}")
            duel_columns = {
                str(row["name"]).strip()
                for row in conn.execute("PRAGMA table_info(duels)").fetchall()
            }
            if "gg_rating_full" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN gg_rating_full REAL")
            if "gg_rating" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN gg_rating INTEGER")
            conn.commit()
