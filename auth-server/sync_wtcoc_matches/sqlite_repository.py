from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TeamMapping:
    code: str
    name: str


class SqliteWtcocRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())

    def load_db_summary(self, *, tournament_id: str) -> dict:
        with self._connect() as conn:
            tables = self._load_table_names(conn)
            summary = {
                "db_path": self.db_path,
                "tables": sorted(tables),
                "tournament_exists": False,
                "existing_matches_for_tournament": 0,
                "existing_duels_for_tournament": 0,
                "teams_count": 0,
                "profiles_count": 0,
            }
            if "tournaments" in tables:
                row = conn.execute(
                    """
                    SELECT 1
                    FROM tournaments
                    WHERE upper(trim(COALESCE(id, ''))) = upper(trim(?))
                    LIMIT 1
                    """,
                    (tournament_id,),
                ).fetchone()
                summary["tournament_exists"] = row is not None
            if "matches" in tables:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM matches
                    WHERE upper(trim(COALESCE(tournament_id, ''))) = upper(trim(?))
                      AND (deleted_at IS NULL OR trim(COALESCE(deleted_at, '')) = '')
                    """,
                    (tournament_id,),
                ).fetchone()
                summary["existing_matches_for_tournament"] = int(row["count"] or 0) if row else 0
            if "duels" in tables:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM duels
                    WHERE upper(trim(COALESCE(tournament_id, ''))) = upper(trim(?))
                      AND (deleted_at IS NULL OR trim(COALESCE(deleted_at, '')) = '')
                    """,
                    (tournament_id,),
                ).fetchone()
                summary["existing_duels_for_tournament"] = int(row["count"] or 0) if row else 0
            if "teams" in tables:
                row = conn.execute("SELECT COUNT(*) AS count FROM teams").fetchone()
                summary["teams_count"] = int(row["count"] or 0) if row else 0
            if "profiles" in tables:
                row = conn.execute("SELECT COUNT(*) AS count FROM profiles").fetchone()
                summary["profiles_count"] = int(row["count"] or 0) if row else 0
            return summary

    def load_team_mappings(self) -> list[TeamMapping]:
        with self._connect() as conn:
            if "teams" not in self._load_table_names(conn):
                return []
            rows = conn.execute(
                """
                SELECT
                  trim(COALESCE(id, '')) AS code,
                  trim(COALESCE(name, '')) AS name
                FROM teams
                WHERE trim(COALESCE(name, '')) <> ''
                ORDER BY name COLLATE NOCASE ASC
                """
            ).fetchall()
        return [
            TeamMapping(code=str(row["code"] or "").strip(), name=str(row["name"] or "").strip())
            for row in rows
        ]

    def upsert_matches_and_duels(
        self,
        *,
        tournament_id: str,
        actor_id: str,
        matches: list[dict],
        duels: list[dict],
    ) -> dict:
        normalized_actor_id = str(actor_id or "").strip() or "1"
        with self._connect() as conn:
            tables = self._load_table_names(conn)
            missing_tables = [name for name in ("matches", "duels", "teams") if name not in tables]
            if missing_tables:
                raise RuntimeError(f"SQLite DB is missing required tables: {', '.join(missing_tables)}")

            conn.execute("BEGIN IMMEDIATE TRANSACTION")
            try:
                inserted_matches = 0
                updated_matches = 0
                inserted_duels = 0
                updated_duels = 0

                for item in matches:
                    existing = conn.execute(
                        """
                        SELECT 1
                        FROM matches
                        WHERE trim(COALESCE(id, '')) = trim(?)
                        LIMIT 1
                        """,
                        (item["id"],),
                    ).fetchone()
                    if existing is None:
                        inserted_matches += 1
                    else:
                        updated_matches += 1
                    conn.execute(
                        """
                        INSERT INTO matches (
                          id,
                          tournament_id,
                          time_utc,
                          lineup_type,
                          lineup_deadline_h,
                          lineup_deadline_utc,
                          number_of_duels,
                          team_1,
                          team_2,
                          status,
                          dw1,
                          dw2,
                          gw1,
                          gw2,
                          created_by,
                          updated_by,
                          deleted_by,
                          deleted_at,
                          created_at,
                          updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'Planned', NULL, NULL, NULL, NULL, ?, ?, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT(id) DO UPDATE SET
                          tournament_id = excluded.tournament_id,
                          time_utc = excluded.time_utc,
                          lineup_type = excluded.lineup_type,
                          lineup_deadline_h = excluded.lineup_deadline_h,
                          lineup_deadline_utc = excluded.lineup_deadline_utc,
                          number_of_duels = excluded.number_of_duels,
                          team_1 = excluded.team_1,
                          team_2 = excluded.team_2,
                          updated_by = excluded.updated_by,
                          deleted_by = NULL,
                          deleted_at = NULL,
                          updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            item["id"],
                            tournament_id,
                            item["time_utc"],
                            item["lineup_type"],
                            item["lineup_deadline_h"],
                            item["lineup_deadline_utc"],
                            item["number_of_duels"],
                            item["team_1"],
                            item["team_2"],
                            normalized_actor_id,
                            normalized_actor_id,
                        ),
                    )

                for item in duels:
                    existing = conn.execute(
                        """
                        SELECT 1
                        FROM duels
                        WHERE trim(COALESCE(id, '')) = trim(?)
                        LIMIT 1
                        """,
                        (item["id"],),
                    ).fetchone()
                    if existing is None:
                        inserted_duels += 1
                    else:
                        updated_duels += 1
                    conn.execute(
                        """
                        INSERT INTO duels (
                          id,
                          tournament_id,
                          match_id,
                          duel_number,
                          duel_format,
                          time_utc,
                          custom_time,
                          player_1_id,
                          player_2_id,
                          dw1,
                          dw2,
                          rating_full,
                          rating,
                          status,
                          results_last_error,
                          results_checked_at,
                          created_by,
                          updated_by,
                          deleted_by,
                          deleted_at,
                          created_at,
                          updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, 'Planned', NULL, NULL, ?, ?, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT(id) DO UPDATE SET
                          tournament_id = excluded.tournament_id,
                          match_id = excluded.match_id,
                          duel_number = excluded.duel_number,
                          duel_format = excluded.duel_format,
                          time_utc = excluded.time_utc,
                          custom_time = excluded.custom_time,
                          player_1_id = excluded.player_1_id,
                          player_2_id = excluded.player_2_id,
                          updated_by = excluded.updated_by,
                          deleted_by = NULL,
                          deleted_at = NULL,
                          updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            item["id"],
                            tournament_id,
                            item["match_id"],
                            item["duel_number"],
                            item["duel_format"],
                            item["time_utc"],
                            item["custom_time"],
                            item["player_1_id"],
                            item["player_2_id"],
                            normalized_actor_id,
                            normalized_actor_id,
                        ),
                    )

                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return {
            "actor_id": normalized_actor_id,
            "tournament_id": tournament_id,
            "matches_processed": len(matches),
            "matches_inserted": inserted_matches,
            "matches_updated": updated_matches,
            "duels_processed": len(duels),
            "duels_inserted": inserted_duels,
            "duels_updated": updated_duels,
        }

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _load_table_names(conn: sqlite3.Connection) -> set[str]:
        rows = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            """
        ).fetchall()
        return {str(row["name"] or "").strip() for row in rows if row["name"] is not None}
