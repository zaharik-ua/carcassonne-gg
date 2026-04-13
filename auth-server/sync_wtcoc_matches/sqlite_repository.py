from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TeamMapping:
    code: str
    name: str


@dataclass(frozen=True)
class ProfileMapping:
    id: str
    bga_nickname: str | None
    name: str | None


class SqliteWtcocRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())

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

    def load_profile_mappings(self) -> list[ProfileMapping]:
        with self._connect() as conn:
            if "profiles" not in self._load_table_names(conn):
                return []
            rows = conn.execute(
                """
                SELECT
                  trim(COALESCE(id, '')) AS id,
                  trim(COALESCE(bga_nickname, '')) AS bga_nickname,
                  trim(COALESCE(name, '')) AS name
                FROM profiles
                WHERE trim(COALESCE(id, '')) <> ''
                  AND trim(COALESCE(deleted_at, '')) = ''
                ORDER BY id COLLATE NOCASE ASC
                """
            ).fetchall()
        return [
            ProfileMapping(
                id=str(row["id"] or "").strip(),
                bga_nickname=str(row["bga_nickname"] or "").strip() or None,
                name=str(row["name"] or "").strip() or None,
            )
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
                unchanged_matches = 0
                inserted_duels = 0
                updated_duels = 0
                unchanged_duels = 0
                changed_match_ids: list[str] = []
                changed_duel_ids: list[str] = []

                for item in matches:
                    existing = conn.execute(
                        """
                        SELECT
                          trim(COALESCE(tournament_id, '')) AS tournament_id,
                          trim(COALESCE(time_utc, '')) AS time_utc,
                          trim(COALESCE(lineup_type, '')) AS lineup_type,
                          lineup_deadline_h,
                          trim(COALESCE(lineup_deadline_utc, '')) AS lineup_deadline_utc,
                          number_of_duels,
                          trim(COALESCE(team_1, '')) AS team_1,
                          trim(COALESCE(team_2, '')) AS team_2,
                          trim(COALESCE(deleted_at, '')) AS deleted_at
                        FROM matches
                        WHERE trim(COALESCE(id, '')) = trim(?)
                        LIMIT 1
                        """,
                        (item["id"],),
                    ).fetchone()
                    if existing is None:
                        inserted_matches += 1
                        changed_match_ids.append(str(item["id"]))
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
                        continue

                    normalized_existing_match = {
                        "tournament_id": self._normalize_db_value(existing["tournament_id"]),
                        "time_utc": self._normalize_db_value(existing["time_utc"]),
                        "lineup_type": self._normalize_db_value(existing["lineup_type"]),
                        "lineup_deadline_h": self._normalize_nullable_number(existing["lineup_deadline_h"]),
                        "lineup_deadline_utc": self._normalize_db_value(existing["lineup_deadline_utc"]),
                        "number_of_duels": self._normalize_nullable_number(existing["number_of_duels"]),
                        "team_1": self._normalize_db_value(existing["team_1"]),
                        "team_2": self._normalize_db_value(existing["team_2"]),
                        "deleted_at": self._normalize_db_value(existing["deleted_at"]),
                    }
                    normalized_incoming_match = {
                        "tournament_id": self._normalize_db_value(tournament_id),
                        "time_utc": self._normalize_db_value(item["time_utc"]),
                        "lineup_type": self._normalize_db_value(item["lineup_type"]),
                        "lineup_deadline_h": self._normalize_nullable_number(item["lineup_deadline_h"]),
                        "lineup_deadline_utc": self._normalize_db_value(item["lineup_deadline_utc"]),
                        "number_of_duels": self._normalize_nullable_number(item["number_of_duels"]),
                        "team_1": self._normalize_db_value(item["team_1"]),
                        "team_2": self._normalize_db_value(item["team_2"]),
                        "deleted_at": None,
                    }
                    if normalized_existing_match == normalized_incoming_match:
                        unchanged_matches += 1
                        continue

                    updated_matches += 1
                    changed_match_ids.append(str(item["id"]))
                    conn.execute(
                        """
                        UPDATE matches
                        SET
                          tournament_id = ?,
                          time_utc = ?,
                          lineup_type = ?,
                          lineup_deadline_h = ?,
                          lineup_deadline_utc = ?,
                          number_of_duels = ?,
                          team_1 = ?,
                          team_2 = ?,
                          updated_by = ?,
                          deleted_by = NULL,
                          deleted_at = NULL,
                          updated_at = CURRENT_TIMESTAMP
                        WHERE trim(COALESCE(id, '')) = trim(?)
                        """,
                        (
                            tournament_id,
                            item["time_utc"],
                            item["lineup_type"],
                            item["lineup_deadline_h"],
                            item["lineup_deadline_utc"],
                            item["number_of_duels"],
                            item["team_1"],
                            item["team_2"],
                            normalized_actor_id,
                            item["id"],
                        ),
                    )

                for item in duels:
                    existing = conn.execute(
                        """
                        SELECT
                          trim(COALESCE(tournament_id, '')) AS tournament_id,
                          trim(COALESCE(match_id, '')) AS match_id,
                          duel_number,
                          trim(COALESCE(duel_format, '')) AS duel_format,
                          trim(COALESCE(time_utc, '')) AS time_utc,
                          trim(COALESCE(custom_time, '')) AS custom_time,
                          trim(COALESCE(player_1_id, '')) AS player_1_id,
                          trim(COALESCE(player_2_id, '')) AS player_2_id,
                          trim(COALESCE(deleted_at, '')) AS deleted_at
                        FROM duels
                        WHERE trim(COALESCE(id, '')) = trim(?)
                        LIMIT 1
                        """,
                        (item["id"],),
                    ).fetchone()
                    if existing is None:
                        inserted_duels += 1
                        changed_duel_ids.append(str(item["id"]))
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
                        continue

                    normalized_existing_duel = {
                        "tournament_id": self._normalize_db_value(existing["tournament_id"]),
                        "match_id": self._normalize_db_value(existing["match_id"]),
                        "duel_number": self._normalize_nullable_number(existing["duel_number"]),
                        "duel_format": self._normalize_db_value(existing["duel_format"]),
                        "time_utc": self._normalize_db_value(existing["time_utc"]),
                        "custom_time": self._normalize_db_value(existing["custom_time"]),
                        "player_1_id": self._normalize_db_value(existing["player_1_id"]),
                        "player_2_id": self._normalize_db_value(existing["player_2_id"]),
                        "deleted_at": self._normalize_db_value(existing["deleted_at"]),
                    }
                    normalized_incoming_duel = {
                        "tournament_id": self._normalize_db_value(tournament_id),
                        "match_id": self._normalize_db_value(item["match_id"]),
                        "duel_number": self._normalize_nullable_number(item["duel_number"]),
                        "duel_format": self._normalize_db_value(item["duel_format"]),
                        "time_utc": self._normalize_db_value(item["time_utc"]),
                        "custom_time": self._normalize_db_value(item["custom_time"]),
                        "player_1_id": self._normalize_db_value(item["player_1_id"]),
                        "player_2_id": self._normalize_db_value(item["player_2_id"]),
                        "deleted_at": None,
                    }
                    if normalized_existing_duel == normalized_incoming_duel:
                        unchanged_duels += 1
                        continue

                    updated_duels += 1
                    changed_duel_ids.append(str(item["id"]))
                    conn.execute(
                        """
                        UPDATE duels
                        SET
                          tournament_id = ?,
                          match_id = ?,
                          duel_number = ?,
                          duel_format = ?,
                          time_utc = ?,
                          custom_time = ?,
                          player_1_id = ?,
                          player_2_id = ?,
                          updated_by = ?,
                          deleted_by = NULL,
                          deleted_at = NULL,
                          updated_at = CURRENT_TIMESTAMP
                        WHERE trim(COALESCE(id, '')) = trim(?)
                        """,
                        (
                            tournament_id,
                            item["match_id"],
                            item["duel_number"],
                            item["duel_format"],
                            item["time_utc"],
                            item["custom_time"],
                            item["player_1_id"],
                            item["player_2_id"],
                            normalized_actor_id,
                            item["id"],
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
            "matches_unchanged": unchanged_matches,
            "duels_processed": len(duels),
            "duels_inserted": inserted_duels,
            "duels_updated": updated_duels,
            "duels_unchanged": unchanged_duels,
            "changed_match_ids": changed_match_ids,
            "changed_duel_ids": changed_duel_ids,
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

    @staticmethod
    def _normalize_db_value(value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _normalize_nullable_number(value: object) -> int | float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return value
        text = str(value).strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            try:
                return float(text)
            except ValueError:
                return None
