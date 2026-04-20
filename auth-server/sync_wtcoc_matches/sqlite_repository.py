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


@dataclass(frozen=True)
class WtcocMatchLink:
    tournament_id: str
    source: str
    external_match_id: str
    match_id: str
    fallback_date_iso: str | None


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

    def load_wtcoc_match_links(self, tournament_id: str) -> list[WtcocMatchLink]:
        normalized_tournament_id = str(tournament_id or "").strip()
        if not normalized_tournament_id:
            return []
        with self._connect() as conn:
            tables = self._load_table_names(conn)
            if "wtcoc_match_links" not in tables:
                return []
            rows = conn.execute(
                """
                SELECT
                  trim(COALESCE(tournament_id, '')) AS tournament_id,
                  trim(COALESCE(source, '')) AS source,
                  trim(COALESCE(external_match_id, '')) AS external_match_id,
                  trim(COALESCE(match_id, '')) AS match_id,
                  trim(COALESCE(fallback_date_iso, '')) AS fallback_date_iso
                FROM wtcoc_match_links
                WHERE trim(COALESCE(tournament_id, '')) = trim(?)
                ORDER BY source COLLATE NOCASE ASC, external_match_id COLLATE NOCASE ASC
                """,
                (normalized_tournament_id,),
            ).fetchall()
        return [
            WtcocMatchLink(
                tournament_id=str(row["tournament_id"] or "").strip(),
                source=str(row["source"] or "").strip(),
                external_match_id=str(row["external_match_id"] or "").strip(),
                match_id=str(row["match_id"] or "").strip(),
                fallback_date_iso=str(row["fallback_date_iso"] or "").strip() or None,
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
            self._ensure_wtcoc_match_links_table(conn)
            link_map = self._load_wtcoc_match_link_map(conn, tournament_id)

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
                    source = str(item.get("source") or "").strip()
                    external_match_id = str(item.get("external_match_id") or "").strip()
                    fallback_date_iso = self._normalize_db_value(item.get("fallback_date_iso"))
                    link_key = self._build_link_key(source=source, external_match_id=external_match_id)
                    linked_match_id = link_map.get(link_key).match_id if link_key and link_key in link_map else None
                    candidate_existing_ids = [
                        linked_match_id,
                        self._normalize_db_value(item.get("id")),
                        self._build_legacy_wtcoc_match_id(
                            tournament_id=tournament_id,
                            source=source,
                            external_match_id=external_match_id,
                        ),
                    ]
                    existing_match_id = next(
                        (
                            candidate
                            for candidate in candidate_existing_ids
                            if candidate and self._match_exists(conn, candidate)
                        ),
                        None,
                    )
                    if existing_match_id and existing_match_id != item["id"]:
                        renamed_duel_ids = self._rename_match_and_duels(
                            conn,
                            old_match_id=existing_match_id,
                            new_match_id=str(item["id"]),
                            actor_id=normalized_actor_id,
                        )
                        changed_match_ids.append(str(item["id"]))
                        changed_duel_ids.extend(renamed_duel_ids)
                    self._upsert_wtcoc_match_link(
                        conn,
                        tournament_id=tournament_id,
                        source=source,
                        external_match_id=external_match_id,
                        match_id=str(item["id"]),
                        fallback_date_iso=fallback_date_iso,
                    )
                    if link_key:
                        link_map[link_key] = WtcocMatchLink(
                            tournament_id=tournament_id,
                            source=source,
                            external_match_id=external_match_id,
                            match_id=str(item["id"]),
                            fallback_date_iso=fallback_date_iso,
                        )
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
            "changed_match_ids": self._dedupe_preserving_order(changed_match_ids),
            "changed_duel_ids": self._dedupe_preserving_order(changed_duel_ids),
        }

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _build_link_key(*, source: str, external_match_id: str) -> str | None:
        normalized_source = str(source or "").strip().lower()
        normalized_external_match_id = str(external_match_id or "").strip()
        if not normalized_source or not normalized_external_match_id:
            return None
        return f"{normalized_source}:{normalized_external_match_id}"

    def _load_wtcoc_match_link_map(self, conn: sqlite3.Connection, tournament_id: str) -> dict[str, WtcocMatchLink]:
        rows = conn.execute(
            """
            SELECT
              trim(COALESCE(tournament_id, '')) AS tournament_id,
              trim(COALESCE(source, '')) AS source,
              trim(COALESCE(external_match_id, '')) AS external_match_id,
              trim(COALESCE(match_id, '')) AS match_id,
              trim(COALESCE(fallback_date_iso, '')) AS fallback_date_iso
            FROM wtcoc_match_links
            WHERE trim(COALESCE(tournament_id, '')) = trim(?)
            """,
            (tournament_id,),
        ).fetchall()
        result: dict[str, WtcocMatchLink] = {}
        for row in rows:
            link = WtcocMatchLink(
                tournament_id=str(row["tournament_id"] or "").strip(),
                source=str(row["source"] or "").strip(),
                external_match_id=str(row["external_match_id"] or "").strip(),
                match_id=str(row["match_id"] or "").strip(),
                fallback_date_iso=str(row["fallback_date_iso"] or "").strip() or None,
            )
            key = self._build_link_key(source=link.source, external_match_id=link.external_match_id)
            if key:
                result[key] = link
        return result

    @staticmethod
    def _ensure_wtcoc_match_links_table(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wtcoc_match_links (
              tournament_id TEXT NOT NULL,
              source TEXT NOT NULL,
              external_match_id TEXT NOT NULL,
              match_id TEXT NOT NULL,
              fallback_date_iso TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (tournament_id, source, external_match_id)
            )
            """
        )

    def _upsert_wtcoc_match_link(
        self,
        conn: sqlite3.Connection,
        *,
        tournament_id: str,
        source: str,
        external_match_id: str,
        match_id: str,
        fallback_date_iso: str | None,
    ) -> None:
        normalized_tournament_id = str(tournament_id or "").strip()
        normalized_source = str(source or "").strip()
        normalized_external_match_id = str(external_match_id or "").strip()
        normalized_match_id = str(match_id or "").strip()
        if not normalized_tournament_id or not normalized_source or not normalized_external_match_id or not normalized_match_id:
            return
        conn.execute(
            """
            INSERT INTO wtcoc_match_links (
              tournament_id,
              source,
              external_match_id,
              match_id,
              fallback_date_iso
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(tournament_id, source, external_match_id)
            DO UPDATE SET
              match_id = excluded.match_id,
              fallback_date_iso = COALESCE(excluded.fallback_date_iso, wtcoc_match_links.fallback_date_iso),
              updated_at = CURRENT_TIMESTAMP
            """,
            (
                normalized_tournament_id,
                normalized_source,
                normalized_external_match_id,
                normalized_match_id,
                fallback_date_iso,
            ),
        )

    @staticmethod
    def _build_legacy_wtcoc_match_id(*, tournament_id: str, source: str, external_match_id: str) -> str | None:
        normalized_tournament_id = str(tournament_id or "").strip()
        normalized_source = str(source or "").strip()
        normalized_external_match_id = str(external_match_id or "").strip()
        if not normalized_tournament_id or not normalized_source or not normalized_external_match_id:
            return None
        suffix = "PO" if normalized_source == "playoff" else "M"
        return f"{normalized_tournament_id}-{suffix}{normalized_external_match_id}"

    @staticmethod
    def _match_exists(conn: sqlite3.Connection, match_id: str) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM matches
            WHERE trim(COALESCE(id, '')) = trim(?)
            LIMIT 1
            """,
            (match_id,),
        ).fetchone()
        return row is not None

    def _rename_match_and_duels(
        self,
        conn: sqlite3.Connection,
        *,
        old_match_id: str,
        new_match_id: str,
        actor_id: str,
    ) -> list[str]:
        normalized_old_match_id = str(old_match_id or "").strip()
        normalized_new_match_id = str(new_match_id or "").strip()
        normalized_actor_id = str(actor_id or "").strip() or "1"
        if not normalized_old_match_id or not normalized_new_match_id or normalized_old_match_id == normalized_new_match_id:
            return []
        if self._match_exists(conn, normalized_new_match_id):
            raise RuntimeError(f"Cannot rename WTCOC match {normalized_old_match_id} to {normalized_new_match_id}: target id already exists")

        duel_rows = conn.execute(
            """
            SELECT
              trim(COALESCE(id, '')) AS id,
              duel_number
            FROM duels
            WHERE trim(COALESCE(match_id, '')) = trim(?)
            ORDER BY duel_number ASC, id ASC
            """,
            (normalized_old_match_id,),
        ).fetchall()
        changed_duel_ids: list[str] = []
        for row in duel_rows:
            current_duel_id = str(row["id"] or "").strip()
            duel_number = self._normalize_nullable_number(row["duel_number"])
            next_duel_id = self._build_generated_duel_id(normalized_new_match_id, duel_number)
            if current_duel_id and next_duel_id and current_duel_id != next_duel_id:
                conn.execute(
                    """
                    UPDATE duels
                    SET
                      id = ?,
                      match_id = ?,
                      updated_by = ?,
                      updated_at = CURRENT_TIMESTAMP
                    WHERE trim(COALESCE(id, '')) = trim(?)
                    """,
                    (next_duel_id, normalized_new_match_id, normalized_actor_id, current_duel_id),
                )
                changed_duel_ids.append(next_duel_id)
                continue
            conn.execute(
                """
                UPDATE duels
                SET
                  match_id = ?,
                  updated_by = ?,
                  updated_at = CURRENT_TIMESTAMP
                WHERE trim(COALESCE(id, '')) = trim(?)
                """,
                (normalized_new_match_id, normalized_actor_id, current_duel_id),
            )
            if current_duel_id:
                changed_duel_ids.append(current_duel_id)

        conn.execute(
            """
            UPDATE matches
            SET
              id = ?,
              updated_by = ?,
              updated_at = CURRENT_TIMESTAMP
            WHERE trim(COALESCE(id, '')) = trim(?)
            """,
            (normalized_new_match_id, normalized_actor_id, normalized_old_match_id),
        )
        conn.execute(
            """
            UPDATE wtcoc_match_links
            SET
              match_id = ?,
              updated_at = CURRENT_TIMESTAMP
            WHERE trim(COALESCE(match_id, '')) = trim(?)
            """,
            (normalized_new_match_id, normalized_old_match_id),
        )
        return changed_duel_ids

    @staticmethod
    def _build_generated_duel_id(match_id: str, duel_number: int | float | None) -> str | None:
        normalized_match_id = str(match_id or "").strip()
        normalized_duel_number = SqliteWtcocRepository._normalize_nullable_number(duel_number)
        if not normalized_match_id or normalized_duel_number is None:
            return None
        return f"{normalized_match_id}-D{int(normalized_duel_number)}"

    @staticmethod
    def _dedupe_preserving_order(values: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            normalized = str(value or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
        return result

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
