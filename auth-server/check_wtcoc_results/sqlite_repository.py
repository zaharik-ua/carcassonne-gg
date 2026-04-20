from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LocalDuelResult:
    id: str
    match_id: str
    duel_number: int | None
    status: str | None
    dw1: int | None
    dw2: int | None


@dataclass(frozen=True)
class LocalMatchResult:
    source: str
    external_match_id: str
    match_id: str
    status: str | None
    dw1: int | None
    dw2: int | None
    gw1: int | None
    gw2: int | None


class SqliteWtcocResultsRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())

    def load_linked_matches(self, tournament_id: str) -> dict[str, LocalMatchResult]:
        normalized_tournament_id = str(tournament_id or "").strip()
        if not normalized_tournament_id:
            return {}

        with self._connect() as conn:
            tables = self._load_table_names(conn)
            required_tables = {"wtcoc_match_links", "matches"}
            missing_tables = sorted(required_tables - tables)
            if missing_tables:
                raise RuntimeError(f"SQLite DB is missing required tables: {', '.join(missing_tables)}")

            match_columns = self._load_column_names(conn, "matches")
            where_parts = [
                "trim(COALESCE(l.tournament_id, '')) = trim(?)",
            ]
            if "deleted_at" in match_columns:
                where_parts.append("m.deleted_at IS NULL")

            rows = conn.execute(
                f"""
                SELECT
                  trim(COALESCE(l.source, '')) AS source,
                  trim(COALESCE(l.external_match_id, '')) AS external_match_id,
                  trim(COALESCE(m.id, '')) AS match_id,
                  trim(COALESCE(m.status, '')) AS status,
                  m.dw1 AS dw1,
                  m.dw2 AS dw2,
                  m.gw1 AS gw1,
                  m.gw2 AS gw2
                FROM wtcoc_match_links l
                JOIN matches m
                  ON trim(COALESCE(m.id, '')) = trim(COALESCE(l.match_id, ''))
                WHERE {" AND ".join(where_parts)}
                ORDER BY l.source COLLATE NOCASE ASC, l.external_match_id COLLATE NOCASE ASC
                """,
                (normalized_tournament_id,),
            ).fetchall()

        return {
            self._build_link_key(source=row["source"], external_match_id=row["external_match_id"]): LocalMatchResult(
                source=str(row["source"] or "").strip(),
                external_match_id=str(row["external_match_id"] or "").strip(),
                match_id=str(row["match_id"] or "").strip(),
                status=str(row["status"] or "").strip() or None,
                dw1=self._to_int_or_none(row["dw1"]),
                dw2=self._to_int_or_none(row["dw2"]),
                gw1=self._to_int_or_none(row["gw1"]),
                gw2=self._to_int_or_none(row["gw2"]),
            )
            for row in rows
        }

    def load_duels_by_match_ids(self, match_ids: list[str]) -> dict[str, list[LocalDuelResult]]:
        normalized_match_ids = [str(match_id or "").strip() for match_id in match_ids if str(match_id or "").strip()]
        if not normalized_match_ids:
            return {}

        with self._connect() as conn:
            tables = self._load_table_names(conn)
            if "duels" not in tables:
                raise RuntimeError("SQLite DB is missing required table: duels")

            duel_columns = self._load_column_names(conn, "duels")
            where_parts = [f"trim(COALESCE(d.match_id, '')) IN ({', '.join('?' for _ in normalized_match_ids)})"]
            if "deleted_at" in duel_columns:
                where_parts.append("d.deleted_at IS NULL")

            rows = conn.execute(
                f"""
                SELECT
                  trim(COALESCE(d.id, '')) AS id,
                  trim(COALESCE(d.match_id, '')) AS match_id,
                  d.duel_number AS duel_number,
                  trim(COALESCE(d.status, '')) AS status,
                  d.dw1 AS dw1,
                  d.dw2 AS dw2
                FROM duels d
                WHERE {" AND ".join(where_parts)}
                ORDER BY d.match_id COLLATE NOCASE ASC, d.duel_number ASC, d.id COLLATE NOCASE ASC
                """,
                normalized_match_ids,
            ).fetchall()

        duels_by_match_id: dict[str, list[LocalDuelResult]] = {}
        for row in rows:
            match_id = str(row["match_id"] or "").strip()
            duels_by_match_id.setdefault(match_id, []).append(
                LocalDuelResult(
                    id=str(row["id"] or "").strip(),
                    match_id=match_id,
                    duel_number=self._to_int_or_none(row["duel_number"]),
                    status=str(row["status"] or "").strip() or None,
                    dw1=self._to_int_or_none(row["dw1"]),
                    dw2=self._to_int_or_none(row["dw2"]),
                )
            )
        return duels_by_match_id

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _load_table_names(conn: sqlite3.Connection) -> set[str]:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        return {str(row["name"] or "").strip().lower() for row in rows}

    @staticmethod
    def _load_column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row["name"] or "").strip().lower() for row in rows}

    @staticmethod
    def _build_link_key(*, source: str, external_match_id: str) -> str:
        return f"{str(source or '').strip().lower()}:{str(external_match_id or '').strip()}"

    @staticmethod
    def _to_int_or_none(value) -> int | None:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        try:
            return int(float(raw))
        except ValueError:
            return None
