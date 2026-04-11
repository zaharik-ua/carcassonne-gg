from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AssociationMapping:
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
                "associations_count": 0,
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
            if "associations" in tables:
                row = conn.execute("SELECT COUNT(*) AS count FROM associations").fetchone()
                summary["associations_count"] = int(row["count"] or 0) if row else 0
            if "profiles" in tables:
                row = conn.execute("SELECT COUNT(*) AS count FROM profiles").fetchone()
                summary["profiles_count"] = int(row["count"] or 0) if row else 0
            return summary

    def load_association_mappings(self) -> list[AssociationMapping]:
        with self._connect() as conn:
            if "associations" not in self._load_table_names(conn):
                return []
            rows = conn.execute(
                """
                SELECT
                  trim(COALESCE(code, '')) AS code,
                  trim(COALESCE(name, '')) AS name
                FROM associations
                WHERE trim(COALESCE(name, '')) <> ''
                ORDER BY name COLLATE NOCASE ASC
                """
            ).fetchall()
        return [
            AssociationMapping(code=str(row["code"] or "").strip(), name=str(row["name"] or "").strip())
            for row in rows
        ]

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
