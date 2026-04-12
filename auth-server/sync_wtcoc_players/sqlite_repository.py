from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AssociationMapping:
    id: str
    name: str


class SqliteWtcocPlayersRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())

    def load_summary(self) -> dict:
        with self._connect() as conn:
            tables = self._load_table_names(conn)
            summary = {
                "db_path": self.db_path,
                "tables": sorted(tables),
                "profiles_count": 0,
                "associations_count": 0,
            }
            if "profiles" in tables:
                row = conn.execute("SELECT COUNT(*) AS count FROM profiles").fetchone()
                summary["profiles_count"] = int(row["count"] or 0) if row else 0
            if "associations" in tables:
                row = conn.execute("SELECT COUNT(*) AS count FROM associations").fetchone()
                summary["associations_count"] = int(row["count"] or 0) if row else 0
            return summary

    def load_existing_profile_ids(self) -> set[str]:
        with self._connect() as conn:
            tables = self._load_table_names(conn)
            if "profiles" not in tables:
                raise RuntimeError(f"profiles table was not found in SQLite DB: {self.db_path}")
            rows = conn.execute(
                """
                SELECT trim(COALESCE(id, '')) AS id
                FROM profiles
                WHERE trim(COALESCE(id, '')) <> ''
                """
            ).fetchall()
        return {str(row["id"]).strip() for row in rows if str(row["id"] or "").strip()}

    def load_association_mappings(self) -> list[AssociationMapping]:
        with self._connect() as conn:
            tables = self._load_table_names(conn)
            if "associations" not in tables:
                raise RuntimeError(f"associations table was not found in SQLite DB: {self.db_path}")
            rows = conn.execute(
                """
                SELECT
                  trim(COALESCE(id, '')) AS id,
                  trim(COALESCE(name, '')) AS name
                FROM associations
                WHERE trim(COALESCE(name, '')) <> ''
                ORDER BY name COLLATE NOCASE ASC
                """
            ).fetchall()
        return [
            AssociationMapping(id=str(row["id"] or "").strip(), name=str(row["name"] or "").strip())
            for row in rows
        ]

    def insert_profiles(self, *, actor_id: str, profiles: list[dict]) -> dict:
        normalized_actor_id = str(actor_id or "").strip() or "1"
        with self._connect() as conn:
            tables = self._load_table_names(conn)
            missing_tables = [name for name in ("profiles", "associations") if name not in tables]
            if missing_tables:
                raise RuntimeError(f"SQLite DB is missing required tables: {', '.join(missing_tables)}")

            conn.execute("BEGIN IMMEDIATE TRANSACTION")
            try:
                inserted = 0
                skipped_existing = 0
                for item in profiles:
                    existing = conn.execute(
                        """
                        SELECT 1
                        FROM profiles
                        WHERE trim(COALESCE(id, '')) = trim(?)
                        LIMIT 1
                        """,
                        (item["id"],),
                    ).fetchone()
                    if existing is not None:
                        skipped_existing += 1
                        continue

                    conn.execute(
                        """
                        INSERT INTO profiles (
                          id,
                          bga_nickname,
                          association,
                          email,
                          status,
                          name,
                          master_title,
                          team_captain,
                          created_by,
                          updated_by,
                          deleted_by,
                          deleted_at,
                          created_at,
                          updated_at
                        )
                        VALUES (?, ?, ?, ?, 'Active', NULL, 0, 0, ?, ?, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                        (
                            item["id"],
                            item["bga_nickname"],
                            item["association"],
                            item["email"],
                            normalized_actor_id,
                            normalized_actor_id,
                        ),
                    )
                    inserted += 1

                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return {
            "actor_id": normalized_actor_id,
            "profiles_requested": len(profiles),
            "profiles_inserted": inserted,
            "profiles_skipped_existing": skipped_existing,
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
        return {str(row["name"]).strip() for row in rows if str(row["name"] or "").strip()}
