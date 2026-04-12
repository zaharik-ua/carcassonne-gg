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
            profile_columns = self._load_profile_columns(conn)
            required_columns = {"id", "bga_nickname", "association"}
            missing_profile_columns = sorted(required_columns - profile_columns)
            if missing_profile_columns:
                raise RuntimeError(f"profiles table is missing required columns: {', '.join(missing_profile_columns)}")

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
                    if not str(item.get("association") or "").strip():
                        raise RuntimeError(f"Profile {item['id']} has empty association and cannot be inserted")

                    insert_columns: list[str] = []
                    insert_values: list[str] = []
                    insert_params: list[str] = []

                    def add_value(column: str, value: object) -> None:
                        if column in profile_columns:
                            insert_columns.append(column)
                            insert_values.append("?")
                            insert_params.append(value)

                    def add_sql_value(column: str, sql_value: str) -> None:
                        if column in profile_columns:
                            insert_columns.append(column)
                            insert_values.append(sql_value)

                    add_value("id", item["id"])
                    add_value("bga_nickname", item["bga_nickname"])
                    add_value("association", item["association"])
                    add_value("status", "Active")
                    add_sql_value("created_at", "CURRENT_TIMESTAMP")
                    add_sql_value("updated_at", "CURRENT_TIMESTAMP")
                    add_value("created_by", normalized_actor_id)
                    add_value("updated_by", normalized_actor_id)

                    conn.execute(
                        f"""
                        INSERT INTO profiles (
                          {", ".join(insert_columns)}
                        )
                        VALUES ({", ".join(insert_values)})
                        """,
                        insert_params,
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
    def _load_profile_columns(conn: sqlite3.Connection) -> set[str]:
        rows = conn.execute("PRAGMA table_info(profiles)").fetchall()
        return {str(row["name"]).strip() for row in rows if str(row["name"] or "").strip()}

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
