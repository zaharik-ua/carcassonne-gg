from __future__ import annotations

import sqlite3
from pathlib import Path


class SqliteJobRunsRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())
        self._ensure_schema()

    def mark_success(self, job_name: str) -> dict[str, object]:
        normalized_job_name = str(job_name).strip()
        if not normalized_job_name:
            raise ValueError("job_name must not be empty")

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO job_runs (
                  job_name,
                  last_success_at,
                  last_started_at,
                  last_finished_at,
                  last_status,
                  last_error,
                  updated_at
                )
                VALUES (
                  ?,
                  CURRENT_TIMESTAMP,
                  NULL,
                  CURRENT_TIMESTAMP,
                  'success',
                  NULL,
                  CURRENT_TIMESTAMP
                )
                ON CONFLICT(job_name) DO UPDATE SET
                  last_success_at = CURRENT_TIMESTAMP,
                  last_finished_at = CURRENT_TIMESTAMP,
                  last_status = 'success',
                  last_error = NULL,
                  updated_at = CURRENT_TIMESTAMP
                """,
                (normalized_job_name,),
            )
            row = conn.execute(
                """
                SELECT
                  job_name,
                  last_success_at,
                  last_started_at,
                  last_finished_at,
                  last_status,
                  last_error,
                  updated_at
                FROM job_runs
                WHERE job_name = ?
                LIMIT 1
                """,
                (normalized_job_name,),
            ).fetchone()
            conn.commit()

        return {
            "job_name": str(row["job_name"]),
            "last_success_at": row["last_success_at"],
            "last_started_at": row["last_started_at"],
            "last_finished_at": row["last_finished_at"],
            "last_status": row["last_status"],
            "last_error": row["last_error"],
            "updated_at": row["updated_at"],
        }

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_runs (
                  job_name TEXT PRIMARY KEY,
                  last_success_at TEXT,
                  last_started_at TEXT,
                  last_finished_at TEXT,
                  last_status TEXT,
                  last_error TEXT,
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()
