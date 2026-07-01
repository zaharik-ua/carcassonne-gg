from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from .service import ProfileGgEloUpdateService
from .sqlite_repository import SqliteProfileGgEloRepository


class StubClient:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def fetch(self) -> dict:
        return self.payload


class ProfileGgEloUpdateServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE profiles (
                  id TEXT,
                  status TEXT,
                  deleted_at TEXT,
                  updated_by TEXT,
                  updated_at TEXT
                );
                CREATE TABLE audit_trail (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  entity_type TEXT
                );
                INSERT INTO profiles (id, status, deleted_at, updated_by, updated_at)
                VALUES
                  ('100', 'Active', NULL, 'admin-7', '2026-01-02 03:04:05'),
                  ('200', 'Active', NULL, 'admin-8', '2026-02-03 04:05:06'),
                  ('250', 'Removed', NULL, 'admin-10', '2026-02-04 05:06:07'),
                  ('300', 'Active', '2026-03-01 00:00:00', 'admin-9', '2026-03-01 00:00:00');
                """
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_updates_only_gg_elo_fields_without_audit(self) -> None:
        repository = SqliteProfileGgEloRepository(str(self.db_path))
        service = ProfileGgEloUpdateService(
            repository=repository,
            client=StubClient({
                "gg_profiles": [
                    {"id": "100", "gg_elo": "1829.95"},
                    {"profile_id": "200", "gg_elo": 1700},
                    {"id": "250", "gg_elo": "1900"},
                    {"id": "300", "gg_elo": "1600.25"},
                    {"id": "400", "gg_elo": "1500"},
                    {"id": "500", "gg_elo": None},
                ]
            }),
        )

        summary = service.run()

        self.assertEqual(summary["matched_profiles"], 3)
        self.assertEqual(summary["ranked_active_profiles"], 2)
        self.assertEqual(summary["updated_profiles"], 3)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT id, gg_elo, gg_elo_updated_at, gg_rating_position, updated_by, updated_at
                FROM profiles
                ORDER BY id
                """
            ).fetchall()
            audit_count = conn.execute("SELECT COUNT(*) FROM audit_trail").fetchone()[0]

        self.assertEqual(rows[0][1], 1829.95)
        self.assertIsNotNone(rows[0][2])
        self.assertEqual(rows[0][3], 1)
        self.assertEqual(rows[0][4:], ("admin-7", "2026-01-02 03:04:05"))
        self.assertEqual(rows[1][1], 1700.0)
        self.assertIsNotNone(rows[1][2])
        self.assertEqual(rows[1][3], 2)
        self.assertEqual(rows[1][4:], ("admin-8", "2026-02-03 04:05:06"))
        self.assertEqual(rows[2][1], 1900.0)
        self.assertIsNone(rows[2][3])
        self.assertIsNone(rows[3][1])
        self.assertIsNone(rows[3][2])
        self.assertEqual(audit_count, 0)


if __name__ == "__main__":
    unittest.main()
