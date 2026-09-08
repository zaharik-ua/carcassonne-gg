from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from update_profile_bga_data_batch.service import ProfileBgaDataBatchService


class ProfileBgaDataBatchServiceTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "auth.sqlite")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE profiles (
                  id TEXT PRIMARY KEY,
                  bga_nickname TEXT,
                  avatar TEXT,
                  status TEXT NOT NULL DEFAULT 'Active',
                  deleted_at TEXT,
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  bga_data_updated_at TEXT
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO profiles (id, bga_nickname, bga_data_updated_at)
                VALUES (?, ?, ?)
                """,
                [
                    ("101", "Never refreshed", None),
                    ("102", "Older", "2026-09-08 09:59:59"),
                    ("103", "At cutoff", "2026-09-08 10:00:00"),
                    ("104", "Newer", "2026-09-08 10:00:01"),
                ],
            )
            conn.commit()

    def tearDown(self):
        self.temp_dir.cleanup()

    def create_selector(self, cutoff=None):
        service = ProfileBgaDataBatchService.__new__(ProfileBgaDataBatchService)
        service.db_path = self.db_path
        service.include_removed = False
        service.bga_data_updated_before = service._normalize_updated_before(cutoff)
        return service

    def test_fetches_only_never_refreshed_or_older_profiles(self):
        service = self.create_selector("2026-09-08T10:00:00.000Z")

        self.assertEqual(service._fetch_player_ids(limit=20), ["101", "102"])

    def test_empty_cutoff_keeps_all_eligible_profiles(self):
        service = self.create_selector()

        self.assertEqual(
            set(service._fetch_player_ids(limit=20)),
            {"101", "102", "103", "104"},
        )


if __name__ == "__main__":
    unittest.main()
