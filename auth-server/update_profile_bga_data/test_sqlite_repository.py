from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from update_profile_bga_data.models import ProfileBgaDataUpdateResult
from update_profile_bga_data.sqlite_repository import SqliteProfileBgaDataRepository


class ProfileBgaDataRepositoryTest(unittest.TestCase):
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
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                "INSERT INTO profiles (id, bga_nickname, avatar) VALUES (?, ?, ?)",
                ("123", "Player", "avatar-token"),
            )
            conn.commit()

    def tearDown(self):
        self.temp_dir.cleanup()

    def load_refresh_timestamp(self):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT bga_data_updated_at FROM profiles WHERE id = '123'"
            ).fetchone()
        return row[0]

    def test_successful_refresh_sets_bga_data_updated_at_even_when_data_is_unchanged(self):
        repository = SqliteProfileBgaDataRepository(self.db_path)
        player = repository.fetch_player("123")
        self.assertIsNotNone(player)

        repository.save_player_result(
            player,
            ProfileBgaDataUpdateResult(
                status="success",
                bga_nickname=player.bga_nickname,
                avatar=player.avatar,
                matched_player_id=player.bga_player_id,
            ),
        )
        snapshot = repository.load_profile_snapshot("123")

        self.assertIsNotNone(self.load_refresh_timestamp())
        self.assertIsNotNone(snapshot["bga_data_updated_at"])

    def test_schema_migration_does_not_backfill_refresh_timestamp(self):
        SqliteProfileBgaDataRepository(self.db_path)

        self.assertIsNone(self.load_refresh_timestamp())


if __name__ == "__main__":
    unittest.main()
