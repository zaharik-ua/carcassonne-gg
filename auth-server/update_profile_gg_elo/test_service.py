from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from .service import ProfileGgEloUpdateService
from .sqlite_repository import SqliteProfileGgEloRepository


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
                  gg_base_elo REAL,
                  gg_elo REAL,
                  gg_elo_period_delta REAL,
                  gg_elo_updated_at TEXT,
                  gg_rating_position INTEGER
                );
                CREATE TABLE duels (
                  id TEXT,
                  time_utc TEXT,
                  player_1_id TEXT,
                  player_2_id TEXT,
                  dw1 INTEGER,
                  dw2 INTEGER,
                  duel_format TEXT,
                  ranking INTEGER,
                  deleted_at TEXT
                );
                CREATE TABLE system_settings (
                  setting_key TEXT PRIMARY KEY,
                  setting_value TEXT
                );

                INSERT INTO system_settings (setting_key, setting_value)
                VALUES
                  ('gg_rating_base_date', '2026-01-01'),
                  ('gg_rating_delta_start_date', '2026-02-01');

                INSERT INTO profiles (id, status, deleted_at, gg_base_elo)
                VALUES
                  ('100', 'Active', NULL, 1600),
                  ('200', 'Active', NULL, NULL),
                  ('300', 'Removed', NULL, 1400),
                  ('400', 'Active', '2026-01-05 00:00:00', 1800),
                  ('500', 'Active', NULL, NULL);

                INSERT INTO duels (id, time_utc, player_1_id, player_2_id, dw1, dw2, duel_format, ranking, deleted_at)
                VALUES
                  ('before-base', '2025-12-31T23:59:59Z', '100', '200', 1, 0, 'Bo1', 1, NULL),
                  ('before-delta', '2026-01-10T10:00:00Z', '100', '200', 1, 0, 'Bo1', 1, NULL),
                  ('after-delta', '2026-02-10T10:00:00Z', '200', '100', 1, 0, 'Bo1', 1, NULL),
                  ('ranking-zero', '2026-02-10T11:00:00Z', '100', '200', 1, 0, 'Bo1', 0, NULL),
                  ('unknown-player', '2026-02-11T10:00:00Z', '100', '999', 1, 0, 'Bo1', 1, NULL),
                  ('deleted-duel', '2026-02-12T10:00:00Z', '100', '200', 1, 0, 'Bo1', 1, '2026-02-12');
                """
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_recalculates_profile_gg_elo_from_local_duels(self) -> None:
        repository = SqliteProfileGgEloRepository(str(self.db_path))
        service = ProfileGgEloUpdateService(repository=repository)

        summary = service.run()

        self.assertEqual(summary["profiles"], 4)
        self.assertEqual(summary["selected_duels"], 3)
        self.assertEqual(summary["processed_duels"], 2)
        self.assertEqual(summary["period_duels"], 1)
        self.assertEqual(summary["skipped_duels_unknown_players"], 1)
        self.assertEqual(summary["base_elo_backfills"], 1)
        self.assertEqual(summary["updated_profiles"], 4)

        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT id, gg_base_elo, gg_elo, gg_elo_period_delta, gg_elo_updated_at, gg_rating_position
                FROM profiles
                ORDER BY id
                """
            ).fetchall()

        by_id = {row[0]: row for row in rows}
        self.assertEqual(by_id["100"][1], 1600)
        self.assertAlmostEqual(by_id["100"][2], 1590.08, places=2)
        self.assertAlmostEqual(by_id["100"][3], -21.44, places=2)
        self.assertIsNotNone(by_id["100"][4])
        self.assertEqual(by_id["100"][5], 1)

        self.assertEqual(by_id["200"][1], 1500)
        self.assertAlmostEqual(by_id["200"][2], 1509.92, places=2)
        self.assertAlmostEqual(by_id["200"][3], 21.44, places=2)
        self.assertIsNotNone(by_id["200"][4])
        self.assertEqual(by_id["200"][5], 2)

        self.assertEqual(by_id["300"][1], 1400)
        self.assertAlmostEqual(by_id["300"][2], 1400.0, places=2)
        self.assertAlmostEqual(by_id["300"][3], 0.0, places=2)
        self.assertIsNone(by_id["300"][5])

        self.assertEqual(by_id["400"][1], 1800)
        self.assertIsNone(by_id["400"][2])
        self.assertIsNone(by_id["400"][3])

        self.assertIsNone(by_id["500"][1])
        self.assertAlmostEqual(by_id["500"][2], 1500.0, places=2)
        self.assertAlmostEqual(by_id["500"][3], 0.0, places=2)
        self.assertEqual(by_id["500"][5], 3)

        with sqlite3.connect(self.db_path) as conn:
            duel_rows = conn.execute(
                """
                SELECT
                  id,
                  player1_elo_before,
                  player1_elo_after,
                  player2_elo_before,
                  player2_elo_after
                FROM duels
                ORDER BY id
                """
            ).fetchall()

        duels_by_id = {row[0]: row for row in duel_rows}
        self.assertAlmostEqual(duels_by_id["before-delta"][1], 1600.0, places=2)
        self.assertAlmostEqual(duels_by_id["before-delta"][2], 1611.52, places=2)
        self.assertAlmostEqual(duels_by_id["before-delta"][3], 1500.0, places=2)
        self.assertAlmostEqual(duels_by_id["before-delta"][4], 1488.48, places=2)

        self.assertAlmostEqual(duels_by_id["after-delta"][1], 1488.48, places=2)
        self.assertAlmostEqual(duels_by_id["after-delta"][2], 1509.92, places=2)
        self.assertAlmostEqual(duels_by_id["after-delta"][3], 1611.52, places=2)
        self.assertAlmostEqual(duels_by_id["after-delta"][4], 1590.08, places=2)

        self.assertIsNone(duels_by_id["ranking-zero"][1])
        self.assertIsNone(duels_by_id["unknown-player"][1])

    def test_dry_run_does_not_update_profiles(self) -> None:
        repository = SqliteProfileGgEloRepository(str(self.db_path))
        service = ProfileGgEloUpdateService(repository=repository)

        summary = service.run(dry_run=True)

        self.assertTrue(summary["dry_run"])
        self.assertEqual(summary["updated_profiles"], 0)
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute(
                """
                SELECT COUNT(*)
                FROM profiles
                WHERE gg_elo IS NOT NULL
                   OR gg_elo_period_delta IS NOT NULL
                   OR (id = '200' AND gg_base_elo IS NOT NULL)
                """
            ).fetchone()[0]
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
