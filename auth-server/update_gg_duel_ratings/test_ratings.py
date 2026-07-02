from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from .calculator import build_anchors, calculate_gg_rating_full, round_gg_rating
from .sqlite_repository import SqliteGgDuelRatingsRepository


class GgRatingCalculatorTest(unittest.TestCase):
    def test_formula_and_elite_override(self) -> None:
        anchors = build_anchors([1000, 1200, 1400, 1600, 1800])
        self.assertEqual(anchors.low, 1120)
        self.assertEqual(anchors.high, 1760)
        self.assertEqual(calculate_gg_rating_full(1800, 1800, anchors), 6.0)

        rating_full = calculate_gg_rating_full(1400, 1600, anchors)
        self.assertAlmostEqual(rating_full, 3.6505809577542965)
        self.assertEqual(round_gg_rating(rating_full), 4)


class GgDuelRatingsRepositoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE profiles (id TEXT, gg_elo REAL, deleted_at TEXT);
                INSERT INTO profiles VALUES
                  ('p1', 1000, NULL), ('p2', 1200, NULL), ('p3', 1400, NULL),
                  ('p4', 1600, NULL), ('p5', 1800, NULL);
                CREATE TABLE duels (
                  id TEXT,
                  tournament_id TEXT,
                  player_1_id TEXT,
                  player_2_id TEXT,
                  status TEXT,
                  source_type TEXT,
                  deleted_at TEXT,
                  updated_at TEXT,
                  updated_by TEXT
                );
                INSERT INTO duels VALUES
                  ('d1', 'T1', 'p3', 'p4', 'Planned', 'challenge', NULL, 'old-time', 'keeper'),
                  ('d2', 'T1', 'p4', 'p5', 'Done', 'import', NULL, 'old-time', 'keeper'),
                  ('d3', 'T2', 'p1', 'missing', 'Planned', 'challenge', NULL, 'old-time', 'keeper');
                CREATE TABLE audit_trail (id INTEGER PRIMARY KEY, event_type TEXT);
                """
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_combined_filters_update_only_gg_fields(self) -> None:
        repository = SqliteGgDuelRatingsRepository(str(self.db_path))
        summary = repository.recalculate(status="Planned", source_type="challenge")

        self.assertEqual(summary["selected_duels"], 2)
        self.assertEqual(summary["calculated_duels"], 1)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT id, gg_rating_full, gg_rating, updated_at, updated_by
                FROM duels ORDER BY id
                """
            ).fetchall()
            audit_count = conn.execute("SELECT COUNT(*) FROM audit_trail").fetchone()[0]
        self.assertAlmostEqual(rows[0][1], 3.6505809577542965)
        self.assertEqual(rows[0][2:], (4, "old-time", "keeper"))
        self.assertEqual(rows[1][1:], (None, None, "old-time", "keeper"))
        self.assertEqual(rows[2][1:], (None, None, "old-time", "keeper"))
        self.assertEqual(audit_count, 0)


if __name__ == "__main__":
    unittest.main()
