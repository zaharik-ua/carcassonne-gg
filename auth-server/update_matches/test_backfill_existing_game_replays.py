from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from .backfill_existing_game_replays import backfill_existing_game_replays
from .game_replay import ensure_game_replays_schema


class BackfillExistingGameReplaysTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        self.now = datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE games (
                  id TEXT PRIMARY KEY,
                  bga_table_id TEXT,
                  deleted_at TEXT
                )
                """
            )
            conn.executemany(
                "INSERT INTO games (id, bga_table_id) VALUES (?, ?)",
                [
                    ("error-game", "100"),
                    ("bga-game", "101"),
                    ("fallback-game", "102"),
                ],
            )
            ensure_game_replays_schema(conn)
            conn.execute(
                """
                INSERT INTO game_replays (
                  game_id, bga_table_id, status, players_json, events_json,
                  scoring_json, player_time_json, color_source
                ) VALUES ('error-game', '100', 'error', '[]', '[]', '{}', '{}', NULL)
                """
            )
            conn.execute(
                """
                INSERT INTO game_replays (
                  game_id, bga_table_id, status, players_json, events_json,
                  scoring_json, player_time_json, color_source
                ) VALUES (?, '101', 'ready', ?, ?, '{}', '{}', 'bga')
                """,
                (
                    "bga-game",
                    json.dumps([
                        {"player_id": "1", "player_name": "Alpha", "color_hex": "ff0000"},
                        {"player_id": "2", "player_name": "Beta", "color_hex": "0000ff"},
                    ]),
                    json.dumps([
                        {
                            "type": "playTile", "player_id": "1", "player_name": "Alpha",
                            "tile_type": 2, "x": 1, "y": 0, "rotation": 0,
                        },
                        {
                            "type": "playTile", "player_id": "2", "player_name": "Beta",
                            "tile_type": 3, "x": 1, "y": 1, "rotation": 1,
                        },
                    ]),
                ),
            )
            conn.execute(
                """
                INSERT INTO game_replays (
                  game_id, bga_table_id, status, players_json, events_json,
                  scoring_json, player_time_json, color_source
                ) VALUES (?, '102', 'ready', ?, ?, '{}', '{}', 'fallback')
                """,
                (
                    "fallback-game",
                    json.dumps([
                        {"player_id": "1", "player_name": "Alpha", "color_hex": None},
                        {"player_id": "2", "player_name": "Beta", "color_hex": None},
                    ]),
                    json.dumps([
                        {
                            "type": "playTile", "player_id": "1", "player_name": "Alpha",
                            "tile_type": 2, "x": 1, "y": 0, "rotation": 0,
                        },
                        {
                            "type": "playTile", "player_id": "2", "player_name": "Beta",
                            "tile_type": 3, "x": 1, "y": 1, "rotation": 1,
                        },
                    ]),
                ),
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_dry_run_reports_without_changing_rows(self) -> None:
        summary = backfill_existing_game_replays(
            self.db_path,
            now=self.now,
            batch_id="test-batch",
        )

        self.assertEqual(summary["mode"], "dry-run")
        self.assertEqual(summary["error_rows_deleted"], 1)
        self.assertEqual(summary["ready_rows_processed"], 2)
        self.assertEqual(summary["color_refresh_scheduled"], 1)
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM game_replays").fetchone()[0],
                3,
            )
            self.assertIsNone(conn.execute(
                "SELECT retry_reason FROM game_replays WHERE game_id = 'fallback-game'"
            ).fetchone()[0])

    def test_apply_deletes_errors_and_backfills_ready_rows(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_backfill_color_source_update
                BEFORE UPDATE OF color_source ON game_replays
                BEGIN
                  SELECT RAISE(ABORT, 'color_source must remain unchanged');
                END
                """
            )

        summary = backfill_existing_game_replays(
            self.db_path,
            apply=True,
            now=self.now,
            batch_id="test-batch",
        )

        self.assertEqual(summary["mode"], "apply")
        self.assertEqual(summary["error_rows_deleted"], 1)
        self.assertEqual(summary["ready_bga_colors"], 1)
        self.assertEqual(summary["ready_fallback_colors"], 1)
        self.assertEqual(summary["scheduled_game_ids"], ["fallback-game"])

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            self.assertIsNone(conn.execute(
                "SELECT game_id FROM game_replays WHERE game_id = 'error-game'"
            ).fetchone())
            bga = conn.execute(
                "SELECT * FROM game_replays WHERE game_id = 'bga-game'"
            ).fetchone()
            fallback = conn.execute(
                "SELECT * FROM game_replays WHERE game_id = 'fallback-game'"
            ).fetchone()

        self.assertEqual(bga["status"], "ready")
        self.assertEqual(bga["color_source"], "bga")
        self.assertEqual(bga["queue_class"], "historical")
        self.assertIsNone(bga["retry_reason"])
        self.assertIsNone(bga["next_attempt_at"])
        self.assertEqual(bga["history_request_count"], 1)
        self.assertEqual(json.loads(bga["board_stats_json"]), {"width": 2, "height": 2})
        self.assertIn("colors=red,blue", bga["carcassonne_lab_url"])

        self.assertEqual(fallback["status"], "ready")
        self.assertEqual(fallback["color_source"], "fallback")
        self.assertEqual(fallback["queue_class"], "historical")
        self.assertEqual(fallback["retry_reason"], "colors")
        self.assertEqual(fallback["next_attempt_at"], "2026-09-24 12:00:00.000000")
        self.assertEqual(fallback["historical_batch_id"], "test-batch")
        self.assertEqual(fallback["color_refresh_count"], 0)
        self.assertEqual(
            [player.get("color_hex") for player in json.loads(fallback["players_json"])],
            [None, None],
        )

    def test_rerun_does_not_reschedule_completed_fallback_refresh(self) -> None:
        backfill_existing_game_replays(
            self.db_path,
            apply=True,
            now=self.now,
            batch_id="test-batch",
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE game_replays
                SET color_refresh_count = 1,
                    retry_reason = NULL,
                    next_attempt_at = NULL
                WHERE game_id = 'fallback-game'
                """
            )

        summary = backfill_existing_game_replays(
            self.db_path,
            apply=True,
            now=self.now,
            batch_id="second-batch",
        )

        self.assertEqual(summary["color_refresh_scheduled"], 0)
        self.assertEqual(summary["color_refresh_already_attempted"], 1)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """
                SELECT retry_reason, next_attempt_at, color_refresh_count
                FROM game_replays
                WHERE game_id = 'fallback-game'
                """
            ).fetchone()
        self.assertEqual(row, (None, None, 1))


if __name__ == "__main__":
    unittest.main()
