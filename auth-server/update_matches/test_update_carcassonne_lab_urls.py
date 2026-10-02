from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from . import game_replay
from . import update_carcassonne_lab_urls as cli
from .game_replay import build_carcassonne_lab_url, ensure_game_replays_schema, normalize_replay_events
from .update_carcassonne_lab_urls import restore_saved_discards, update_existing_carcassonne_lab_urls


class UpdateCarcassonneLabUrlsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        self.query = "players=Alpha%2C%20Player,Beta&colors=red,blue&custom=kept"
        self.old_url = f"https://www.carcassonnelab.com/#/0/0/CED/34L523H0?{self.query}"
        self.new_url = f"https://www.carcassonnelab.com/#/0/0.1/xor/34L523H0?{self.query}"
        self.players = [
            {"player_id": "100", "player_name": "Alpha", "meeple_color": "red"},
            {"player_id": "200", "player_name": "Beta", "meeple_color": "blue"},
        ]
        self.legacy_events = normalize_replay_events([{"data": [
            event for event in self._logs()[0]["data"] if event["type"] != "cantPlay"
        ]}])
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE games (id TEXT PRIMARY KEY)")
            ensure_game_replays_schema(conn)
            for game_id, table_id, url, status in (
                ("a", "101", self.old_url, "ready"),
                ("b", "102", None, "ready"),
                ("c", "103", "", "ready"),
                ("d", "104", "   ", "pending"),
                ("e", "105", self.old_url, "error"),
            ):
                conn.execute("INSERT INTO games (id) VALUES (?)", (game_id,))
                conn.execute(
                    """
                    INSERT INTO game_replays (
                        game_id, bga_table_id, status, carcassonne_lab_url, players_json,
                        events_json, board_stats_json, meeple_stats_json, scoring_json,
                        player_time_json, color_source, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '{}', '{}', '{}', '{}', 'bga', '2020-01-01')
                    """,
                    (game_id, table_id, status, url, json.dumps(self.players), json.dumps(self.legacy_events)),
                )
        bga_patch = patch.object(game_replay, "fetch_bga_replay", side_effect=AssertionError("No BGA requests"))
        self.bga_fetch = bga_patch.start()
        self.addCleanup(bga_patch.stop)

    def _snapshot(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return {
                row["game_id"]: dict(row)
                for row in conn.execute("SELECT * FROM game_replays ORDER BY game_id")
            }

    def _set_events(self, game_id, events):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE game_replays SET events_json = ? WHERE game_id = ?", (json.dumps(events), game_id))

    @staticmethod
    def _logs():
        return [{"data": [
            {"type": "pickTile", "args": {"id": "49", "type": "17"}},
            {"type": "playTile", "args": {
                "player_id": "100", "player_name": "Alpha renamed", "type": "17",
                "x": "-2", "y": "3", "ori": "4",
            }},
            {"type": "playPartisan", "args": {"pos": "5"}},
            {"type": "pickTile", "args": {"id": "72", "type": "24"}},
            {"type": "cantPlay", "args": {"tile_id": "72"}},
            {"type": "pickTile", "args": {"id": "7", "type": "4"}},
            {"type": "playTile", "args": {
                "player_id": "200", "player_name": "Beta", "type": "4",
                "x": "-1", "y": "2", "ori": "1",
            }},
        ]}]

    def test_dry_run_computes_changes_without_writes_or_bga(self) -> None:
        before = self._snapshot()
        summary = update_existing_carcassonne_lab_urls(self.db_path)
        self.assertEqual(summary["mode"], "dry-run")
        self.assertEqual(summary["candidates"], 2)
        self.assertEqual(summary["would_update"], 2)
        self.assertEqual(summary["inferred_discards"], 2)
        self.assertEqual(summary["updated"], 0)
        self.assertEqual(self._snapshot(), before)
        self.bga_fetch.assert_not_called()
        self.assertFalse(self.db_path.with_name("auth.sqlite.bga-replay-worker.lock").exists())

    def test_apply_restores_legacy_discards_and_only_updates_populated_urls(self) -> None:
        before = self._snapshot()
        summary = update_existing_carcassonne_lab_urls(self.db_path, apply=True)
        self.assertEqual(summary["updated"], 2)
        self.assertEqual(summary["recovered_legacy_rows"], 2)
        after = self._snapshot()
        for game_id in ("a", "e"):
            self.assertEqual(after[game_id]["carcassonne_lab_url"], self.new_url)
            for column in before[game_id]:
                if column not in {"carcassonne_lab_url", "updated_at"}:
                    self.assertEqual(after[game_id][column], before[game_id][column])
        for game_id in ("b", "c", "d"):
            self.assertEqual(after[game_id], before[game_id])
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM bga_replay_requests").fetchone()[0], 0)
        self.bga_fetch.assert_not_called()

    def test_rerun_is_idempotent_without_checkpoints(self) -> None:
        update_existing_carcassonne_lab_urls(self.db_path, apply=True)
        before = self._snapshot()
        summary = update_existing_carcassonne_lab_urls(self.db_path, apply=True)
        self.assertEqual(summary["updated"], 0)
        self.assertEqual(summary["unchanged"], 2)
        self.assertEqual(self._snapshot(), before)
        self.assertFalse(self.db_path.with_name("auth.sqlite.carcassonne-lab-url-backfill.json").exists())

    def test_saved_cant_play_events_are_not_counted_twice(self) -> None:
        self._set_events("a", normalize_replay_events(self._logs()))
        summary = update_existing_carcassonne_lab_urls(self.db_path, apply=True)
        self.assertEqual(summary["updated"], 2)
        self.assertEqual(summary["inferred_discards"], 1)
        self.assertEqual(self._snapshot()["a"]["carcassonne_lab_url"], self.new_url)

    def test_optional_legacy_raw_logs_are_reused_locally(self) -> None:
        self._set_events("a", [])
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("ALTER TABLE game_replays ADD COLUMN logs_json TEXT")
            conn.execute("UPDATE game_replays SET logs_json = ? WHERE game_id = 'a'", (json.dumps(self._logs()),))
        before = self._snapshot()
        summary = update_existing_carcassonne_lab_urls(self.db_path, apply=True)
        self.assertEqual(summary["updated"], 2)
        self.assertEqual(summary["raw_log_rows"], 1)
        self.assertEqual(self._snapshot()["a"]["logs_json"], before["a"]["logs_json"])
        self.bga_fetch.assert_not_called()

    def test_incomplete_draw_history_is_reported_and_preserves_the_url(self) -> None:
        self._set_events("a", self.legacy_events[1:])
        before = self._snapshot()
        summary = update_existing_carcassonne_lab_urls(self.db_path, apply=True)
        self.assertEqual(summary["status"], "partial")
        self.assertEqual(summary["insufficient_data"], 1)
        self.assertEqual(summary["updated"], 1)
        self.assertEqual(summary["errors"][0]["game_id"], "a")
        self.assertEqual(self._snapshot()["a"], before["a"])

    def test_ambiguous_final_draw_is_not_assumed_to_be_a_discard(self) -> None:
        self._set_events("a", [*self.legacy_events, {"type": "pickTile", "tile_id": 1, "tile_type": 1}])
        before = self._snapshot()
        summary = update_existing_carcassonne_lab_urls(self.db_path, apply=True)
        self.assertEqual(summary["insufficient_data"], 1)
        self.assertEqual(self._snapshot()["a"], before["a"])

    def test_explicit_final_discards_are_encoded(self) -> None:
        events = normalize_replay_events(self._logs())
        events = [event for event in events if event.get("tile_id") != 72]
        self._set_events("a", [*events, {"type": "cantPlay", "tile_id": 72, "tile_type": 24}])
        summary = update_existing_carcassonne_lab_urls(self.db_path, apply=True)
        self.assertEqual(summary["updated"], 2)
        self.assertIn("#/0/0.2/xhf/", self._snapshot()["a"]["carcassonne_lab_url"])

    def test_repeated_draw_ids_and_mismatched_placements_are_reported(self) -> None:
        for events in (
            [self.legacy_events[0], *self.legacy_events],
            [*self.legacy_events[:-1], {**self.legacy_events[-1], "tile_type": 3}],
        ):
            with self.subTest(events=events):
                self._set_events("a", events)
                summary = update_existing_carcassonne_lab_urls(self.db_path)
                self.assertEqual(summary["insufficient_data"], 1)

    def test_unknown_discarded_tiles_do_not_replace_urls(self) -> None:
        events = [dict(event) for event in self.legacy_events]
        events[3]["tile_id"] = 73
        self._set_events("a", events)
        before = self._snapshot()
        summary = update_existing_carcassonne_lab_urls(self.db_path, apply=True)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(self._snapshot()["a"], before["a"])

    def test_all_71_placements_need_no_draw_history(self) -> None:
        events = [{"type": "playTile"} for _index in range(71)]
        restored, inferred = restore_saved_discards(events)
        self.assertEqual(restored, events)
        self.assertEqual(inferred, 0)

    def test_infers_initial_and_consecutive_discard_positions(self) -> None:
        base = [event for event in self.legacy_events if event.get("tile_id") != 72]
        for index, draws, expected in (
            (0, [(1, 1)], "0.0/vWD"),
            (3, [(72, 24)], "0.1/xor"),
            (3, [(47, 16), (48, 17)], "0.1.1/SmND"),
        ):
            with self.subTest(expected=expected):
                events = [
                    *base[:index],
                    *({"type": "pickTile", "tile_id": tile_id, "tile_type": tile_type} for tile_id, tile_type in draws),
                    *base[index:],
                ]
                restored, inferred = restore_saved_discards(events)
                self.assertEqual(inferred, len(draws))
                self.assertIn(f"#/0/{expected}/34L523H0?", build_carcassonne_lab_url(restored, self.players))

    def test_active_worker_leases_are_deferred(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE game_replays SET lease_until = '2099-01-01' WHERE game_id = 'a'")
        before = self._snapshot()
        summary = update_existing_carcassonne_lab_urls(self.db_path, apply=True)
        self.assertEqual(summary["deferred"], 1)
        self.assertEqual(summary["updated"], 1)
        self.assertEqual(self._snapshot()["a"], before["a"])

    def test_fatal_sql_errors_roll_back_the_entire_batch(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TRIGGER reject_last_url BEFORE UPDATE ON game_replays
                WHEN NEW.game_id = 'e' BEGIN SELECT RAISE(ABORT, 'forced error'); END
            """)
        before = self._snapshot()
        with self.assertRaises(sqlite3.IntegrityError):
            update_existing_carcassonne_lab_urls(self.db_path, apply=True)
        self.assertEqual(self._snapshot(), before)

    def test_cli_defaults_to_local_dry_run(self) -> None:
        output = io.StringIO()
        with patch("sys.argv", ["update_carcassonne_lab_urls.py", "--db-path", str(self.db_path)]), redirect_stdout(output):
            code = cli.main()
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue())["would_update"], 2)
        self.bga_fetch.assert_not_called()

    def test_missing_database_is_not_created(self) -> None:
        missing = self.db_path.with_name("missing.sqlite")
        with self.assertRaises(FileNotFoundError):
            update_existing_carcassonne_lab_urls(missing, apply=True)
        self.assertFalse(missing.exists())


if __name__ == "__main__":
    unittest.main()
