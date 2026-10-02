from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from .inspect_replay_tile_draws import find_repeated_player_draws, inspect_replay_tile_draws


class InspectReplayTileDrawsTest(unittest.TestCase):
    @staticmethod
    def _events():
        return [
            {"seq": 131, "type": "pickTile", "player_id": "41973045", "tile_id": 14, "tile_type": 7},
            {"seq": 132, "type": "playTile", "player_id": "other"},
            {"seq": 133, "type": "pickTile", "player_id": "41973045", "tile_id": 33, "tile_type": 14},
            {"seq": 134, "type": "playPartisan", "player_id": "41973045"},
            {"seq": 135, "type": "playTile", "player_id": "41973045", "player_name": "Lawyer"},
        ]

    def test_other_player_placement_does_not_break_the_run(self) -> None:
        events = self._events()
        matches, unknown = find_repeated_player_draws(events)
        self.assertEqual(unknown, 0)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["player_id"], "41973045")
        self.assertEqual(matches[0]["player_name"], "Lawyer")
        self.assertEqual(matches[0]["pick_events"], [events[0], events[2]])
        self.assertEqual(matches[0]["events"], events[:3])
        self.assertEqual(matches[0]["following_playTile"], events[-1])

    def test_same_player_placement_breaks_the_run(self) -> None:
        events = self._events()
        events[1]["player_id"] = "41973045"
        self.assertEqual(find_repeated_player_draws(events)[0], [])

    def test_interleaved_players_have_independent_runs(self) -> None:
        events = [
            {"seq": 1, "type": "pickTile", "player_id": "a"},
            {"seq": 2, "type": "pickTile", "player_id": "b"},
            {"seq": 3, "type": "pickTile", "player_id": "a"},
            {"seq": 4, "type": "playTile", "player_id": "a"},
            {"seq": 5, "type": "pickTile", "player_id": "b"},
            {"seq": 6, "type": "pickTile", "player_id": "b"},
        ]
        matches, _unknown = find_repeated_player_draws(events)
        self.assertEqual([match["player_id"] for match in matches], ["a", "b"])
        self.assertEqual([match["pick_count"] for match in matches], [2, 3])
        self.assertIsNone(matches[1]["following_playTile"])

    def test_missing_player_ids_are_not_grouped_and_numeric_ids_match(self) -> None:
        events = [
            {"type": "pickTile", "player_id": None},
            {"type": "pickTile", "player_id": None},
            {"type": "pickTile", "player_id": 123},
            {"type": "playPartisan", "player_id": 123},
            {"type": "cantPlay", "player_id": 123},
            {"type": "pickTile", "player_id": "123"},
            {"type": "playTile", "player_id": "123"},
            {"type": "pickTile", "player_id": 123},
        ]
        matches, unknown = find_repeated_player_draws(events)
        self.assertEqual(unknown, 2)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["pick_count"], 2)

    def test_database_report_is_read_only_and_supports_filters(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "auth.sqlite"
            with sqlite3.connect(path) as conn:
                conn.execute("""
                    CREATE TABLE game_replays (
                        game_id TEXT, bga_table_id TEXT, status TEXT,
                        carcassonne_lab_url TEXT, events_json TEXT
                    )
                """)
                conn.executemany("INSERT INTO game_replays VALUES (?, ?, ?, ?, ?)", [
                    ("game-a", "101", "ready", "existing-url", json.dumps(self._events())),
                    ("game-b", "102", "ready", None, json.dumps(self._events())),
                    ("game-c", "103", "error", None, "invalid json"),
                ])
            before = path.read_bytes()
            report = inspect_replay_tile_draws(db_path=path)
            self.assertEqual(report["bga_table_ids"], ["101", "102"])
            self.assertEqual(report["matches_found"], 2)
            self.assertEqual(report["status"], "partial")
            self.assertEqual(report["errors"][0]["game_id"], "game-c")
            filtered = inspect_replay_tile_draws(db_path=path, with_url=True, player_id="41973045")
            self.assertEqual(filtered["bga_table_ids"], ["101"])
            self.assertEqual(filtered["matches"][0]["game_id"], "game-a")
            self.assertEqual(inspect_replay_tile_draws(db_path=path, table_id="102")["matches_found"], 1)
            self.assertEqual(inspect_replay_tile_draws(db_path=path, player_id="other")["matches_found"], 0)
            self.assertEqual(path.read_bytes(), before)

    def test_events_file_reports_original_pick_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.json"
            path.write_text(json.dumps(self._events()), encoding="utf-8")
            before = path.read_bytes()
            report = inspect_replay_tile_draws(events_file=path, player_id="41973045")
            self.assertEqual(report["matches_found"], 1)
            self.assertEqual([event["seq"] for event in report["matches"][0]["pick_events"]], [131, 133])
            self.assertEqual(path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
