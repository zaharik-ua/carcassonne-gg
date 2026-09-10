from __future__ import annotations

import unittest

from .carcassonne_lab import (
    CarcassonneLabReplayError,
    build_carcassonne_lab_url,
    encode_movement,
    encode_tile_types,
    fetch_bga_replay_logs,
    normalize_table_id,
)


class CarcassonneLabTest(unittest.TestCase):
    def test_normalizes_table_id(self) -> None:
        self.assertEqual(normalize_table_id(" 909842009 "), "909842009")
        for invalid in ("", "0", "0123", "abc", "12.3"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                normalize_table_id(invalid)

    def test_encodes_tiles_and_movements_like_the_bookmarklet(self) -> None:
        self.assertEqual(encode_tile_types([15, 3, 4]), "B-z")
        self.assertEqual(
            encode_movement({"col": -1, "row": 2, "rotation": 2, "meeple_position": 0}),
            "23K0",
        )

    def test_builds_review_url_with_players_colors_and_meeple(self) -> None:
        logs = [
            {
                "data": [
                    {
                        "type": "gameStateChange",
                        "args": {
                            "args": {
                                "result": [
                                    {"player": "10", "color": "ff0000"},
                                    {"player": "20", "color": "0000ff"},
                                ]
                            }
                        },
                    },
                    {
                        "type": "playTile",
                        "args": [
                            {
                                "piece": "tile",
                                "player_id": "10",
                                "player_name": "Alice A",
                                "type": "3",
                                "x": "0",
                                "y": "0",
                                "ori": "1",
                            }
                        ],
                    },
                    {"type": "playPartisan", "args": [{"piece": "partisan", "pos": "5"}]},
                    {
                        "type": "playTile",
                        "args": [
                            {
                                "piece": "tile",
                                "player_id": "20",
                                "player_name": "Bob",
                                "type": "4",
                                "x": "-1",
                                "y": "2",
                                "ori": "3",
                            }
                        ],
                    },
                ]
            }
        ]

        self.assertEqual(
            build_carcassonne_lab_url(logs),
            "https://www.carcassonnelab.com/#/0/0/B-z/111523K0?players=Alice%20A,Bob&colors=red,blue",
        )

    def test_uses_archive_player_colors_and_safe_fallbacks(self) -> None:
        logs = [
            {
                "data": [
                    {
                        "type": "playTile",
                        "args": {
                            "player_id": "10",
                            "player_name": "Alice",
                            "type": 3,
                            "x": 0,
                            "y": 0,
                            "ori": 1,
                        },
                    },
                    {
                        "type": "playTile",
                        "args": {
                            "player_id": "20",
                            "player_name": "Bob",
                            "type": 4,
                            "x": 1,
                            "y": 0,
                            "ori": 2,
                        },
                    },
                ]
            }
        ]

        url = build_carcassonne_lab_url(logs, [{"id": "10", "color": "008000"}])

        self.assertTrue(url.endswith("?players=Alice,Bob&colors=green,red"))

    def test_requests_archive_when_replay_logs_are_missing(self) -> None:
        calls: list[tuple[str, dict | None]] = []
        responses = iter(
            [
                {"status": "0", "error": "Cannot find gamenotifs log file"},
                {"status": "1", "data": {}},
                {"status": "1", "data": {"logs": [{"data": []}]}},
            ]
        )

        def request(path: str, params: dict | None = None) -> dict:
            calls.append((path, params))
            return next(responses)

        logs = fetch_bga_replay_logs("909842009", request=request, sleep=lambda _seconds: None)

        self.assertEqual(logs, [{"data": []}])
        self.assertEqual(calls[1][0], "/gamereview/gamereview/requestTableArchive.html")
        self.assertEqual(calls[1][1], {"table": "909842009"})

    def test_rejects_bga_replay_errors(self) -> None:
        with self.assertRaisesRegex(CarcassonneLabReplayError, "Invalid session"):
            fetch_bga_replay_logs(
                "909842009",
                request=lambda *_args, **_kwargs: {"status": "0", "error": "Invalid session"},
                sleep=lambda _seconds: None,
            )


if __name__ == "__main__":
    unittest.main()
