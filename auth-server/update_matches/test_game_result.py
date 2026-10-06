import unittest

from .game_result import GameResultNotFound, fetch_game_result


class GameResultTest(unittest.TestCase):
    def setUp(self):
        self.table = {"table_id": "1234567890", "players": "22,11", "scores": "90,100",
                      "ranks": "2,1", "start": 1000000, "end": 1003600}
        self.info = {"gamestart": 1000000, "gameend": 1003600, "penalties": {}}
        self.requests = []

    def request(self, endpoint, *, params):
        self.requests.append((endpoint, params))
        data = {"result": self.info} if "tableinfos" in endpoint else {"tables": [self.table]}
        return {"status": "1", "data": data}

    def fetch(self):
        return fetch_game_result("1234567890", "11", "22", request=self.request)

    def test_exact_table_reorders_scores_and_ranks_for_duel_players(self):
        result = self.fetch()
        self.assertEqual(result["player_1_score"], 100)
        self.assertEqual(result["player_2_score"], 90)
        self.assertEqual(result["player_1_rank"], 1)
        self.assertEqual(result["player_2_rank"], 0)
        self.assertEqual(self.requests[1][1]["game_id"], 1)
        self.assertEqual(self.requests[1][1]["start_date"], 1000000 - 86400)
        self.assertEqual(self.requests[1][1]["end_date"], 1003600 + 86400)

    def test_wrong_table_wrong_players_multiplayer_and_unfinished_games_are_rejected(self):
        for field, value in [("table_id", "987654321"), ("players", "11,33"),
                             ("players", "11,22,33"), ("end", 0), ("scores", None), ("ranks", "1,1")]:
            with self.subTest(field=field, value=value):
                original = self.table[field]
                self.table[field] = value
                with self.assertRaises(GameResultNotFound):
                    self.fetch()
                self.table[field] = original

    def test_active_clock_penalty_overrides_higher_score_and_cancelled_penalty_does_not(self):
        self.info["penalties"] = {"11": {"clock": "1", "clock_cancelled": "0"}}
        result = self.fetch()
        self.assertEqual((result["player_1_rank"], result["player_2_rank"]), (0, 1))
        self.assertEqual(result["player_1_clock"], 1)
        self.info["penalties"]["11"]["clock_cancelled"] = "1"
        result = self.fetch()
        self.assertEqual(result["player_1_clock"], 0)
        self.assertEqual(result["player_1_rank"], 1)

    def test_bga_error_is_not_reported_as_a_missing_game(self):
        with self.assertRaises(RuntimeError):
            fetch_game_result("1234567890", "11", "22", request=lambda *args, **kwargs: {"status": "0"})


if __name__ == "__main__":
    unittest.main()
