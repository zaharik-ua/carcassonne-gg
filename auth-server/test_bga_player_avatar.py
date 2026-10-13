import unittest

from get_bga_player_avatar import BgaPlayerNotFound, fetch_player_avatar


class PlayerAvatarTests(unittest.TestCase):
    def test_id_lookup_uses_post_and_checks_returned_id(self):
        calls = []

        def request(path, **kwargs):
            calls.append((path, kwargs))
            return {"status": 1, "data": {"data": {"id": 95236669, "name": "Player", "avatar": "hash"}}}

        result = fetch_player_avatar("95236669", request)
        self.assertEqual(result["player_id"], "95236669")
        self.assertEqual(result["avatar"], "hash")
        self.assertEqual(calls, [("/player/player/getPlayerInfos.html", {
            "method": "POST", "data": {"id": "95236669"},
        })])
        with self.assertRaisesRegex(ValueError, "requested player"):
            fetch_player_avatar("123", request)

    def test_nickname_search_matches_exact_name_not_first_result(self):
        entries = [{"id": "1", "fullname": "ep45other", "avatar": "wrong"},
                   {"id": "2", "fullname": "Ep45", "avatar": "_def_2276"}]
        for payload in [{"players": entries}, {"data": {"players": entries}}]:
            with self.subTest(payload=payload):
                result = fetch_player_avatar(" ep45 ", lambda *args, **kwargs: payload)
                self.assertEqual(result["player_id"], "2")
                self.assertEqual(result["avatar"], "_def_2276")
        with self.assertRaises(BgaPlayerNotFound):
            fetch_player_avatar("missing", lambda *args, **kwargs: {"players": entries})

    def test_avatar_formats_are_returned_without_rewriting(self):
        for avatar in ["hash", "_def_1", "x00000", "https://example.com/photo.jpg?h=test", None]:
            with self.subTest(avatar=avatar):
                result = fetch_player_avatar("123", lambda *args, **kwargs: {
                    "data": {"id": "123", "avatar": avatar},
                })
                self.assertEqual(result["avatar"], avatar)

    def test_invalid_input_and_malformed_responses_are_errors(self):
        for player in ["", "a" * 101]:
            with self.assertRaises(ValueError):
                fetch_player_avatar(player, lambda *args, **kwargs: self.fail("Should not fetch"))
        for payload in [{}, {"data": {"id": "123"}}, {"data": {"data": None}}]:
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                fetch_player_avatar("123", lambda *args, **kwargs: payload)
        with self.assertRaises(ValueError):
            fetch_player_avatar("Player", lambda *args, **kwargs: {"data": {}})


if __name__ == "__main__":
    unittest.main()
