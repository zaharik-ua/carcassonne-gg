from __future__ import annotations

import io
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch

from . import game_replay_cli


class GameReplayCliTest(unittest.TestCase):
    def test_manual_command_uses_account_rotation_and_forwards_options(self) -> None:
        args = SimpleNamespace(
            db_path="/tmp/auth.sqlite",
            game_id="game-1",
            force=True,
            poll_attempts=7,
            poll_delay=0.5,
        )
        with (
            patch.object(game_replay_cli, "parse_args", return_value=args),
            patch.object(
                game_replay_cli,
                "fetch_and_store_game_replay_with_account_rotation",
                return_value={"status": "ready"},
            ) as fetch_replay,
            redirect_stdout(io.StringIO()),
        ):
            exit_code = game_replay_cli.main()

        self.assertEqual(exit_code, 0)
        fetch_replay.assert_called_once_with(
            "/tmp/auth.sqlite",
            "game-1",
            force=True,
            poll_attempts=7,
            poll_delay=0.5,
        )


if __name__ == "__main__":
    unittest.main()
