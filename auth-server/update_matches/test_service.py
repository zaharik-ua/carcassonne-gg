from __future__ import annotations

import unittest
from unittest.mock import patch

from . import service as service_module
from .models import MatchUpdateRequest, MatchUpdateResult
from .repository import MatchRepository, TARGET_FINISHED_PENDING
from .service import MatchUpdateService


class _ReplayRepository(MatchRepository):
    def __init__(self, game_ids: list[str] | None = None) -> None:
        self.game_ids = game_ids or []
        self.finished = False
        self.replay_scan_called = False

    def fetch_duels_for_match(self, *, match_id: str) -> list[MatchUpdateRequest]:
        return []

    def fetch_duel_by_id(self, *, duel_id: str) -> list[MatchUpdateRequest]:
        return [
            MatchUpdateRequest(
                match_id=duel_id,
                target="manual_duel",
                player0="Alpha",
                player1="Beta",
                game_id=1,
                start_date=1,
                end_date=2,
            )
        ]

    def fetch_matches_to_update(self, *, target: str, limit: int) -> list[MatchUpdateRequest]:
        return []

    def save_match_result(self, match: MatchUpdateRequest, result: MatchUpdateResult) -> list[str]:
        return self.game_ids

    def save_match_error(self, match: MatchUpdateRequest, message: str) -> None:
        return None

    def fetch_game_ids_pending_replay(self, *, limit: int) -> list[str]:
        self.replay_scan_called = True
        raise AssertionError("match updater must not scan for replays")

    def finish_update_run(self) -> None:
        self.finished = True


class MatchUpdateServiceReplayTest(unittest.TestCase):
    def test_fetches_replays_only_for_games_returned_as_newly_created(self) -> None:
        repository = _ReplayRepository(["game-1", "game-2"])
        calls: list[str] = []

        def fetch_replay(game_id: str) -> None:
            calls.append(game_id)
            if game_id == "game-2":
                raise RuntimeError("temporary BGA error")

        service = MatchUpdateService(
            repository,
            games_fetcher=lambda batch: [MatchUpdateResult(status="success")],
            replay_fetcher=fetch_replay,
        )
        summary = service.run(targets=[TARGET_FINISHED_PENDING], duel_id="duel-1")

        self.assertEqual(calls, ["game-1", "game-2"])
        self.assertFalse(repository.replay_scan_called)
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["updated"], 1)
        self.assertEqual(summary["replays_processed"], 2)
        self.assertEqual(summary["replays_ready"], 1)
        self.assertEqual(summary["replays_failed"], 1)
        self.assertTrue(repository.finished)

    def test_does_not_scan_for_games_missing_replays(self) -> None:
        repository = _ReplayRepository()
        service = MatchUpdateService(
            repository,
            games_fetcher=lambda batch: [],
            replay_fetcher=lambda game_id: self.fail(f"unexpected replay fetch: {game_id}"),
        )

        summary = service.run(targets=[TARGET_FINISHED_PENDING])

        self.assertFalse(repository.replay_scan_called)
        self.assertEqual(summary["replays_processed"], 0)

    def test_sqlite_repository_path_enables_default_replay_fetcher(self) -> None:
        repository = _ReplayRepository(["game-1"])
        repository.db_path = "/tmp/auth.sqlite"

        with patch.object(
            service_module,
            "fetch_and_store_game_replay_with_account_rotation",
            return_value={"status": "ready"},
        ) as fetch_replay:
            service = MatchUpdateService(
                repository,
                games_fetcher=lambda batch: [MatchUpdateResult(status="success")],
            )
            summary = service.run(targets=[], duel_id="duel-1")

        fetch_replay.assert_called_once_with("/tmp/auth.sqlite", "game-1")
        self.assertEqual(summary["replays_ready"], 1)


if __name__ == "__main__":
    unittest.main()
