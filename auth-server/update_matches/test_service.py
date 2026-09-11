from __future__ import annotations

import unittest

from .models import MatchUpdateRequest, MatchUpdateResult
from .repository import MatchRepository, TARGET_FINISHED_PENDING
from .service import MatchUpdateService


class _ReplayRepository(MatchRepository):
    def __init__(self, game_ids: list[str]) -> None:
        self.game_ids = game_ids
        self.finished = False

    def fetch_duels_for_match(self, *, match_id: str) -> list[MatchUpdateRequest]:
        return []

    def fetch_duel_by_id(self, *, duel_id: str) -> list[MatchUpdateRequest]:
        return []

    def fetch_matches_to_update(self, *, target: str, limit: int) -> list[MatchUpdateRequest]:
        return []

    def save_match_result(self, match: MatchUpdateRequest, result: MatchUpdateResult) -> None:
        return None

    def save_match_error(self, match: MatchUpdateRequest, message: str) -> None:
        return None

    def fetch_game_ids_pending_replay(self, *, limit: int) -> list[str]:
        return self.game_ids[:limit]

    def finish_update_run(self) -> None:
        self.finished = True


class MatchUpdateServiceReplayTest(unittest.TestCase):
    def test_syncs_pending_replays_and_keeps_individual_failures_non_fatal(self) -> None:
        repository = _ReplayRepository(["game-1", "game-2"])
        calls: list[str] = []

        def fetch_replay(game_id: str) -> None:
            calls.append(game_id)
            if game_id == "game-2":
                raise RuntimeError("temporary BGA error")

        service = MatchUpdateService(
            repository,
            replay_batch_size=100,
            replay_fetcher=fetch_replay,
        )
        summary = service.run(targets=[TARGET_FINISHED_PENDING])

        self.assertEqual(calls, ["game-1", "game-2"])
        self.assertEqual(summary["replays_processed"], 2)
        self.assertEqual(summary["replays_ready"], 1)
        self.assertEqual(summary["replays_failed"], 1)
        self.assertTrue(repository.finished)


if __name__ == "__main__":
    unittest.main()
