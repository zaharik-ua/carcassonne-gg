from __future__ import annotations

import unittest

from .models import MatchUpdateRequest, MatchUpdateResult
from .repository import MatchRepository, TARGET_FINISHED_PENDING
from .service import MatchUpdateService


class _NoReplayRepository(MatchRepository):
    def __init__(self) -> None:
        self.finished = False
        self.replay_scan_called = False

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
        self.replay_scan_called = True
        raise AssertionError("match updater must not scan for replays")

    def finish_update_run(self) -> None:
        self.finished = True


class MatchUpdateServiceReplayTest(unittest.TestCase):
    def test_does_not_scan_or_create_replays(self) -> None:
        repository = _NoReplayRepository()
        service = MatchUpdateService(
            repository,
            games_fetcher=lambda batch: [],
        )
        summary = service.run(targets=[TARGET_FINISHED_PENDING])

        self.assertFalse(repository.replay_scan_called)
        self.assertNotIn("replays_processed", summary)
        self.assertEqual(summary["processed"], 0)
        self.assertTrue(repository.finished)


if __name__ == "__main__":
    unittest.main()
