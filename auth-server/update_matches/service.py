from __future__ import annotations

from .config import MATCH_UPDATE_BATCH_SIZE
from .match_fetcher import get_games_batch
from .models import MatchUpdateRequest
from .repository import KNOWN_TARGETS, MatchRepository


class MatchUpdateService:
    def __init__(self, repository: MatchRepository, batch_size: int = MATCH_UPDATE_BATCH_SIZE) -> None:
        self.repository = repository
        self.batch_size = batch_size

    def run(self, *, targets: list[str], total_limit: int | None = None, match_id: str | None = None) -> dict:
        if match_id:
            return self._run_match(match_id=match_id)

        for target in targets:
            if target not in KNOWN_TARGETS:
                raise ValueError(f"Unknown target: {target}")

        summary = {
            "targets": targets,
            "processed": 0,
            "updated": 0,
            "failed": 0,
        }
        remaining = total_limit

        for target in targets:
            seen_ids: set[str] = set()
            while remaining is None or remaining > 0:
                limit = self.batch_size if remaining is None else min(self.batch_size, remaining)
                batch = self.repository.fetch_matches_to_update(target=target, limit=limit)
                batch = self._drop_already_seen(batch, seen_ids)
                if not batch:
                    break

                for match in batch:
                    seen_ids.add(str(match.match_id))

                results = get_games_batch(batch)
                for match, result in zip(batch, results):
                    summary["processed"] += 1
                    if result.status == "success":
                        self.repository.save_match_result(match, result)
                        summary["updated"] += 1
                    else:
                        self.repository.save_match_error(match, result.message or "Unknown error")
                        summary["failed"] += 1

                if remaining is not None:
                    remaining -= len(batch)
                    if remaining <= 0:
                        break

        return summary

    def _run_match(self, *, match_id: str) -> dict:
        batch = self.repository.fetch_lineups_for_match(match_id=match_id)
        summary = {
            "match_id": match_id,
            "processed": 0,
            "updated": 0,
            "failed": 0,
        }
        if not batch:
            return summary

        results = get_games_batch(batch)
        for match, result in zip(batch, results):
            summary["processed"] += 1
            if result.status == "success":
                self.repository.save_match_result(match, result)
                summary["updated"] += 1
            else:
                self.repository.save_match_error(match, result.message or "Unknown error")
                summary["failed"] += 1
        return summary

    @staticmethod
    def _drop_already_seen(batch: list[MatchUpdateRequest], seen_ids: set[str]) -> list[MatchUpdateRequest]:
        return [match for match in batch if str(match.match_id) not in seen_ids]
