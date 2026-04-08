from __future__ import annotations

from abc import ABC, abstractmethod

from .models import MatchUpdateRequest, MatchUpdateResult


TARGET_ONGOING = "ongoing"
TARGET_FINISHED_PENDING = "finished_pending"
TARGET_EMPTY_FINISHED = "empty_finished"
KNOWN_TARGETS = {TARGET_ONGOING, TARGET_FINISHED_PENDING, TARGET_EMPTY_FINISHED}


class MatchRepository(ABC):
    @abstractmethod
    def fetch_duels_for_match(self, *, match_id: str) -> list[MatchUpdateRequest]:
        """Return all duels for a specific match id."""

    @abstractmethod
    def fetch_duel_by_id(self, *, duel_id: str) -> list[MatchUpdateRequest]:
        """Return exactly one duel for a specific duel id."""

    @abstractmethod
    def fetch_matches_to_update(self, *, target: str, limit: int) -> list[MatchUpdateRequest]:
        """Return matches that currently need updating for the requested target."""

    @abstractmethod
    def save_match_result(self, match: MatchUpdateRequest, result: MatchUpdateResult) -> None:
        """Persist a successful BGA update result."""

    @abstractmethod
    def save_match_error(self, match: MatchUpdateRequest, message: str) -> None:
        """Persist an update failure."""
