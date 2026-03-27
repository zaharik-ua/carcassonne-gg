from __future__ import annotations

from abc import ABC, abstractmethod

from .models import PlayerEloUpdateRequest, PlayerEloUpdateResult


class PlayerEloRepository(ABC):
    @abstractmethod
    def fetch_players_to_update(self, *, limit: int) -> list[PlayerEloUpdateRequest]:
        """Return DB rows that currently need a BGA Elo refresh."""

    @abstractmethod
    def save_player_result(self, player: PlayerEloUpdateRequest, result: PlayerEloUpdateResult) -> None:
        """Persist a successful Elo refresh."""

    @abstractmethod
    def save_player_error(self, player: PlayerEloUpdateRequest, message: str) -> None:
        """Persist a failed Elo refresh."""
