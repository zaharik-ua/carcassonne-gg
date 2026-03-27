from __future__ import annotations

from .models import PlayerEloUpdateRequest, PlayerEloUpdateResult
from .repository import PlayerEloRepository


class DatabasePlayerEloRepository(PlayerEloRepository):
    """Template adapter for the target project.

    Replace this class with the real DB implementation after moving the folder.
    Expected behavior:

    - select players that have a BGA numeric id and need Elo refresh
    - store the parsed Elo value in the target player table
    - persist last_error / updated_at fields so retries are visible
    """

    def __init__(self, db_session) -> None:
        self.db_session = db_session

    def fetch_players_to_update(self, *, limit: int) -> list[PlayerEloUpdateRequest]:
        raise NotImplementedError("Implement DB query that returns PlayerEloUpdateRequest objects")

    def save_player_result(self, player: PlayerEloUpdateRequest, result: PlayerEloUpdateResult) -> None:
        raise NotImplementedError("Implement DB write for successful Elo update")

    def save_player_error(self, player: PlayerEloUpdateRequest, message: str) -> None:
        raise NotImplementedError("Implement DB write for failed Elo update")
