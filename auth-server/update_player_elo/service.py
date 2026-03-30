from __future__ import annotations

from .bga_client import BgaEloClient
from .config import PLAYER_ELO_BATCH_SIZE
from .repository import PlayerEloRepository


class PlayerEloUpdateService:
    def __init__(
        self,
        repository: PlayerEloRepository,
        *,
        batch_size: int = PLAYER_ELO_BATCH_SIZE,
        client: BgaEloClient | None = None,
    ) -> None:
        self.repository = repository
        self.batch_size = batch_size
        self.client = client or BgaEloClient()

    def run(self, *, total_limit: int | None = None) -> dict:
        return self.run_with_mode(total_limit=total_limit, selection_mode="stale_first")

    def run_with_mode(self, *, total_limit: int | None = None, selection_mode: str = "stale_first") -> dict:
        summary = {
            "processed": 0,
            "updated": 0,
            "failed": 0,
            "selection_mode": selection_mode,
        }
        remaining = total_limit
        seen_ids: set[str] = set()

        while remaining is None or remaining > 0:
            limit = self.batch_size if remaining is None else min(self.batch_size, remaining)
            batch = self.repository.fetch_players_to_update(
                limit=limit,
                selection_mode=selection_mode,
                exclude_player_ids=seen_ids,
            )
            if not batch:
                break

            for player in batch:
                seen_ids.add(str(player.player_id))
                result = self.client.fetch_player_elo(player)
                summary["processed"] += 1
                if result.status == "success":
                    self.repository.save_player_result(player, result)
                    summary["updated"] += 1
                else:
                    self.repository.save_player_error(player, result.message or "Unknown error")
                    summary["failed"] += 1

            if remaining is not None:
                remaining -= len(batch)

        return summary
