from __future__ import annotations

from .bga_client import BgaProfileDataClient
from .sqlite_repository import SqliteProfileBgaDataRepository


class ProfileBgaDataUpdateService:
    def __init__(
        self,
        repository: SqliteProfileBgaDataRepository,
        *,
        client: BgaProfileDataClient | None = None,
    ) -> None:
        self.repository = repository
        self.client = client or BgaProfileDataClient()

    def run_for_player(self, player_id: str) -> dict:
        player = self.repository.fetch_player(player_id)
        if player is None:
            raise RuntimeError(f"Profile with id={player_id} was not found")

        before = self.repository.load_profile_snapshot(player.player_id) or {}
        result = self.client.fetch_profile_data(player)
        if result.status not in {"success", "removed"}:
            return {
                "ok": False,
                "player_id": player.player_id,
                "status": result.status,
                "source_url": result.source_url,
                "message": result.message or "Unknown error",
            }

        self.repository.save_player_result(player, result)
        after = self.repository.load_profile_snapshot(player.player_id) or {}

        return {
            "ok": True,
            "player_id": player.player_id,
            "status": result.status,
            "source_url": result.source_url,
            "matched_player_id": result.matched_player_id,
            "updated": (
                str(before.get("bga_nickname") or "") != str(after.get("bga_nickname") or "")
                or str(before.get("avatar") or "") != str(after.get("avatar") or "")
                or str(before.get("status") or "") != str(after.get("status") or "")
            ),
            "before": {
                "bga_nickname": before.get("bga_nickname"),
                "avatar": before.get("avatar"),
                "status": before.get("status"),
            },
            "after": {
                "bga_nickname": after.get("bga_nickname"),
                "avatar": after.get("avatar"),
                "status": after.get("status"),
            },
            "message": result.message or ("Player marked as Removed." if result.status == "removed" else "BGA data updated."),
        }
