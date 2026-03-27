from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .models import PlayerEloUpdateRequest, PlayerEloUpdateResult
from .repository import PlayerEloRepository


class JsonFilePlayerEloRepository(PlayerEloRepository):
    """Reference adapter for local testing.

    File shape:
    {
      "players": [
        {
          "player_id": 1,
          "bga_player_id": 12345678,
          "needs_update": true
        }
      ]
    }
    """

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"JSON repository file not found: {self.path}")

    def fetch_players_to_update(self, *, limit: int) -> list[PlayerEloUpdateRequest]:
        payload = self._load()
        players = []
        for row in payload.get("players", []):
            if not row.get("needs_update", True):
                continue
            players.append(
                PlayerEloUpdateRequest(
                    player_id=row["player_id"],
                    bga_player_id=int(row["bga_player_id"]),
                    extra={
                        key: value
                        for key, value in row.items()
                        if key not in {
                            "player_id",
                            "bga_player_id",
                            "needs_update",
                            "elo",
                            "elo_raw",
                            "elo_url",
                            "last_error",
                            "updated_at",
                        }
                    },
                )
            )
            if len(players) >= limit:
                break
        return players

    def save_player_result(self, player: PlayerEloUpdateRequest, result: PlayerEloUpdateResult) -> None:
        payload = self._load()
        now = datetime.now(timezone.utc).isoformat()
        for row in payload.get("players", []):
            if str(row.get("player_id")) != str(player.player_id):
                continue
            row["elo"] = result.elo
            row["elo_raw"] = result.raw_elo
            row["elo_url"] = result.source_url
            row["last_error"] = None
            row["updated_at"] = now
            row["needs_update"] = False
            break
        self._save(payload)

    def save_player_error(self, player: PlayerEloUpdateRequest, message: str) -> None:
        payload = self._load()
        now = datetime.now(timezone.utc).isoformat()
        for row in payload.get("players", []):
            if str(row.get("player_id")) != str(player.player_id):
                continue
            row["last_error"] = message
            row["updated_at"] = now
            break
        self._save(payload)

    def _load(self) -> dict:
        with self.path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _save(self, payload: dict) -> None:
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
