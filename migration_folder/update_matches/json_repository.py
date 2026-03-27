from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from .models import MatchUpdateRequest, MatchUpdateResult
from .repository import MatchRepository


class JsonFileMatchRepository(MatchRepository):
    """Reference adapter for local testing.

    File shape:
    {
      "matches": [
        {
          "match_id": 1,
          "target": "ongoing",
          "player0": "...",
          "player1": "...",
          "game_id": 1,
          "start_date": 1735689600,
          "end_date": 1736294400,
          "gtw": 2,
          "stat": false
        }
      ]
    }
    """

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"JSON repository file not found: {self.path}")

    def fetch_matches_to_update(self, *, target: str, limit: int) -> list[MatchUpdateRequest]:
        payload = self._load()
        matches = []
        for row in payload.get("matches", []):
            if row.get("target") != target:
                continue
            if not row.get("needs_update", True):
                continue
            matches.append(self._to_match_request(row))
            if len(matches) >= limit:
                break
        return matches

    def save_match_result(self, match: MatchUpdateRequest, result: MatchUpdateResult) -> None:
        payload = self._load()
        now = datetime.now(timezone.utc).isoformat()
        for row in payload.get("matches", []):
            if str(row.get("match_id")) != str(match.match_id):
                continue
            row["wins0"] = result.wins0
            row["wins1"] = result.wins1
            row["flags"] = result.flags
            row["players_url"] = result.players_url
            row["table_urls"] = result.table_urls
            row["tables_json"] = [asdict(table) for table in result.tables]
            row["player0_id"] = result.player0_id
            row["player1_id"] = result.player1_id
            row["last_error"] = None
            row["updated_at"] = now
            row["needs_update"] = False
            break
        self._save(payload)

    def save_match_error(self, match: MatchUpdateRequest, message: str) -> None:
        payload = self._load()
        now = datetime.now(timezone.utc).isoformat()
        for row in payload.get("matches", []):
            if str(row.get("match_id")) != str(match.match_id):
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

    @staticmethod
    def _to_match_request(row: dict) -> MatchUpdateRequest:
        return MatchUpdateRequest(
            match_id=row["match_id"],
            target=row["target"],
            player0=row["player0"],
            player1=row["player1"],
            game_id=int(row["game_id"]),
            start_date=int(row["start_date"]),
            end_date=int(row["end_date"]) if row.get("end_date") is not None else None,
            player0_id=int(row["player0_id"]) if row.get("player0_id") is not None else None,
            player1_id=int(row["player1_id"]) if row.get("player1_id") is not None else None,
            gtw=int(row.get("gtw", 2)),
            stat=bool(row.get("stat", False)),
            extra={
                key: value
                for key, value in row.items()
                if key not in {
                    "match_id", "target", "player0", "player1", "game_id", "start_date", "end_date",
                    "player0_id", "player1_id", "gtw", "stat", "needs_update", "wins0", "wins1",
                    "flags", "players_url", "table_urls", "tables_json", "last_error", "updated_at",
                }
            },
        )
