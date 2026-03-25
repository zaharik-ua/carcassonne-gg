from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MatchUpdateRequest:
    match_id: str | int
    target: str
    player0: str
    player1: str
    game_id: int
    start_date: int
    end_date: int | None = None
    player0_id: int | None = None
    player1_id: int | None = None
    gtw: int = 2
    stat: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    def to_bga_payload(self) -> dict[str, Any]:
        payload = {
            "player0": self.player0,
            "player1": self.player1,
            "game_id": self.game_id,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "player0_id": self.player0_id,
            "player1_id": self.player1_id,
            "gtw": self.gtw,
            "stat": self.stat,
        }
        payload.update(self.extra)
        return payload


@dataclass
class MatchTable:
    id: int | str
    url: str
    score0: str
    score1: str
    rank0: str
    rank1: str
    timestamp: int


@dataclass
class MatchUpdateResult:
    status: str
    wins0: int = 0
    wins1: int = 0
    player0_id: int | None = None
    player1_id: int | None = None
    players_url: str = ""
    flags: str = ""
    tables: list[MatchTable] = field(default_factory=list)
    message: str | None = None

    @property
    def table_urls(self) -> list[str]:
        return [table.url for table in self.tables]
