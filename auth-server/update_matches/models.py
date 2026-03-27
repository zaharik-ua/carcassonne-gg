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


@dataclass
class MatchTable:
    id: int | str
    url: str
    score0: str
    score1: str
    rank0: str
    rank1: str
    timestamp: int
    player0_clock: int = 0
    player1_clock: int = 0
    status: str = "Finished"


@dataclass
class MatchUpdateResult:
    status: str
    wins0: int = 0
    wins1: int = 0
    player0_id: int | None = None
    player1_id: int | None = None
    players_url: str = ""
    tables: list[MatchTable] = field(default_factory=list)
    message: str | None = None
