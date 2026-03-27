from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PlayerEloUpdateRequest:
    player_id: str
    bga_player_id: int


@dataclass
class PlayerEloUpdateResult:
    status: str
    elo: int | None = None
    raw_elo: str | None = None
    source_url: str | None = None
    message: str | None = None
