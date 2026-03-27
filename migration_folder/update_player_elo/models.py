from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PlayerEloUpdateRequest:
    player_id: str | int
    bga_player_id: int
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class PlayerEloUpdateResult:
    status: str
    elo: int | None = None
    raw_elo: str | None = None
    source_url: str | None = None
    message: str | None = None
