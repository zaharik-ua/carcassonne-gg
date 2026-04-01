from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ProfileBgaDataUpdateRequest:
    player_id: str
    bga_player_id: int
    bga_nickname: str
    avatar: str | None = None


@dataclass
class ProfileBgaDataUpdateResult:
    status: str
    bga_nickname: str | None = None
    avatar: str | None = None
    matched_player_id: int | None = None
    source_url: str | None = None
    message: str | None = None
