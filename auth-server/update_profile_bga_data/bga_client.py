from __future__ import annotations

from urllib.parse import urlencode

from update_matches.http_session import request_json

from .models import ProfileBgaDataUpdateRequest, ProfileBgaDataUpdateResult


class BgaProfileDataClient:
    def fetch_profile_data(self, player: ProfileBgaDataUpdateRequest) -> ProfileBgaDataUpdateResult:
        params = {"query": player.bga_nickname}
        source_url = f"https://boardgamearena.com/omnibar/omnibar/search.html?{urlencode(params)}"

        try:
            payload = request_json("/omnibar/omnibar/search.html", params=params)
        except Exception as exc:
            return ProfileBgaDataUpdateResult(
                status="error",
                source_url=source_url,
                message=f"HTTP error: {exc}",
            )

        players = None
        if isinstance(payload, dict):
            direct_players = payload.get("players")
            nested_data = payload.get("data")
            nested_players = nested_data.get("players") if isinstance(nested_data, dict) else None
            if isinstance(direct_players, list):
                players = direct_players
            elif isinstance(nested_players, list):
                players = nested_players
        if not isinstance(players, list):
            return ProfileBgaDataUpdateResult(
                status="error",
                source_url=source_url,
                message="BGA response does not contain players array",
            )

        matched = next(
            (
                entry for entry in players
                if str(entry.get("id") or "").strip() == str(player.bga_player_id)
            ),
            None,
        )
        if matched is None:
            return ProfileBgaDataUpdateResult(
                status="error",
                source_url=source_url,
                message=f"Player id {player.bga_player_id} was not found in BGA search results",
            )

        normalized_nickname = str(matched.get("fullname") or "").strip()
        if not normalized_nickname:
            return ProfileBgaDataUpdateResult(
                status="error",
                source_url=source_url,
                matched_player_id=player.bga_player_id,
                message="Matched BGA player does not contain fullname",
            )

        avatar = str(matched.get("avatar") or "").strip() or None
        return ProfileBgaDataUpdateResult(
            status="success",
            bga_nickname=normalized_nickname,
            avatar=avatar,
            matched_player_id=player.bga_player_id,
            source_url=source_url,
        )
