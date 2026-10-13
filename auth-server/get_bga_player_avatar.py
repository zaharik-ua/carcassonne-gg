"""Read a player's avatar from BGA; does not require or modify a GG profile."""
from __future__ import annotations

import argparse
import contextlib
import json
import sys


class BgaPlayerNotFound(ValueError):
    pass


def fetch_player_avatar(player: str, request=None) -> dict:
    player = str(player).strip()
    if not player or len(player) > 100:
        raise ValueError("Provide a BGA player ID or nickname (up to 100 characters)")
    if request is None:
        from update_matches.http_session import request_json
        request = request_json

    if player.isascii() and player.isdigit():
        # The same POST endpoint used by BGA's player selector for a known ID.
        payload = request("/player/player/getPlayerInfos.html", method="POST", data={"id": player})
        matched = payload
        for _ in range(2):
            if isinstance(matched, dict) and "id" not in matched:
                matched = matched.get("data")
        if not isinstance(matched, dict) or str(matched.get("id", "")).strip() != player:
            raise ValueError("BGA response does not contain the requested player")
    else:
        # Reuse the nickname search used by update_profile_bga_data/bga_client.py.
        payload = request("/omnibar/omnibar/search.html", params={"query": player})
        players = payload.get("players") if isinstance(payload, dict) else None
        if not isinstance(players, list) and isinstance(payload, dict):
            nested = payload.get("data")
            players = nested.get("players") if isinstance(nested, dict) else None
        if not isinstance(players, list):
            raise ValueError("BGA response does not contain players array")
        matched = next((entry for entry in players if isinstance(entry, dict)
                        and str(entry.get("fullname") or entry.get("name") or "").strip().casefold()
                        == player.casefold()), None)
        if matched is None:
            raise BgaPlayerNotFound(f"Player not found in BGA: {player}")

    player_id = str(matched.get("id") or "").strip()
    if not player_id.isascii() or not player_id.isdigit():
        raise ValueError("BGA response does not contain a valid player ID")
    if "avatar" not in matched:
        raise ValueError("BGA response does not contain avatar")
    return {
        "ok": True,
        "player_id": player_id,
        "bga_nickname": str(matched.get("fullname") or matched.get("name") or "").strip(),
        "avatar": str(matched.get("avatar") or "").strip() or None,
    }


def main() -> int:
    from dotenv import load_dotenv
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--player", required=True, help="BGA player ID or exact nickname")
    args = parser.parse_args()
    try:
        with contextlib.redirect_stdout(sys.stderr):
            result = fetch_player_avatar(args.player)
    except BgaPlayerNotFound as error:
        result = {"ok": False, "not_found": True, "message": str(error)}
    except Exception as error:
        print(f"BGA avatar lookup failed: {error}", file=sys.stderr)
        result = {"ok": False, "message": "Could not load the BGA avatar. Please try again."}
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
