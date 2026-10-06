"""Read one completed BGA table without changing stored duel results."""
from __future__ import annotations

import time


class GameResultNotFound(ValueError):
    pass


def _data(payload: dict) -> dict:
    if str(payload.get("status")) != "1":
        raise RuntimeError("BGA could not load the game. Please try again.")
    return payload.get("data", {})


def fetch_game_result(table_id: str, player_1_id: str, player_2_id: str, *, request=None) -> dict:
    if request is None:
        from .http_session import request_json
        request = request_json

    info = _data(request("/table/table/tableinfos.html", params={"id": table_id})).get("result", {})
    # Use the table's date to avoid truncating a long history between these players.
    def timestamp(*keys):
        for key in keys:
            try:
                value = int(info.get(key) or 0)
                if value > 0:
                    return value
            except (TypeError, ValueError):
                continue
        return 0

    start = timestamp("start", "start_time", "gamestart")
    end = timestamp("end", "end_time", "gameend")
    payload = _data(request("/gamestats/gamestats/getGames.html", params={
        "game_id": 1,
        "player": player_1_id,
        "opponent_id": player_2_id,
        "start_date": max(0, start - 86400) if start else 0,
        "end_date": max(start, end) + 86400 if start or end else int(time.time()),
        "finished": 1,
        "updateStats": 1,
    }))
    expected_players = [str(player_1_id), str(player_2_id)]
    for table in payload.get("tables", []):
        if str(table.get("table_id")) != str(table_id):
            continue
        players = str(table.get("players", "")).split(",")
        if len(players) != 2 or set(players) != set(expected_players):
            break
        scores = str(table.get("scores", "")).split(",")
        ranks = str(table.get("ranks", "")).split(",")
        try:
            if int(table.get("end") or 0) <= 0 or len(scores) != 2 or len(ranks) != 2:
                break
            ordered_scores = [int(scores[players.index(player)]) for player in expected_players]
            ordered_ranks = [int(ranks[players.index(player)]) for player in expected_players]
        except (ValueError, TypeError):
            break
        if ordered_ranks not in ([1, 2], [2, 1]):
            raise GameResultNotFound("This BGA game has no decisive result for these players.")
        wins = [1 if rank == 1 else 0 for rank in ordered_ranks]
        penalties = info.get("penalties", {}) or {}
        clocks = []
        for player in expected_players:
            penalty = penalties.get(player, {}) or {}
            clocks.append(int(str(penalty.get("clock")) == "1" and str(penalty.get("clock_cancelled")) != "1"))
        # Match the existing BGA updater's time-loss handling.
        if clocks[0] and ordered_scores[0] > ordered_scores[1]:
            wins = [0, 1]
        if clocks[1] and ordered_scores[1] > ordered_scores[0]:
            wins = [1, 0]
        return {
            "bga_table_id": str(table_id),
            "player_1_score": ordered_scores[0],
            "player_2_score": ordered_scores[1],
            "player_1_rank": wins[0],
            "player_2_rank": wins[1],
            "player_1_clock": clocks[0],
            "player_2_clock": clocks[1],
            "status": "Finished",
        }
    raise GameResultNotFound("No completed Carcassonne game was found at this Table ID for both duel players. Check the Table ID and try again.")
