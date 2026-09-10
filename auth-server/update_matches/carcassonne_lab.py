from __future__ import annotations

import time
from collections.abc import Callable
from math import isfinite
from urllib.parse import quote

ALPHABET = [
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "J",
    "K",
    "L",
    "M",
    "N",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "i",
    "j",
    "k",
    "m",
    "n",
    "o",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z",
]
BASE64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
TILE_TYPE_BASE = 24
VERSION = 0
STARTING_TILE_TYPE = 15
APP_URL = "https://www.carcassonnelab.com/"
COLOR_MAP = {
    "000000": "black",
    "0000ff": "blue",
    "008000": "green",
    "ff0000": "red",
    "ffa500": "yellow",
}


class CarcassonneLabReplayError(RuntimeError):
    pass


def normalize_table_id(value: object) -> str:
    table_id = str(value or "").strip()
    if not table_id.isdigit() or table_id.startswith("0"):
        raise ValueError("Invalid BGA table number")
    return table_id


def _integer(value: object, minimum: int, maximum: int) -> int:
    if value is None or value == "":
        raise CarcassonneLabReplayError("Replay contains unsupported tile or move data")
    try:
        numeric_value = float(value)
    except (TypeError, ValueError) as exc:
        raise CarcassonneLabReplayError("Replay contains unsupported tile or move data") from exc
    if not isfinite(numeric_value) or not numeric_value.is_integer():
        raise CarcassonneLabReplayError("Replay contains unsupported tile or move data")
    number = int(numeric_value)
    if number < minimum or number > maximum:
        raise CarcassonneLabReplayError("Replay contains unsupported tile or move data")
    return number


def encode_tile_types(tile_types: list[int]) -> str:
    number_base_10 = 0
    for tile_type in tile_types:
        normalized_type = int(tile_type)
        if normalized_type < 1 or normalized_type > TILE_TYPE_BASE:
            raise CarcassonneLabReplayError(f"Unsupported Carcassonne tile type: {normalized_type}")
        number_base_10 = number_base_10 * TILE_TYPE_BASE + normalized_type - 1

    number_base_8 = format(number_base_10, "o")
    if len(number_base_8) % 2:
        number_base_8 = "0" + number_base_8

    return "".join(
        BASE64_ALPHABET[int(number_base_8[index : index + 2], 8)]
        for index in range(0, len(number_base_8), 2)
    )


def encode_movement(movement: dict[str, int]) -> str:
    row_value = int(movement["row"])
    column_value = int(movement["col"])
    rotation = int(movement["rotation"])
    meeple_position = int(movement["meeple_position"])

    if column_value < 0:
        rotation += 16
    if row_value < 0:
        rotation += 32

    if rotation < 0:
        raise CarcassonneLabReplayError("Replay contains an unsupported tile rotation")
    try:
        row = ALPHABET[abs(row_value)]
        column = ALPHABET[abs(column_value)]
        encoded_rotation = ALPHABET[rotation]
    except IndexError as exc:
        raise CarcassonneLabReplayError("Replay contains a board coordinate outside the supported range") from exc

    return f"{column}{row}{encoded_rotation}{meeple_position}"


def _flatten_replay_events(game_logs: list[dict]) -> list[dict]:
    events: list[dict] = []
    for line in game_logs:
        if not isinstance(line, dict):
            continue
        line_events = line.get("data")
        if not isinstance(line_events, list):
            continue
        events.extend(event for event in line_events if isinstance(event, dict))
    return events


def _flatten_movements(events: list[dict]) -> list[tuple[str, dict]]:
    movements: list[tuple[str, dict]] = []
    for event in events:
        event_type = str(event.get("type") or "")
        if event_type not in {"playTile", "playPartisan"}:
            continue
        args = event.get("args")
        if isinstance(args, list):
            movements.extend((event_type, item) for item in args if isinstance(item, dict))
        elif isinstance(args, dict):
            movements.append((event_type, args))
    return movements


def _player_colors(
    player_ids: list[str],
    events: list[dict],
    archive_players: list[dict],
) -> list[str]:
    colors_by_player_id: dict[str, str] = {}

    def set_color(player_id: object, value: object) -> None:
        raw_color = str(value or "").strip().removeprefix("#").lower()
        color = COLOR_MAP.get(raw_color)
        if player_id is not None and color:
            colors_by_player_id[str(player_id)] = color

    for event in events:
        event_args = event.get("args")
        nested_args = event_args.get("args") if isinstance(event_args, dict) else None
        result = nested_args.get("result") if isinstance(nested_args, dict) else None
        if event.get("type") != "gameStateChange" or not isinstance(result, list):
            continue
        for player in result:
            if not isinstance(player, dict):
                continue
            player_id = player.get("player")
            if player_id is None:
                player_id = player.get("id")
            set_color(player_id, player.get("color"))

    for player in archive_players:
        if isinstance(player, dict):
            set_color(player.get("id"), player.get("color"))

    used_colors = {colors_by_player_id.get(player_id) for player_id in player_ids}
    fallback_colors = [
        color
        for color in ["red", "blue", "green", "yellow", "black"]
        if color not in used_colors
    ]
    colors: list[str] = []
    for player_id in player_ids:
        color = colors_by_player_id.get(player_id)
        if not color:
            color = fallback_colors.pop(0) if fallback_colors else "red"
        colors.append(color)
    return colors


def build_carcassonne_lab_url(
    game_logs: list[dict],
    archive_players: list[dict] | None = None,
) -> str:
    if not isinstance(game_logs, list):
        raise CarcassonneLabReplayError("BGA did not return replay logs")

    events = _flatten_replay_events(game_logs)
    movements = _flatten_movements(events)
    stack_types = [STARTING_TILE_TYPE]
    encoded_movements: list[dict[str, int]] = []
    player_ids: list[str] = []
    player_names: list[str] = []

    for event_type, movement in movements:
        if event_type == "playPartisan":
            if not encoded_movements:
                raise CarcassonneLabReplayError("Replay contains a meeple placement before the first tile")
            encoded_movements[-1]["meeple_position"] = _integer(movement.get("pos"), 0, 9)
            continue

        if movement.get("player_id") is None:
            raise CarcassonneLabReplayError("Replay contains a tile placement with missing player data")
        player_id = str(movement["player_id"])
        if player_id not in player_ids:
            player_ids.append(player_id)
            player_names.append(str(movement.get("player_name") or ""))

        try:
            stack_types.append(_integer(movement.get("type"), 1, TILE_TYPE_BASE))
            encoded_movements.append(
                {
                    "col": _integer(movement.get("x"), -55, 55),
                    "row": _integer(movement.get("y"), -55, 55),
                    "rotation": _integer(movement.get("ori"), 1, 4) - 1,
                    "meeple_position": 0,
                }
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CarcassonneLabReplayError("Replay contains an invalid tile movement") from exc

    if not encoded_movements:
        raise CarcassonneLabReplayError("BGA replay does not contain any tile movements")

    colors = _player_colors(player_ids, events, archive_players or [])
    path = (
        f"{VERSION}/0/{encode_tile_types(stack_types)}/"
        + "".join(encode_movement(movement) for movement in encoded_movements)
    )
    players = ",".join(quote(name, safe="-_.!~*'()") for name in player_names)
    encoded_colors = ",".join(quote(color, safe="-_.!~*'()") for color in colors)
    return f"{APP_URL}#/{path}?players={players}&colors={encoded_colors}"


def _response_error(response: object) -> str:
    if not isinstance(response, dict):
        return "BGA returned an invalid response"
    data = response.get("data")
    data_error = data.get("error") if isinstance(data, dict) else None
    return str(response.get("error") or data_error or "BGA rejected the replay request")


def _response_logs(response: object) -> list[dict] | None:
    if not isinstance(response, dict):
        return None
    data = response.get("data")
    logs = data.get("logs") if isinstance(data, dict) else None
    return logs if isinstance(logs, list) else None


def _response_players(response: object) -> list[dict]:
    if not isinstance(response, dict):
        return []
    data = response.get("data")
    players = data.get("players") if isinstance(data, dict) else None
    return players if isinstance(players, list) else []


def fetch_bga_replay_logs(
    table_id: object,
    *,
    request: Callable[..., dict] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict]:
    logs, _players = fetch_bga_replay(
        table_id,
        request=request,
        sleep=sleep,
    )
    return logs


def fetch_bga_replay(
    table_id: object,
    *,
    request: Callable[..., dict] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[list[dict], list[dict]]:
    normalized_table_id = normalize_table_id(table_id)
    if request is None:
        from .http_session import request_json

        request = request_json

    logs_path = "/archive/archive/logs.html"
    logs_params = {"table": normalized_table_id, "translated": "true"}

    response = request(logs_path, params=logs_params)
    logs = _response_logs(response)
    if logs is not None:
        return logs, _response_players(response)

    error_message = _response_error(response)
    if "Cannot find gamenotifs log file" not in error_message:
        raise CarcassonneLabReplayError(error_message)

    archive_response = request(
        "/gamereview/gamereview/requestTableArchive.html",
        params={"table": normalized_table_id},
    )
    if isinstance(archive_response, dict) and str(archive_response.get("status")) == "0":
        raise CarcassonneLabReplayError(_response_error(archive_response))

    for attempt in range(3):
        response = request(logs_path, params=logs_params)
        logs = _response_logs(response)
        if logs is not None:
            return logs, _response_players(response)
        error_message = _response_error(response)
        if "Cannot find gamenotifs log file" not in error_message:
            break
        if attempt < 2:
            sleep(0.5 * (attempt + 1))

    raise CarcassonneLabReplayError(error_message)


def generate_carcassonne_lab_url(table_id: object) -> str:
    game_logs, archive_players = fetch_bga_replay(table_id)
    return build_carcassonne_lab_url(game_logs, archive_players)
