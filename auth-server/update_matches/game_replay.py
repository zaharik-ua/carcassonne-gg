from __future__ import annotations

import json
import re
import sqlite3
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote


LOGS_PATH = "/archive/archive/logs.html"
ARCHIVE_REQUEST_PATH = "/gamereview/gamereview/requestTableArchive.html"
MISSING_ARCHIVE_MESSAGE = "Cannot find gamenotifs log file"
CARCASSONNE_LAB_URL = "https://www.carcassonnelab.com/"
CARCASSONNE_LAB_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
CARCASSONNE_LAB_BASE64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
CARCASSONNE_LAB_TILE_TYPE_BASE = 24
CARCASSONNE_LAB_STARTING_TILE_TYPE = 15
MEEPLE_COLOR_NAMES = {
    "000000": "black",
    "0000ff": "blue",
    "008000": "green",
    "ff0000": "red",
    "ffa500": "yellow",
}
SCORE_FEATURE_ALIASES = {
    "fields": ("field", "fields", "farm", "farms", "farmer", "farmers"),
    "cities": ("city", "cities", "town", "towns"),
    "roads": ("road", "roads"),
    "monasteries": ("monastery", "monasteries", "cloister", "cloisters", "abbey", "abbeys"),
}
LEGACY_GAME_REPLAY_COLUMNS = (
    "logs_json",
    "event_count",
    "tile_count",
    "meeple_count",
    "archive_requested",
)

RequestJson = Callable[..., dict[str, Any]]
Authenticate = Callable[[], Any]
Sleep = Callable[[float], None]


class GameReplayError(RuntimeError):
    """Base error for the manual BGA replay fetch."""


class GameNotFoundError(GameReplayError):
    pass


class BgaReplayError(GameReplayError):
    pass


def ensure_game_replays_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS game_replays (
          game_id TEXT PRIMARY KEY,
          bga_table_id TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          events_json TEXT,
          players_json TEXT,
          carcassonne_lab_url TEXT,
          board_stats_json TEXT,
          meeple_stats_json TEXT,
          scoring_json TEXT,
          player_time_json TEXT,
          fetched_at TEXT,
          last_attempt_at TEXT,
          last_error TEXT,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (game_id) REFERENCES games(id)
            ON UPDATE CASCADE ON DELETE CASCADE
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_game_replays_bga_table_id
          ON game_replays(bga_table_id);
        """
    )
    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(game_replays)").fetchall()
    }
    text_columns = (
        "carcassonne_lab_url",
        "board_stats_json",
        "meeple_stats_json",
        "scoring_json",
        "player_time_json",
    )
    for column in text_columns:
        if column not in columns:
            conn.execute(f"ALTER TABLE game_replays ADD COLUMN {column} TEXT")
    for column in LEGACY_GAME_REPLAY_COLUMNS:
        if column in columns:
            conn.execute(f"ALTER TABLE game_replays DROP COLUMN {column}")


def fetch_and_store_game_replay(
    db_path: str | Path,
    game_id: str,
    *,
    force: bool = False,
    poll_attempts: int = 10,
    poll_delay: float = 1.0,
    request: RequestJson | None = None,
    authenticate: Authenticate | None = None,
    sleep: Sleep = time.sleep,
) -> dict[str, Any]:
    normalized_game_id = str(game_id or "").strip()
    if not normalized_game_id:
        raise GameNotFoundError("games.id must not be empty")
    if poll_attempts < 1:
        raise ValueError("poll_attempts must be at least 1")
    if poll_delay < 0:
        raise ValueError("poll_delay must not be negative")

    path = Path(db_path).expanduser()
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        ensure_game_replays_schema(conn)
        game = conn.execute(
            """
            SELECT id, bga_table_id
            FROM games
            WHERE id = ? AND deleted_at IS NULL
            """,
            (normalized_game_id,),
        ).fetchone()
        if game is None:
            raise GameNotFoundError(f"Game not found: games.id={normalized_game_id}")

        table_id = str(game["bga_table_id"] or "").strip()
        if not table_id:
            raise GameReplayError(f"Game {normalized_game_id} has no bga_table_id")
        if not table_id.isdigit():
            raise GameReplayError(
                f"Game {normalized_game_id} has invalid bga_table_id={table_id!r}"
            )

        cached = conn.execute(
            """
            SELECT game_id, bga_table_id, status, fetched_at, players_json,
                   carcassonne_lab_url, board_stats_json, meeple_stats_json,
                   scoring_json, player_time_json, events_json
            FROM game_replays
            WHERE game_id = ?
            """,
            (normalized_game_id,),
        ).fetchone()
        if cached is not None and cached["status"] == "ready" and not force:
            stored_board_stats = _from_json(cached["board_stats_json"], {})
            has_compact_board_stats = (
                isinstance(stored_board_stats, dict)
                and set(stored_board_stats) == {"width", "height"}
            )
            if has_compact_board_stats:
                return _summary_from_row(cached, cached=True)

            stored_events = _from_json(cached["events_json"], [])
            if isinstance(stored_events, list):
                board_stats_json = _to_json(build_board_stats(stored_events))
                conn.execute(
                    """
                    UPDATE game_replays
                    SET board_stats_json = ?, updated_at = ?
                    WHERE game_id = ?
                    """,
                    (
                        board_stats_json,
                        _utc_now(),
                        normalized_game_id,
                    ),
                )
                conn.commit()
                cached_values = dict(cached)
                cached_values["board_stats_json"] = board_stats_json
                return _summary_from_row(cached_values, cached=True)
            return _summary_from_row(cached, cached=True)

        attempted_at = _utc_now()
        conn.execute(
            """
            INSERT INTO game_replays (
              game_id, bga_table_id, status, last_attempt_at, last_error, updated_at
            ) VALUES (?, ?, 'fetching', ?, NULL, ?)
            ON CONFLICT(game_id) DO UPDATE SET
              bga_table_id = excluded.bga_table_id,
              status = 'fetching',
              last_attempt_at = excluded.last_attempt_at,
              last_error = NULL,
              updated_at = excluded.updated_at
            """,
            (normalized_game_id, table_id, attempted_at, attempted_at),
        )
        conn.commit()

    if request is None or authenticate is None:
        from .http_session import get_http_session, request_json

        if request is None:
            request = request_json
        if authenticate is None:
            authenticate = get_http_session

    try:
        authenticate()
        logs, response_players = fetch_bga_replay(
            table_id,
            request=request,
            poll_attempts=poll_attempts,
            poll_delay=poll_delay,
            sleep=sleep,
        )
        events = normalize_replay_events(logs)
        players = normalize_replay_players(logs, response_players, events)
        _add_player_colors_to_events(events, players)
        carcassonne_lab_url = build_carcassonne_lab_url(events, players)
        board_stats = build_board_stats(events)
        meeple_stats = build_meeple_stats(events, players, logs)
        scoring = build_scoring_stats(logs, players)
        player_time = build_player_time_stats(logs, players)
        fetched_at = _utc_now()

        with sqlite3.connect(path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(
                """
                UPDATE game_replays
                SET status = 'ready',
                    events_json = ?,
                    players_json = ?,
                    carcassonne_lab_url = ?,
                    board_stats_json = ?,
                    meeple_stats_json = ?,
                    scoring_json = ?,
                    player_time_json = ?,
                    fetched_at = ?,
                    last_error = NULL,
                    updated_at = ?
                WHERE game_id = ?
                """,
                (
                    _to_json(events),
                    _to_json(players),
                    carcassonne_lab_url,
                    _to_json(board_stats),
                    _to_json(meeple_stats),
                    _to_json(scoring),
                    _to_json(player_time),
                    fetched_at,
                    fetched_at,
                    normalized_game_id,
                ),
            )
            conn.commit()

        return {
            "game_id": normalized_game_id,
            "bga_table_id": table_id,
            "status": "ready",
            "players": players,
            "carcassonne_lab_url": carcassonne_lab_url,
            "board_stats": board_stats,
            "meeple_stats": meeple_stats,
            "scoring": scoring,
            "player_time": player_time,
            "fetched_at": fetched_at,
            "cached": False,
        }
    except Exception as exc:
        error_message = str(exc) or exc.__class__.__name__
        failed_at = _utc_now()
        with sqlite3.connect(path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(
                """
                UPDATE game_replays
                SET status = 'error',
                    last_error = ?,
                    updated_at = ?
                WHERE game_id = ?
                """,
                (error_message, failed_at, normalized_game_id),
            )
            conn.commit()
        if isinstance(exc, GameReplayError):
            raise
        raise GameReplayError(error_message) from exc


def fetch_and_store_game_replay_with_account_rotation(
    db_path: str | Path,
    game_id: str,
    *,
    force: bool = False,
    poll_attempts: int = 10,
    poll_delay: float = 1.0,
    sleep: Sleep = time.sleep,
    max_account_attempts: int | None = None,
    replay_fetcher: Callable[..., dict[str, Any]] | None = None,
    account_rotator: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Fetch a replay and rotate through configured BGA accounts on failure."""
    if replay_fetcher is None:
        replay_fetcher = fetch_and_store_game_replay
    if account_rotator is None:
        from .http_session import rotate_http_session

        account_rotator = rotate_http_session
    if max_account_attempts is None:
        from .bga_login import get_bga_credentials

        max_account_attempts = len(get_bga_credentials())
    attempts = max_account_attempts
    attempts = max(1, int(attempts or 1))

    for attempt in range(attempts):
        try:
            return replay_fetcher(
                db_path,
                game_id,
                force=force or attempt > 0,
                poll_attempts=poll_attempts,
                poll_delay=poll_delay,
                sleep=sleep,
            )
        except GameReplayError as replay_error:
            if attempt + 1 >= attempts:
                raise
            try:
                account_rotator(
                    reason=f"game_replay_retry_{attempt + 2}_of_{attempts}"
                )
            except Exception as rotation_error:
                raise GameReplayError(
                    f"{replay_error}; failed to switch BGA account: {rotation_error}"
                ) from rotation_error

    raise GameReplayError(f"Failed to fetch replay for game {game_id}")


def fetch_bga_replay(
    table_id: str,
    *,
    request: RequestJson,
    poll_attempts: int = 10,
    poll_delay: float = 1.0,
    sleep: Sleep = time.sleep,
) -> tuple[list[Any], Any]:
    params = {"table": table_id, "translated": "true"}
    payload = request(LOGS_PATH, params=params)
    logs = _extract_logs(payload)
    if logs is not None:
        return logs, _extract_players(payload)

    error_message = _response_error(payload)
    if MISSING_ARCHIVE_MESSAGE not in error_message:
        raise BgaReplayError(error_message or "BGA did not return replay logs")

    archive_payload = request(ARCHIVE_REQUEST_PATH, params={"table": table_id})
    archive_error = _response_error(archive_payload)
    if _is_failed_response(archive_payload) and MISSING_ARCHIVE_MESSAGE not in archive_error:
        raise BgaReplayError(archive_error or "BGA rejected the archive request")

    last_error = error_message
    for attempt in range(poll_attempts):
        if attempt > 0 and poll_delay:
            sleep(poll_delay)
        payload = request(LOGS_PATH, params=params)
        logs = _extract_logs(payload)
        if logs is not None:
            return logs, _extract_players(payload)
        last_error = _response_error(payload) or "BGA did not return replay logs yet"

    raise BgaReplayError(
        f"Replay archive was requested but did not become available after "
        f"{poll_attempts} attempts: {last_error}"
    )


def normalize_replay_events(logs: list[Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    last_tile_seq: int | None = None
    last_player_id: str | None = None
    last_player_name: str | None = None
    active_player_id: str | None = None

    for raw_event in _iter_log_events(logs):
        event_type = str(raw_event.get("type") or "")
        if event_type == "newActivePlayer":
            active_player_id = _event_player_id(raw_event) or active_player_id
            continue
        if event_type == "gameStateChange":
            for args in _event_args(raw_event.get("args")):
                if str(args.get("type") or "") == "activeplayer":
                    active_player_id = (
                        _optional_text(args.get("active_player"))
                        or active_player_id
                    )
            continue
        if event_type not in {"pickTile", "playTile", "playPartisan"}:
            continue

        for args in _event_args(raw_event.get("args")):
            if event_type == "pickTile":
                events.append(
                    {
                        "seq": len(events) + 1,
                        "type": "pickTile",
                        "player_id": (
                            _optional_text(args.get("player_id"))
                            or active_player_id
                        ),
                        "tile_id": _optional_int(args.get("id")),
                        "tile_type": _optional_int(args.get("type")),
                    }
                )
                continue

            piece = str(args.get("piece") or "")
            is_meeple = event_type == "playPartisan" or piece == "partisan"
            if is_meeple:
                player_id = _optional_text(args.get("player_id")) or last_player_id
                player_name = _optional_text(args.get("player_name")) or last_player_name
                normalized = {
                    "seq": len(events) + 1,
                    "type": "playPartisan",
                    "player_id": player_id,
                    "player_name": player_name,
                    "position": _optional_int(args.get("pos")),
                    "tile_event_seq": last_tile_seq,
                }
                events.append(normalized)
                continue

            player_id = _optional_text(args.get("player_id")) or active_player_id
            player_name = _optional_text(args.get("player_name"))
            orientation_raw = _optional_int(args.get("ori"))
            normalized = {
                "seq": len(events) + 1,
                "type": "playTile",
                "player_id": player_id,
                "player_name": player_name,
                "tile_type": _optional_int(args.get("type")),
                "x": _optional_int(args.get("x")),
                "y": _optional_int(args.get("y")),
                "orientation": orientation_raw,
                "rotation": orientation_raw - 1 if orientation_raw is not None else None,
            }
            events.append(normalized)
            last_tile_seq = normalized["seq"]
            last_player_id = player_id
            last_player_name = player_name

    return events


def build_board_stats(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Return final board dimensions, including the starting tile at (0, 0)."""
    bounds = {"min_x": 0, "max_x": 0, "min_y": 0, "max_y": 0}

    for event in events:
        if str(event.get("type") or "") != "playTile":
            continue
        x = _optional_int(event.get("x"))
        y = _optional_int(event.get("y"))
        if x is None or y is None:
            continue
        bounds["min_x"] = min(bounds["min_x"], x)
        bounds["max_x"] = max(bounds["max_x"], x)
        bounds["min_y"] = min(bounds["min_y"], y)
        bounds["max_y"] = max(bounds["max_y"], y)

    return {
        "width": bounds["max_x"] - bounds["min_x"] + 1,
        "height": bounds["max_y"] - bounds["min_y"] + 1,
    }


def build_meeple_stats(
    events: list[dict[str, Any]],
    players: list[dict[str, Any]],
    logs: list[Any] | None = None,
) -> dict[str, Any]:
    placements_by_player: dict[str, int] = {}
    returns_by_player: dict[str, int] = {}
    for event in events:
        if str(event.get("type") or "") != "playPartisan":
            continue
        player_id = _optional_text(event.get("player_id"))
        if player_id:
            placements_by_player[player_id] = placements_by_player.get(player_id, 0) + 1

    for raw_event in _iter_log_events(logs or []):
        if str(raw_event.get("type") or "") != "realizationAchieved":
            continue
        for args in _event_args(raw_event.get("args")):
            recovered = args.get("part_to_recover")
            if not isinstance(recovered, dict):
                continue
            for raw_player_id, pieces in recovered.items():
                player_id = _optional_text(raw_player_id)
                if not player_id or not isinstance(pieces, dict):
                    continue
                returned = sum(
                    max(0, _optional_int(count) or 0)
                    for count in pieces.values()
                )
                returns_by_player[player_id] = (
                    returns_by_player.get(player_id, 0) + returned
                )

    player_rows = []
    known_player_ids: set[str] = set()
    for player in players:
        player_id = _optional_text(player.get("player_id"))
        if not player_id:
            continue
        known_player_ids.add(player_id)
        player_rows.append(
            {
                "player_id": player_id,
                "player_name": _optional_text(player.get("player_name")),
                "color_hex": _optional_text(player.get("color_hex")),
                "meeple_color": _optional_text(player.get("meeple_color")),
                "placements": placements_by_player.get(player_id, 0),
                "returns": returns_by_player.get(player_id, 0),
                "remaining_on_board_at_end": max(
                    0,
                    placements_by_player.get(player_id, 0)
                    - returns_by_player.get(player_id, 0),
                ),
            }
        )
    for player_id in placements_by_player.keys() | returns_by_player.keys():
        if player_id in known_player_ids:
            continue
        placements = placements_by_player.get(player_id, 0)
        player_rows.append(
            {
                "player_id": player_id,
                "player_name": None,
                "color_hex": None,
                "meeple_color": None,
                "placements": placements,
                "returns": returns_by_player.get(player_id, 0),
                "remaining_on_board_at_end": max(
                    0,
                    placements - returns_by_player.get(player_id, 0),
                ),
            }
        )

    total_placements = sum(placements_by_player.values())
    total_returns = sum(returns_by_player.values())
    return {
        "total_placements": total_placements,
        "total_returns": total_returns,
        "remaining_on_board_at_end": max(0, total_placements - total_returns),
        "players": player_rows,
    }


def build_scoring_stats(
    logs: list[Any],
    players: list[dict[str, Any]],
) -> dict[str, Any]:
    player_details = {
        str(player.get("player_id") or ""): player
        for player in players
        if player.get("player_id")
    }
    scoring_events: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    event_records = list(_iter_log_event_records(logs))
    has_realization_events = any(
        str(raw_event.get("type") or "") == "realizationAchieved"
        for _raw_seq, _line, raw_event in event_records
    )

    for raw_seq, line, raw_event in event_records:
        event_type = str(raw_event.get("type") or "")
        # A realizationAchieved event is immediately followed by winPoints in
        # current BGA logs. The latter is a UI notification for the same score.
        if has_realization_events and event_type == "winPoints":
            continue
        occurred_at = _event_occurred_at(line, raw_event)
        nested_args = list(_nested_event_args(raw_event.get("args")))
        event_feature = next(
            (
                feature
                for args in nested_args
                if (feature := _score_feature(event_type, raw_event.get("log"), args))
            ),
            None,
        )
        event_player_ids = _event_player_ids(raw_event)
        for args in nested_args:
            feature = _score_feature(event_type, raw_event.get("log"), args) or event_feature
            points = _score_points(event_type, args)
            player_ids = _player_ids_from_args(args) or event_player_ids
            if not feature or points is None or not player_ids:
                continue
            normalized_points = _compact_number(points)
            for player_id in player_ids:
                signature = (raw_seq, player_id, feature, normalized_points)
                if signature in seen:
                    continue
                seen.add(signature)
                details = player_details.get(player_id, {})
                scoring_events.append(
                    {
                        "seq": len(scoring_events) + 1,
                        "raw_event_seq": raw_seq,
                        "raw_type": event_type,
                        "occurred_at": occurred_at,
                        "player_id": player_id,
                        "player_name": (
                            _optional_text(args.get("player_name"))
                            or _optional_text(details.get("player_name"))
                        ),
                        "feature": feature,
                        "points": normalized_points,
                        "realization_id": _optional_text(args.get("real_id")),
                        "phase": _scoring_phase(event_type, raw_event.get("log")),
                    }
                )

    empty_totals = {feature: 0 for feature in SCORE_FEATURE_ALIASES}
    totals = dict(empty_totals)
    by_player: dict[str, dict[str, Any]] = {}
    for player_id, details in player_details.items():
        by_player[player_id] = {
            "player_id": player_id,
            "player_name": _optional_text(details.get("player_name")),
            "total_points": 0,
            "points": dict(empty_totals),
        }
    for event in scoring_events:
        player_id = str(event["player_id"])
        player = by_player.setdefault(
            player_id,
            {
                "player_id": player_id,
                "player_name": event.get("player_name"),
                "total_points": 0,
                "points": dict(empty_totals),
            },
        )
        feature = str(event["feature"])
        points = event["points"]
        totals[feature] = _compact_number(float(totals[feature]) + float(points))
        player["points"][feature] = _compact_number(
            float(player["points"][feature]) + float(points)
        )
        player["total_points"] = _compact_number(
            float(player["total_points"]) + float(points)
        )

    return {
        "totals": totals,
        "players": list(by_player.values()),
        "events": scoring_events,
    }


def build_player_time_stats(
    logs: list[Any],
    players: list[dict[str, Any]],
) -> dict[str, Any]:
    timestamped_events: list[tuple[float, dict[str, Any]]] = []
    for _raw_seq, line, raw_event in _iter_log_event_records(logs):
        timestamp = _event_unix_timestamp(line, raw_event)
        if timestamp is not None:
            timestamped_events.append((timestamp, raw_event))

    player_details = {
        str(player.get("player_id") or ""): player
        for player in players
        if player.get("player_id")
    }
    durations: dict[str, float] = {player_id: 0.0 for player_id in player_details}
    turn_counts: dict[str, int] = {player_id: 0 for player_id in player_details}
    source: str | None = None

    if timestamped_events:
        active_player_id: str | None = None
        active_since: float | None = None
        found_active_player_event = False
        for timestamp, raw_event in timestamped_events:
            if str(raw_event.get("type") or "") != "newActivePlayer":
                continue
            player_id = _event_player_id(raw_event)
            if not player_id:
                continue
            found_active_player_event = True
            if active_player_id and active_since is not None and timestamp >= active_since:
                durations[active_player_id] = durations.get(active_player_id, 0.0) + (timestamp - active_since)
                turn_counts[active_player_id] = turn_counts.get(active_player_id, 0) + 1
            active_player_id = player_id
            active_since = timestamp

        if found_active_player_event:
            source = "newActivePlayer.time"
            final_timestamp = max(timestamp for timestamp, _event in timestamped_events)
            if active_player_id and active_since is not None and final_timestamp >= active_since:
                durations[active_player_id] = durations.get(active_player_id, 0.0) + (final_timestamp - active_since)
                turn_counts[active_player_id] = turn_counts.get(active_player_id, 0) + 1
        else:
            active_player_events: list[tuple[float, str]] = []
            for timestamp, raw_event in timestamped_events:
                if str(raw_event.get("type") or "") != "gameStateChange":
                    continue
                for args in _event_args(raw_event.get("args")):
                    if str(args.get("type") or "") != "activeplayer":
                        continue
                    player_id = _optional_text(args.get("active_player"))
                    if player_id and player_id != "0":
                        active_player_events.append((timestamp, player_id))

            if active_player_events:
                source = "gameStateChange.active_player/time"
                active_since, active_player_id = active_player_events[0]
                for timestamp, player_id in active_player_events[1:]:
                    if player_id == active_player_id:
                        continue
                    if timestamp >= active_since:
                        durations[active_player_id] = (
                            durations.get(active_player_id, 0.0)
                            + timestamp
                            - active_since
                        )
                        turn_counts[active_player_id] = (
                            turn_counts.get(active_player_id, 0) + 1
                        )
                    active_since = timestamp
                    active_player_id = player_id
                final_timestamp = max(
                    timestamp for timestamp, _event in timestamped_events
                )
                if final_timestamp >= active_since:
                    durations[active_player_id] = (
                        durations.get(active_player_id, 0.0)
                        + final_timestamp
                        - active_since
                    )
                    turn_counts[active_player_id] = (
                        turn_counts.get(active_player_id, 0) + 1
                    )

        if source is None:
            previous_timestamp = timestamped_events[0][0]
            last_tile_player_id: str | None = None
            last_tile_timestamp: float | None = None
            for timestamp, raw_event in timestamped_events:
                if str(raw_event.get("type") or "") != "playTile":
                    continue
                player_id = _event_player_id(raw_event)
                if not player_id or timestamp < previous_timestamp:
                    continue
                durations[player_id] = durations.get(player_id, 0.0) + (timestamp - previous_timestamp)
                turn_counts[player_id] = turn_counts.get(player_id, 0) + 1
                previous_timestamp = timestamp
                last_tile_player_id = player_id
                last_tile_timestamp = timestamp
            if last_tile_player_id and last_tile_timestamp is not None:
                source = "playTile.time"
                final_timestamp = max(timestamp for timestamp, _event in timestamped_events)
                if final_timestamp >= last_tile_timestamp:
                    durations[last_tile_player_id] = durations.get(last_tile_player_id, 0.0) + (
                        final_timestamp - last_tile_timestamp
                    )

    ordered_player_ids = list(player_details)
    ordered_player_ids.extend(
        player_id for player_id in durations if player_id not in player_details
    )
    player_rows = []
    for player_id in ordered_player_ids:
        details = player_details.get(player_id, {})
        duration = durations.get(player_id, 0.0)
        turn_count = turn_counts.get(player_id, 0)
        player_rows.append(
            {
                "player_id": player_id,
                "player_name": _optional_text(details.get("player_name")),
                "duration_seconds": _compact_number(duration) if source else None,
                "turn_count": turn_count,
                "average_turn_seconds": (
                    _compact_number(duration / turn_count)
                    if source and turn_count
                    else None
                ),
            }
        )

    started_at = min((timestamp for timestamp, _event in timestamped_events), default=None)
    ended_at = max((timestamp for timestamp, _event in timestamped_events), default=None)
    game_duration = (
        max(0.0, ended_at - started_at)
        if started_at is not None and ended_at is not None
        else None
    )
    attributed_duration = sum(durations.values()) if source else None
    return {
        "source": source,
        "game_started_at": _unix_timestamp_to_iso(started_at),
        "game_ended_at": _unix_timestamp_to_iso(ended_at),
        "game_duration_seconds": _compact_number(game_duration) if game_duration is not None else None,
        "attributed_duration_seconds": (
            _compact_number(attributed_duration)
            if attributed_duration is not None
            else None
        ),
        "unattributed_duration_seconds": (
            _compact_number(max(0.0, game_duration - attributed_duration))
            if game_duration is not None and attributed_duration is not None
            else None
        ),
        "players": player_rows,
    }


def normalize_replay_players(
    logs: list[Any],
    response_players: Any,
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    players_by_id: dict[str, dict[str, Any]] = {}

    def add_player(player_id: Any, player_name: Any = None, color: Any = None) -> None:
        normalized_id = _optional_text(player_id)
        if not normalized_id:
            return
        player = players_by_id.setdefault(
            normalized_id,
            {
                "player_id": normalized_id,
                "player_name": None,
                "color_hex": None,
                "meeple_color": None,
            },
        )
        normalized_name = _optional_text(player_name)
        if normalized_name:
            player["player_name"] = normalized_name
        color_hex = _normalize_color_hex(color)
        if color_hex:
            player["color_hex"] = color_hex
            player["meeple_color"] = MEEPLE_COLOR_NAMES.get(color_hex)

    if isinstance(response_players, dict):
        for fallback_id, raw_player in response_players.items():
            if isinstance(raw_player, dict):
                add_player(
                    _first_value(raw_player, "player_id", "player", "id") or fallback_id,
                    _first_value(raw_player, "player_name", "name"),
                    _first_value(raw_player, "color_hex", "color", "player_color"),
                )
            else:
                add_player(fallback_id, raw_player)
    elif isinstance(response_players, list):
        for raw_player in response_players:
            if not isinstance(raw_player, dict):
                continue
            add_player(
                _first_value(raw_player, "player_id", "player", "id"),
                _first_value(raw_player, "player_name", "name"),
                _first_value(raw_player, "color_hex", "color", "player_color"),
            )

    for event in events:
        add_player(event.get("player_id"), event.get("player_name"))

    for raw_event in _iter_log_events(logs):
        if str(raw_event.get("type") or "") != "gameStateChange":
            continue
        for state_player in _game_state_players(raw_event.get("args")):
            add_player(
                _first_value(state_player, "player_id", "player", "id"),
                _first_value(state_player, "player_name", "name"),
                _first_value(state_player, "color", "player_color"),
            )

    return list(players_by_id.values())


def build_carcassonne_lab_url(
    events: list[dict[str, Any]],
    players: list[dict[str, Any]],
) -> str | None:
    player_details = {
        str(player.get("player_id") or ""): player
        for player in players
        if player.get("player_id")
    }
    player_ids: list[str] = []
    player_names: list[str] = []
    player_colors: list[str] = []
    tile_types = [CARCASSONNE_LAB_STARTING_TILE_TYPE]
    movements: list[dict[str, int]] = []

    for event in events:
        event_type = str(event.get("type") or "")
        if event_type == "playPartisan":
            if not movements:
                continue
            position = _optional_int(event.get("position"))
            if position is not None:
                movements[-1]["meeple_position"] = position
            continue
        if event_type != "playTile":
            continue

        tile_type = _optional_int(event.get("tile_type"))
        col = _optional_int(event.get("x"))
        row = _optional_int(event.get("y"))
        rotation = _optional_int(event.get("rotation"))
        player_id = _optional_text(event.get("player_id"))
        if tile_type is None or col is None or row is None or rotation is None or not player_id:
            return None
        if not 1 <= tile_type <= CARCASSONNE_LAB_TILE_TYPE_BASE:
            return None

        if player_id not in player_ids:
            details = player_details.get(player_id, {})
            player_name = (
                _optional_text(event.get("player_name"))
                or _optional_text(details.get("player_name"))
                or player_id
            )
            player_color = _optional_text(details.get("meeple_color"))
            if not player_color:
                return None
            player_ids.append(player_id)
            player_names.append(player_name)
            player_colors.append(player_color)

        tile_types.append(tile_type)
        movements.append(
            {
                "col": col,
                "row": row,
                "rotation": rotation,
                "meeple_position": 0,
            }
        )

    if not movements or not player_ids:
        return None

    try:
        encoded_tiles = _encode_carcassonne_lab_tile_types(tile_types)
        encoded_movements = "".join(
            _encode_carcassonne_lab_movement(movement)
            for movement in movements
        )
    except (IndexError, ValueError):
        return None

    encoded_players = ",".join(_encode_uri_component(name) for name in player_names)
    encoded_colors = ",".join(_encode_uri_component(color) for color in player_colors)
    return (
        f"{CARCASSONNE_LAB_URL}#/0/0/{encoded_tiles}/{encoded_movements}"
        f"?players={encoded_players}&colors={encoded_colors}"
    )


def _encode_carcassonne_lab_tile_types(tile_types: list[int]) -> str:
    decimal_value = 0
    for tile_type in tile_types:
        decimal_value = (
            decimal_value * CARCASSONNE_LAB_TILE_TYPE_BASE
            + tile_type
            - 1
        )
    octal_value = format(decimal_value, "o")
    if len(octal_value) % 2:
        octal_value = f"0{octal_value}"
    return "".join(
        CARCASSONNE_LAB_BASE64_ALPHABET[int(octal_value[index:index + 2], 8)]
        for index in range(0, len(octal_value), 2)
    )


def _encode_carcassonne_lab_movement(movement: dict[str, int]) -> str:
    col = movement["col"]
    row = movement["row"]
    rotation = movement["rotation"]
    if col < 0:
        rotation += 16
    if row < 0:
        rotation += 32
    if rotation < 0:
        raise ValueError("CarcassonneLab rotation must not be negative")
    return (
        f"{CARCASSONNE_LAB_ALPHABET[abs(col)]}"
        f"{CARCASSONNE_LAB_ALPHABET[abs(row)]}"
        f"{CARCASSONNE_LAB_ALPHABET[rotation]}"
        f"{movement['meeple_position']}"
    )


def _encode_uri_component(value: str) -> str:
    return quote(value, safe="~()*!.'-_")


def _add_player_colors_to_events(
    events: list[dict[str, Any]],
    players: list[dict[str, Any]],
) -> None:
    colors_by_id = {
        str(player["player_id"]): player
        for player in players
        if player.get("player_id") and player.get("color_hex")
    }
    for event in events:
        player = colors_by_id.get(str(event.get("player_id") or ""))
        if not player:
            continue
        event["color_hex"] = player["color_hex"]
        event["meeple_color"] = player["meeple_color"]


def _game_state_players(raw_args: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for outer_args in _event_args(raw_args):
        result_candidates = [outer_args.get("result")]
        nested_args = outer_args.get("args")
        if isinstance(nested_args, dict):
            result_candidates.append(nested_args.get("result"))

        for result in result_candidates:
            if isinstance(result, list):
                found.extend(item for item in result if isinstance(item, dict))
            elif isinstance(result, dict):
                for fallback_id, item in result.items():
                    if not isinstance(item, dict):
                        continue
                    if not _first_value(item, "player_id", "player", "id"):
                        item = {**item, "player_id": fallback_id}
                    found.append(item)
    return found


def _iter_log_events(logs: list[Any]):
    for _sequence, _line, event in _iter_log_event_records(logs):
        yield event


def _iter_log_event_records(logs: list[Any]):
    sequence = 0
    for line in logs:
        if not isinstance(line, dict):
            continue
        data = line.get("data")
        if isinstance(data, list):
            for event in data:
                if isinstance(event, dict):
                    sequence += 1
                    yield sequence, line, event
        elif isinstance(data, dict):
            sequence += 1
            yield sequence, line, data


def _event_args(raw_args: Any) -> list[dict[str, Any]]:
    if isinstance(raw_args, dict):
        return [raw_args]
    if isinstance(raw_args, list):
        return [item for item in raw_args if isinstance(item, dict)]
    return []


def _nested_event_args(raw_args: Any):
    seen: set[int] = set()

    def walk(value: Any):
        if isinstance(value, dict):
            object_id = id(value)
            if object_id in seen:
                return
            seen.add(object_id)
            yield value
            for nested in value.values():
                yield from walk(nested)
        elif isinstance(value, list):
            for nested in value:
                yield from walk(nested)

    yield from walk(raw_args)


def _score_feature(event_type: str, log_text: Any, args: dict[str, Any]) -> str | None:
    event_type_text = str(event_type or "").lower()
    candidate_values = [str(log_text or "")]
    feature_keys = {
        "feature",
        "feature_type",
        "featuretype",
        "scoring_type",
        "scoringtype",
        "terrain",
        "terrain_type",
        "terraintype",
        "category",
        "structure",
        "place",
        "zone_type",
        "zonetype",
        "partisan_type",
        "partisantype",
        "real_type",
        "realtype",
        "type",
    }
    for key, value in args.items():
        if str(key).lower() in feature_keys and isinstance(value, (str, int, float)):
            candidate_values.append(str(value))
    candidate_text = " ".join(candidate_values).lower()

    for feature, aliases in SCORE_FEATURE_ALIASES.items():
        for alias in aliases:
            if alias in event_type_text or re.search(rf"\b{re.escape(alias)}\b", candidate_text):
                return feature
    return None


def _score_points(event_type: str, args: dict[str, Any]) -> float | None:
    explicit_keys = (
        "points",
        "point",
        "score_delta",
        "scoreDelta",
        "score_gain",
        "scoreGain",
        "delta_score",
        "deltaScore",
        "points_number",
        "pointsNumber",
    )
    for key in explicit_keys:
        number = _optional_number(args.get(key))
        if number is not None:
            return number
    tile_values = args.get("tile_to_value")
    if isinstance(tile_values, dict):
        values = [
            number
            for value in tile_values.values()
            if (number := _optional_number(value)) is not None
        ]
        if values:
            return sum(values)
    if "score" in str(event_type or "").lower():
        return _optional_number(args.get("score"))
    return None


def _player_ids_from_args(args: dict[str, Any]) -> list[str]:
    player_ids: list[str] = []

    def add(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                add(item)
            return
        if isinstance(value, dict):
            direct_id = _first_value(value, "player_id", "playerId", "player", "id")
            if direct_id is not None:
                add(direct_id)
                return
            for key in value:
                add(key)
            return
        player_id = _optional_text(value)
        if player_id and player_id != "0" and player_id not in player_ids:
            player_ids.append(player_id)

    winners = args.get("winners")
    if winners is not None:
        add(winners)
    if not player_ids:
        add(
            _first_value(
                args,
                "player_id",
                "playerId",
                "player",
                "active_player_id",
                "activePlayerId",
                "active_player",
            )
        )
    return player_ids


def _event_player_ids(raw_event: dict[str, Any]) -> list[str]:
    player_ids: list[str] = []
    for args in _nested_event_args(raw_event.get("args")):
        for player_id in _player_ids_from_args(args):
            if player_id not in player_ids:
                player_ids.append(player_id)
    return player_ids


def _scoring_phase(event_type: str, log_text: Any) -> str | None:
    if str(event_type or "") != "realizationAchieved":
        return None
    text = str(log_text or "").lower()
    if "unachieved" in text or "field" in text:
        return "end_game"
    if "achieved" in text:
        return "completed"
    return "unknown"


def _event_player_id(raw_event: dict[str, Any]) -> str | None:
    for args in _nested_event_args(raw_event.get("args")):
        player_id = _optional_text(
            _first_value(
                args,
                "player_id",
                "playerId",
                "player",
                "active_player_id",
                "activePlayerId",
                "active_player",
            )
        )
        if player_id:
            return player_id
    return None


def _event_unix_timestamp(
    line: dict[str, Any],
    raw_event: dict[str, Any],
) -> float | None:
    for source in (raw_event, line):
        for key in ("time", "timestamp", "created_at", "date"):
            timestamp = _parse_unix_timestamp(source.get(key))
            if timestamp is not None:
                return timestamp
    return None


def _event_occurred_at(
    line: dict[str, Any],
    raw_event: dict[str, Any],
) -> str | None:
    return _unix_timestamp_to_iso(_event_unix_timestamp(line, raw_event))


def _parse_unix_timestamp(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        timestamp = float(raw)
        if timestamp >= 1_000_000_000_000:
            timestamp /= 1000.0
        return timestamp if timestamp >= 1_000_000_000 else None
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except (OverflowError, ValueError):
        return None


def _unix_timestamp_to_iso(value: float | None) -> str | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return None


def _extract_logs(payload: Any) -> list[Any] | None:
    if not isinstance(payload, dict) or _is_failed_response(payload):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    logs = data.get("logs")
    return logs if isinstance(logs, list) else None


def _extract_players(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    players = data.get("players")
    return players if isinstance(players, (list, dict)) else []


def _is_failed_response(payload: Any) -> bool:
    return isinstance(payload, dict) and str(payload.get("status")) == "0"


def _response_error(payload: Any) -> str:
    if not isinstance(payload, dict):
        return "BGA returned an invalid JSON response"
    direct = payload.get("error")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    data = payload.get("data")
    if isinstance(data, dict):
        nested = data.get("error")
        if isinstance(nested, str) and nested.strip():
            return nested.strip()
    return ""


def _optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _optional_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _compact_number(value: float) -> int | float:
    return int(value) if float(value).is_integer() else round(float(value), 3)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_value(source: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = source.get(key)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _normalize_color_hex(value: Any) -> str | None:
    color = str(value or "").strip().lower().removeprefix("#")
    if len(color) != 6 or any(character not in "0123456789abcdef" for character in color):
        return None
    return color


def _to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _summary_from_row(row: sqlite3.Row, *, cached: bool) -> dict[str, Any]:
    return {
        "game_id": row["game_id"],
        "bga_table_id": row["bga_table_id"],
        "status": row["status"],
        "players": _from_json(row["players_json"], []),
        "carcassonne_lab_url": row["carcassonne_lab_url"],
        "board_stats": _from_json(row["board_stats_json"], {}),
        "meeple_stats": _from_json(row["meeple_stats_json"], {}),
        "scoring": _from_json(row["scoring_json"], {}),
        "player_time": _from_json(row["player_time_json"], {}),
        "fetched_at": row["fetched_at"],
        "cached": cached,
    }


def _from_json(value: Any, default: Any) -> Any:
    if not isinstance(value, str) or not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default
