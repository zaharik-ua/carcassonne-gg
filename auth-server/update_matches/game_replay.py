from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LOGS_PATH = "/archive/archive/logs.html"
ARCHIVE_REQUEST_PATH = "/gamereview/gamereview/requestTableArchive.html"
MISSING_ARCHIVE_MESSAGE = "Cannot find gamenotifs log file"

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
          logs_json TEXT,
          events_json TEXT,
          players_json TEXT,
          event_count INTEGER NOT NULL DEFAULT 0,
          tile_count INTEGER NOT NULL DEFAULT 0,
          meeple_count INTEGER NOT NULL DEFAULT 0,
          archive_requested INTEGER NOT NULL DEFAULT 0,
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
            SELECT game_id, bga_table_id, status, event_count, tile_count,
                   meeple_count, archive_requested, fetched_at
            FROM game_replays
            WHERE game_id = ?
            """,
            (normalized_game_id,),
        ).fetchone()
        if cached is not None and cached["status"] == "ready" and not force:
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
        from .http_session import refresh_http_session, request_json

        if request is None:
            request = request_json
        if authenticate is None:
            authenticate = lambda: refresh_http_session(
                reason=f"game_replay_{table_id}",
                require_login=True,
            )

    try:
        authenticate()
        logs, players, archive_requested = fetch_bga_replay(
            table_id,
            request=request,
            poll_attempts=poll_attempts,
            poll_delay=poll_delay,
            sleep=sleep,
        )
        events = normalize_replay_events(logs)
        tile_count = sum(event["type"] == "playTile" for event in events)
        meeple_count = sum(event["type"] == "playPartisan" for event in events)
        fetched_at = _utc_now()

        with sqlite3.connect(path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute(
                """
                UPDATE game_replays
                SET status = 'ready',
                    logs_json = ?,
                    events_json = ?,
                    players_json = ?,
                    event_count = ?,
                    tile_count = ?,
                    meeple_count = ?,
                    archive_requested = ?,
                    fetched_at = ?,
                    last_error = NULL,
                    updated_at = ?
                WHERE game_id = ?
                """,
                (
                    _to_json(logs),
                    _to_json(events),
                    _to_json(players),
                    len(events),
                    tile_count,
                    meeple_count,
                    int(archive_requested),
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
            "event_count": len(events),
            "tile_count": tile_count,
            "meeple_count": meeple_count,
            "archive_requested": archive_requested,
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


def fetch_bga_replay(
    table_id: str,
    *,
    request: RequestJson,
    poll_attempts: int = 10,
    poll_delay: float = 1.0,
    sleep: Sleep = time.sleep,
) -> tuple[list[Any], Any, bool]:
    params = {"table": table_id, "translated": "true"}
    payload = request(LOGS_PATH, params=params)
    logs = _extract_logs(payload)
    if logs is not None:
        return logs, _extract_players(payload), False

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
            return logs, _extract_players(payload), True
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

    for raw_event in _iter_log_events(logs):
        event_type = str(raw_event.get("type") or "")
        if event_type not in {"playTile", "playPartisan"}:
            continue

        for args in _event_args(raw_event.get("args")):
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

            player_id = _optional_text(args.get("player_id"))
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


def _iter_log_events(logs: list[Any]):
    for line in logs:
        if not isinstance(line, dict):
            continue
        data = line.get("data")
        if isinstance(data, list):
            for event in data:
                if isinstance(event, dict):
                    yield event
        elif isinstance(data, dict):
            yield data


def _event_args(raw_args: Any) -> list[dict[str, Any]]:
    if isinstance(raw_args, dict):
        return [raw_args]
    if isinstance(raw_args, list):
        return [item for item in raw_args if isinstance(item, dict)]
    return []


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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _summary_from_row(row: sqlite3.Row, *, cached: bool) -> dict[str, Any]:
    return {
        "game_id": row["game_id"],
        "bga_table_id": row["bga_table_id"],
        "status": row["status"],
        "event_count": row["event_count"],
        "tile_count": row["tile_count"],
        "meeple_count": row["meeple_count"],
        "archive_requested": bool(row["archive_requested"]),
        "fetched_at": row["fetched_at"],
        "cached": cached,
    }
