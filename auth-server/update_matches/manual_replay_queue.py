from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Mapping
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .game_replay import (
    COLOR_SOURCE_BGA,
    MEEPLE_COLOR_NAMES,
    _has_complete_bga_colors,
    ensure_game_replays_schema,
)
from .replay_worker import (
    RETRY_REASONS,
    enqueue_historical_game_replay,
    replay_worker_lock,
)


DEFAULT_PREVIEW_LIMIT = 100
BASE_REQUIRED_COLUMNS = {
    "duels": {"id", "deleted_at"},
    "games": {"id", "duel_id", "bga_table_id", "game_number", "deleted_at"},
    "game_replays": {
        "game_id",
        "status",
        "events_json",
        "players_json",
        "color_source",
        "queue_class",
        "retry_reason",
        "next_attempt_at",
    },
}

GameLoader = Callable[[sqlite3.Connection], list[sqlite3.Row]]


def populate_replay_queue(
    db_path: str | Path,
    *,
    load_games: GameLoader,
    required_columns: Mapping[str, set[str]],
    summary_fields: Mapping[str, Any],
    batch_prefix: str,
    apply: bool = False,
    batch_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Populate historical replay jobs selected by ``load_games``."""
    path = Path(db_path).expanduser()
    if not path.is_file():
        raise FileNotFoundError(f"SQLite database not found: {path}")

    run_now = now or datetime.now(timezone.utc)
    normalized_batch_id = _text(batch_id) or _default_batch_id(
        batch_prefix,
        run_now,
    )
    lock_path = path.with_name(f"{path.name}.bga-replay-worker.lock")
    lock_context = replay_worker_lock(lock_path) if apply else nullcontext()

    summary: dict[str, Any] = {
        "status": "ok",
        "mode": "apply" if apply else "dry-run",
        "db_path": str(path),
        **dict(summary_fields),
        "historical_batch_id": normalized_batch_id,
        "games_found": 0,
        "complete_replays": 0,
        "missing_replays": 0,
        "incomplete_replays": 0,
        "create_historical": 0,
        "requeue_historical": 0,
        "already_historical": 0,
        "invalid_bga_table_id": 0,
        "changed": 0,
        "failed": 0,
        "items": [],
        "errors": [],
    }

    with lock_context:
        with sqlite3.connect(path, timeout=30) as conn:
            conn.row_factory = sqlite3.Row
            if apply:
                ensure_game_replays_schema(conn)
                conn.commit()
            _validate_schema(conn, additional_required_columns=required_columns)
            games = load_games(conn)

        summary["games_found"] = len(games)
        for row in games:
            game_id = _text(row["game_id"])
            table_id = _text(row["bga_table_id"])
            has_replay = bool(_text(row["replay_game_id"]))
            reasons = [] if not has_replay else _incomplete_reasons(row)

            if has_replay and not reasons:
                summary["complete_replays"] += 1
                continue

            if has_replay:
                summary["incomplete_replays"] += 1
            else:
                summary["missing_replays"] += 1

            if not table_id or not table_id.isdigit():
                summary["invalid_bga_table_id"] += 1
                _append_item(
                    summary,
                    {
                        "game_id": game_id,
                        "bga_table_id": table_id or None,
                        "action": "skip_invalid_bga_table_id",
                        "reasons": reasons or ["missing_replay"],
                    },
                )
                continue

            if not has_replay:
                summary["create_historical"] += 1
                planned_action = "create_historical"
                force = False
                reset_color_source = False
            else:
                status = _text(row["replay_status"]).lower()
                color_source = _text(row["color_source"]).lower()
                reset_color_source = (
                    (status != "ready" and bool(color_source))
                    or (
                        color_source == COLOR_SOURCE_BGA
                        and "incomplete_player_colors" in reasons
                    )
                )
                if _is_active_historical(row) and not reset_color_source:
                    summary["already_historical"] += 1
                    _append_item(
                        summary,
                        {
                            "game_id": game_id,
                            "bga_table_id": table_id,
                            "action": "already_historical",
                            "reasons": reasons,
                        },
                    )
                    continue
                summary["requeue_historical"] += 1
                planned_action = "requeue_historical"
                force = True

            item = {
                "game_id": game_id,
                "bga_table_id": table_id,
                "action": planned_action,
                "reasons": reasons or ["missing_replay"],
            }
            if not apply:
                _append_item(summary, item)
                continue

            try:
                result = enqueue_historical_game_replay(
                    path,
                    game_id,
                    historical_batch_id=normalized_batch_id,
                    force=force,
                    reset_color_source=reset_color_source,
                    now=run_now,
                )
                summary["changed"] += int(result["action"] == "queued")
                item["result"] = result["action"]
                _append_item(summary, item)
            except (OSError, sqlite3.Error, ValueError) as exc:
                summary["failed"] += 1
                summary["errors"].append(
                    {"game_id": game_id, "error": str(exc) or exc.__class__.__name__}
                )

    if summary["failed"] or summary["invalid_bga_table_id"]:
        summary["status"] = "partial"
    summary["items_truncated"] = (
        summary["games_found"] - summary["complete_replays"]
        > len(summary["items"])
    )
    return summary


def _default_batch_id(prefix: str, value: datetime) -> str:
    normalized = value
    if normalized.tzinfo is None:
        normalized = normalized.replace(tzinfo=timezone.utc)
    timestamp = normalized.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _json_list(value: Any) -> list[Any] | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, list) else None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row[1]).strip()
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        if str(row[1] or "").strip()
    }


def _validate_schema(
    conn: sqlite3.Connection,
    *,
    additional_required_columns: Mapping[str, set[str]],
) -> None:
    required_columns = {
        table_name: set(columns)
        for table_name, columns in BASE_REQUIRED_COLUMNS.items()
    }
    for table_name, columns in additional_required_columns.items():
        required_columns.setdefault(table_name, set()).update(columns)
    tables = {
        str(row[0]).strip()
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    missing_tables = sorted(set(required_columns) - tables)
    if missing_tables:
        raise RuntimeError("Missing required tables: " + ", ".join(missing_tables))

    for table_name, required in required_columns.items():
        missing_columns = sorted(required - _table_columns(conn, table_name))
        if missing_columns:
            raise RuntimeError(
                f"Table {table_name} is missing required columns: "
                + ", ".join(missing_columns)
            )


def _has_complete_stored_player_colors(
    events: list[Any],
    players: list[Any],
) -> bool:
    if not _has_complete_bga_colors(events, players):
        return False
    moving_player_ids: list[str] = []
    for event in events:
        if not isinstance(event, dict) or event.get("type") != "playTile":
            continue
        player_id = _text(event.get("player_id"))
        if player_id and player_id not in moving_player_ids:
            moving_player_ids.append(player_id)
    players_by_id = {
        _text(player.get("player_id")): player
        for player in players
        if isinstance(player, dict) and _text(player.get("player_id"))
    }
    return all(
        MEEPLE_COLOR_NAMES.get(
            _text(players_by_id[player_id].get("color_hex"))
            .lower()
            .removeprefix("#")
        )
        == _text(players_by_id[player_id].get("meeple_color")).lower()
        for player_id in moving_player_ids
    )


def _incomplete_reasons(row: sqlite3.Row) -> list[str]:
    reasons: list[str] = []
    status = _text(row["replay_status"]).lower()
    color_source = _text(row["color_source"]).lower()
    events = _json_list(row["events_json"])
    players = _json_list(row["players_json"])

    if status != "ready":
        reasons.append(f"status:{status or 'missing'}")
    if events is None:
        reasons.append("invalid_events_json")
    if players is None:
        reasons.append("invalid_players_json")
    if color_source != COLOR_SOURCE_BGA:
        reasons.append(f"color_source:{color_source or 'missing'}")
    if (
        events is None
        or players is None
        or not _has_complete_stored_player_colors(events, players)
    ):
        reasons.append("incomplete_player_colors")
    return reasons


def _is_active_historical(row: sqlite3.Row) -> bool:
    return (
        _text(row["queue_class"]).lower() == "historical"
        and _text(row["retry_reason"]).lower() in RETRY_REASONS
        and bool(_text(row["next_attempt_at"]))
    )


def _append_item(summary: dict[str, Any], item: dict[str, Any]) -> None:
    if len(summary["items"]) < DEFAULT_PREVIEW_LIMIT:
        summary["items"].append(item)
