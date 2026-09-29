from __future__ import annotations

import argparse
import json
import os
import sqlite3
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from .game_replay import (
    COLOR_SOURCE_BGA,
    MEEPLE_COLOR_NAMES,
    _has_complete_bga_colors,
    ensure_game_replays_schema,
)
from .replay_worker import (
    RETRY_REASONS,
    ReplayWorkerAlreadyRunningError,
    enqueue_historical_game_replay,
    replay_worker_lock,
)


TOURNAMENT_ID = "ETCOC-2026"
DEFAULT_PREVIEW_LIMIT = 100
REQUIRED_COLUMNS = {
    "duels": {"id", "tournament_id", "deleted_at"},
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


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def _default_batch_id(value: datetime) -> str:
    normalized = value
    if normalized.tzinfo is None:
        normalized = normalized.replace(tzinfo=timezone.utc)
    return f"{TOURNAMENT_ID}-replay-queue-{normalized.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}"


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


def _validate_schema(conn: sqlite3.Connection) -> None:
    tables = {
        str(row[0]).strip()
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    missing_tables = sorted(set(REQUIRED_COLUMNS) - tables)
    if missing_tables:
        raise RuntimeError("Missing required tables: " + ", ".join(missing_tables))

    for table_name, required in REQUIRED_COLUMNS.items():
        missing_columns = sorted(required - _table_columns(conn, table_name))
        if missing_columns:
            raise RuntimeError(
                f"Table {table_name} is missing required columns: "
                + ", ".join(missing_columns)
            )


def _load_tournament_games(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          g.id AS game_id,
          g.duel_id,
          g.bga_table_id,
          g.game_number,
          gr.game_id AS replay_game_id,
          gr.status AS replay_status,
          gr.events_json,
          gr.players_json,
          gr.color_source,
          gr.queue_class,
          gr.retry_reason,
          gr.next_attempt_at
        FROM games g
        JOIN duels d
          ON trim(COALESCE(d.id, '')) = trim(COALESCE(g.duel_id, ''))
        LEFT JOIN game_replays gr ON gr.game_id = g.id
        WHERE upper(trim(COALESCE(d.tournament_id, ''))) = upper(trim(?))
          AND trim(COALESCE(d.deleted_at, '')) = ''
          AND trim(COALESCE(g.deleted_at, '')) = ''
          AND trim(COALESCE(g.id, '')) <> ''
        ORDER BY
          d.id COLLATE NOCASE ASC,
          COALESCE(g.game_number, 999999) ASC,
          g.id COLLATE NOCASE ASC
        """,
        (TOURNAMENT_ID,),
    ).fetchall()


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


def populate_etcoc_2026_replay_queue(
    db_path: str | Path,
    *,
    apply: bool = False,
    batch_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    path = Path(db_path).expanduser()
    if not path.is_file():
        raise FileNotFoundError(f"SQLite database not found: {path}")

    run_now = now or datetime.now(timezone.utc)
    normalized_batch_id = _text(batch_id) or _default_batch_id(run_now)
    lock_path = path.with_name(f"{path.name}.bga-replay-worker.lock")
    lock_context = replay_worker_lock(lock_path) if apply else nullcontext()

    summary: dict[str, Any] = {
        "status": "ok",
        "mode": "apply" if apply else "dry-run",
        "db_path": str(path),
        "tournament_id": TOURNAMENT_ID,
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
            _validate_schema(conn)
            games = _load_tournament_games(conn)

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


def _append_item(summary: dict[str, Any], item: dict[str, Any]) -> None:
    if len(summary["items"]) < DEFAULT_PREVIEW_LIMIT:
        summary["items"].append(item)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            f"Populate the historical BGA replay queue for {TOURNAMENT_ID}. "
            "Dry-run is the default; pass --apply to write changes."
        )
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or os.getenv("DB_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--batch-id",
        help="Optional historical batch id",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Create/requeue rows; without this flag no data is changed",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    try:
        summary = populate_etcoc_2026_replay_queue(
            args.db_path,
            apply=args.apply,
            batch_id=args.batch_id,
        )
    except (
        FileNotFoundError,
        OSError,
        ReplayWorkerAlreadyRunningError,
        RuntimeError,
        sqlite3.Error,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["status"] in {"ok", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
