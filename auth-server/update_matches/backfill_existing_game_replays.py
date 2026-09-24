from __future__ import annotations

import argparse
import copy
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
    COLOR_SOURCE_FALLBACK,
    _synchronize_meeple_stats_colors,
    apply_replay_player_colors,
    build_board_stats,
    build_carcassonne_lab_url,
    build_meeple_stats,
)
from .replay_worker import replay_worker_lock


DEFAULT_BATCH_PREFIX = "legacy-ready-backfill"
REQUIRED_COLUMNS = {
    "game_id",
    "status",
    "events_json",
    "players_json",
    "carcassonne_lab_url",
    "board_stats_json",
    "meeple_stats_json",
    "scoring_json",
    "player_time_json",
    "fetched_at",
    "last_attempt_at",
    "last_error",
    "next_attempt_at",
    "retry_reason",
    "queue_class",
    "queued_at",
    "historical_batch_id",
    "history_request_count",
    "color_refresh_count",
    "color_source",
    "archive_requested_at",
    "last_account_label",
    "lease_owner",
    "lease_until",
    "created_at",
    "updated_at",
}


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def _format_timestamp(value: datetime) -> str:
    normalized = value
    if normalized.tzinfo is None:
        normalized = normalized.replace(tzinfo=timezone.utc)
    return normalized.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def _default_batch_id(value: datetime) -> str:
    normalized = value
    if normalized.tzinfo is None:
        normalized = normalized.replace(tzinfo=timezone.utc)
    return f"{DEFAULT_BATCH_PREFIX}-{normalized.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}"


def _json_array(value: Any) -> list[Any] | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, list) else None


def _json_object(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _text(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _clear_player_colors(players: list[Any]) -> None:
    for player in players:
        if not isinstance(player, dict):
            continue
        player["color_hex"] = None
        player["meeple_color"] = None


def _normalized_ready_payload(row: sqlite3.Row) -> dict[str, Any]:
    raw_events = _json_array(row["events_json"])
    raw_players = _json_array(row["players_json"])
    valid_replay_json = raw_events is not None and raw_players is not None
    events = raw_events or []
    players = raw_players or []

    # color_source was populated by the schema migration and is authoritative.
    # Do not try to infer provenance from red/green values: those can be either
    # real BGA colors or the already-applied fallback palette.
    color_source = _text(row["color_source"])
    if color_source not in {COLOR_SOURCE_BGA, COLOR_SOURCE_FALLBACK}:
        raise ValueError(
            f"Ready replay {row['game_id']} has invalid color_source={color_source!r}"
        )

    # Normalize colors only in temporary copies used to rebuild derived data.
    # The persisted replay payload and color_source remain untouched.
    derived_events = copy.deepcopy(events)
    derived_players = copy.deepcopy(players)
    if color_source == COLOR_SOURCE_FALLBACK:
        _clear_player_colors(derived_players)
    apply_replay_player_colors(derived_events, derived_players)

    board_stats_json = row["board_stats_json"]
    carcassonne_lab_url = row["carcassonne_lab_url"]
    meeple_stats_json = row["meeple_stats_json"]
    derived_backfilled: list[str] = []

    if valid_replay_json:
        board_stats_json = _to_json(build_board_stats(derived_events))
        derived_backfilled.append("board_stats_json")

        rebuilt_lab_url = build_carcassonne_lab_url(derived_events, derived_players)
        if rebuilt_lab_url:
            carcassonne_lab_url = rebuilt_lab_url
            derived_backfilled.append("carcassonne_lab_url")

        meeple_stats = _json_object(meeple_stats_json)
        if meeple_stats is None:
            meeple_stats = build_meeple_stats(derived_events, derived_players)
        else:
            _synchronize_meeple_stats_colors(meeple_stats, derived_players)
        meeple_stats_json = _to_json(meeple_stats)
        derived_backfilled.append("meeple_stats_json")

    color_refresh_count = _non_negative_int(row["color_refresh_count"])
    needs_color_refresh = (
        color_source == COLOR_SOURCE_FALLBACK
        and color_refresh_count < 1
    )
    already_scheduled = (
        _text(row["retry_reason"]) == "colors"
        and _text(row["next_attempt_at"]) is not None
    )

    missing_unrecoverable_fields = [
        field
        for field in ("scoring_json", "player_time_json")
        if _json_object(row[field]) is None
    ]

    return {
        "valid_replay_json": valid_replay_json,
        "events_json": row["events_json"],
        "players_json": row["players_json"],
        "carcassonne_lab_url": carcassonne_lab_url,
        "board_stats_json": board_stats_json,
        "meeple_stats_json": meeple_stats_json,
        "color_source": color_source,
        "color_refresh_count": color_refresh_count,
        "needs_color_refresh": needs_color_refresh,
        "already_scheduled": already_scheduled,
        "derived_backfilled": derived_backfilled,
        "missing_unrecoverable_fields": missing_unrecoverable_fields,
    }


def backfill_existing_game_replays(
    db_path: str | Path,
    *,
    apply: bool = False,
    batch_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    path = Path(db_path).expanduser()
    if not path.is_file():
        raise FileNotFoundError(f"SQLite database not found: {path}")

    migration_time = now or datetime.now(timezone.utc)
    timestamp = _format_timestamp(migration_time)
    normalized_batch_id = _text(batch_id) or _default_batch_id(migration_time)

    summary: dict[str, Any] = {
        "status": "ok",
        "mode": "apply" if apply else "dry-run",
        "db_path": str(path),
        "batch_id": normalized_batch_id,
        "error_rows_deleted": 0,
        "ready_rows_processed": 0,
        "ready_bga_colors": 0,
        "ready_fallback_colors": 0,
        "color_refresh_scheduled": 0,
        "color_refresh_already_scheduled": 0,
        "color_refresh_already_attempted": 0,
        "invalid_replay_json": 0,
        "missing_unrecoverable_derived": {},
        "scheduled_game_ids": [],
    }

    lock_path = path.with_name(f"{path.name}.bga-replay-worker.lock")
    lock_context = replay_worker_lock(lock_path) if apply else nullcontext()
    with lock_context, sqlite3.connect(path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")

        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(game_replays)").fetchall()
        }
        missing_columns = sorted(REQUIRED_COLUMNS - columns)
        if missing_columns:
            raise RuntimeError(
                "game_replays schema is not ready; missing columns: "
                + ", ".join(missing_columns)
            )

        error_count = int(conn.execute(
            "SELECT COUNT(*) FROM game_replays WHERE lower(trim(status)) = 'error'"
        ).fetchone()[0])
        summary["error_rows_deleted"] = error_count

        ready_rows = conn.execute(
            "SELECT * FROM game_replays WHERE lower(trim(status)) = 'ready' ORDER BY game_id"
        ).fetchall()

        if apply:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM game_replays WHERE lower(trim(status)) = 'error'"
            )

        try:
            for row in ready_rows:
                payload = _normalized_ready_payload(row)
                summary["ready_rows_processed"] += 1
                if not payload["valid_replay_json"]:
                    summary["invalid_replay_json"] += 1

                color_source = payload["color_source"]
                if color_source == COLOR_SOURCE_BGA:
                    summary["ready_bga_colors"] += 1
                else:
                    summary["ready_fallback_colors"] += 1

                for field in payload["missing_unrecoverable_fields"]:
                    missing = summary["missing_unrecoverable_derived"]
                    missing[field] = int(missing.get(field, 0)) + 1

                needs_refresh = bool(payload["needs_color_refresh"])
                already_scheduled = bool(payload["already_scheduled"])
                if needs_refresh:
                    summary["color_refresh_scheduled"] += 1
                    summary["scheduled_game_ids"].append(str(row["game_id"]))
                    if already_scheduled:
                        summary["color_refresh_already_scheduled"] += 1
                elif (
                    color_source == COLOR_SOURCE_FALLBACK
                    and int(payload["color_refresh_count"]) >= 1
                ):
                    summary["color_refresh_already_attempted"] += 1

                if not apply:
                    continue

                existing_queue_class = _text(row["queue_class"])
                has_managed_queue_metadata = any((
                    _text(row["retry_reason"]),
                    _text(row["queued_at"]),
                    _text(row["historical_batch_id"]),
                    _text(row["last_account_label"]),
                    _non_negative_int(row["history_request_count"]),
                    _non_negative_int(row["color_refresh_count"]),
                ))
                queue_class = (
                    existing_queue_class
                    if has_managed_queue_metadata
                    and existing_queue_class in {"fresh", "historical"}
                    else "historical"
                )
                retry_reason = "colors" if needs_refresh else None
                next_attempt_at = (
                    _text(row["next_attempt_at"])
                    if needs_refresh and already_scheduled
                    else timestamp if needs_refresh else None
                )
                queued_at = (
                    _text(row["queued_at"]) or timestamp
                    if needs_refresh
                    else _text(row["queued_at"])
                )
                historical_batch_id = _text(row["historical_batch_id"])
                if needs_refresh and queue_class == "historical":
                    historical_batch_id = historical_batch_id or normalized_batch_id

                fetched_at = (
                    _text(row["fetched_at"])
                    or _text(row["last_attempt_at"])
                    or _text(row["updated_at"])
                    or _text(row["created_at"])
                    or timestamp
                )

                conn.execute(
                    """
                    UPDATE game_replays
                    SET status = 'ready',
                        carcassonne_lab_url = ?,
                        board_stats_json = ?,
                        meeple_stats_json = ?,
                        fetched_at = ?,
                        last_error = NULL,
                        next_attempt_at = ?,
                        retry_reason = ?,
                        queue_class = ?,
                        queued_at = ?,
                        historical_batch_id = ?,
                        history_request_count = MAX(COALESCE(history_request_count, 0), 1),
                        color_refresh_count = ?,
                        lease_owner = NULL,
                        lease_until = NULL,
                        updated_at = ?
                    WHERE game_id = ?
                    """,
                    (
                        payload["carcassonne_lab_url"],
                        payload["board_stats_json"],
                        payload["meeple_stats_json"],
                        fetched_at,
                        next_attempt_at,
                        retry_reason,
                        queue_class,
                        queued_at,
                        historical_batch_id,
                        payload["color_refresh_count"],
                        timestamp,
                        row["game_id"],
                    ),
                )

            if apply:
                conn.commit()
        except Exception:
            if apply:
                conn.rollback()
            raise

    summary["scheduled_game_ids"] = summary["scheduled_game_ids"][:50]
    summary["scheduled_game_ids_truncated"] = (
        summary["color_refresh_scheduled"] > len(summary["scheduled_game_ids"])
    )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "One-off cleanup/backfill for existing game_replays. "
            "Dry-run is the default; pass --apply to commit one transaction."
        )
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or os.getenv("DB_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--batch-id",
        help="Historical batch id for newly scheduled legacy color refreshes",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit the cleanup/backfill; without this flag nothing is changed",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    try:
        summary = backfill_existing_game_replays(
            args.db_path,
            apply=args.apply,
            batch_id=args.batch_id,
        )
    except (FileNotFoundError, OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
