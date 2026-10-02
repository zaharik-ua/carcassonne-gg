from __future__ import annotations

import argparse
import json
import os
import sqlite3
from contextlib import nullcontext
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from .game_replay import (
    CARCASSONNE_LAB_TILE_TYPE_BASE,
    CARCASSONNE_LAB_TILE_TYPES_BY_ID,
    _optional_int,
    build_carcassonne_lab_url,
    normalize_replay_events,
)
from .replay_worker import replay_worker_lock


REQUIRED_COLUMNS = {
    "game_id", "events_json", "players_json", "carcassonne_lab_url",
    "lease_until", "updated_at",
}
AVAILABLE_LEASE_SQL = """
    (lease_until IS NULL OR trim(lease_until) = ''
     OR datetime(lease_until) <= datetime('now'))
"""


class IncompleteDrawHistoryError(ValueError):
    """Saved events cannot establish the discarded draws unambiguously."""


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def _json_objects(value: Any, field: str) -> list[dict[str, Any]]:
    objects = json.loads(value)
    if not isinstance(objects, list) or not objects or not all(
        isinstance(item, dict) for item in objects
    ):
        raise ValueError(f"{field} must be a nonempty array of objects")
    return objects


def _discarded_draw(draw: dict[str, Any]) -> dict[str, Any]:
    tile_id = _optional_int(draw.get("tile_id"))
    if tile_id is None or not 1 <= tile_id <= len(CARCASSONNE_LAB_TILE_TYPES_BY_ID):
        raise ValueError(f"Unknown discarded tile id: {draw.get('tile_id')!r}")
    tile_type = CARCASSONNE_LAB_TILE_TYPES_BY_ID[tile_id - 1]
    saved_type = _optional_int(draw.get("tile_type"))
    if saved_type is not None and saved_type != tile_type:
        raise ValueError(f"Discarded tile {tile_id} has conflicting saved type {saved_type}")
    return {**draw, "type": "cantPlay", "tile_type": tile_type}


def restore_saved_discards(
    events: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Recover legacy discards only when saved draws cover every placement."""
    if any(event.get("type") == "cantPlay" for event in events):
        return [
            _discarded_draw(event) if event.get("type") == "cantPlay" else event
            for event in events
        ], 0

    tile_count = sum(event.get("type") == "playTile" for event in events)
    # All 71 non-starting base-game tiles were placed, so no draws were lost.
    if tile_count == len(CARCASSONNE_LAB_TILE_TYPES_BY_ID) - 1:
        return events, 0

    restored: list[dict[str, Any]] = []
    pending_draw: dict[str, Any] | None = None
    seen_ids: set[int] = set()
    inferred = 0
    for event in events:
        event_type = event.get("type")
        if event_type == "pickTile":
            tile_id = _optional_int(event.get("tile_id"))
            if tile_id is not None:
                if tile_id in seen_ids:
                    raise IncompleteDrawHistoryError(f"Repeated pickTile id: {tile_id}")
                seen_ids.add(tile_id)
            if pending_draw is not None:
                restored.append(_discarded_draw(pending_draw))
                inferred += 1
            pending_draw = event
        elif event_type == "playTile":
            if pending_draw is None:
                raise IncompleteDrawHistoryError("Missing pickTile before a saved placement")
            drawn_type = _optional_int(pending_draw.get("tile_type"))
            if drawn_type is None:
                tile_id = _optional_int(pending_draw.get("tile_id"))
                if tile_id is not None and 1 <= tile_id <= len(CARCASSONNE_LAB_TILE_TYPES_BY_ID):
                    drawn_type = CARCASSONNE_LAB_TILE_TYPES_BY_ID[tile_id - 1]
            if (
                drawn_type is None
                or not 1 <= drawn_type <= CARCASSONNE_LAB_TILE_TYPE_BASE
                or drawn_type != _optional_int(event.get("tile_type"))
            ):
                raise IncompleteDrawHistoryError("Saved pickTile does not match the next placement")
            pending_draw = None
        restored.append(event)
    if pending_draw is not None:
        # A final drawn tile can also belong to an unfinished/abandoned turn.
        raise IncompleteDrawHistoryError("Final unplayed pickTile has no saved cantPlay")
    return restored, inferred


def update_existing_carcassonne_lab_urls(
    db_path: str | Path,
    *,
    apply: bool = False,
) -> dict[str, Any]:
    """Rebuild populated URLs exclusively from previously stored SQLite data."""
    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"SQLite database not found: {path}")
    summary: dict[str, Any] = {
        "status": "ok",
        "mode": "apply" if apply else "dry-run",
        "db_path": str(path),
        "candidates": 0,
        "would_update": 0,
        "updated": 0,
        "unchanged": 0,
        "inferred_discards": 0,
        "recovered_legacy_rows": 0,
        "raw_log_rows": 0,
        "insufficient_data": 0,
        "failed": 0,
        "deferred": 0,
        "errors": [],
    }
    lock_path = path.with_name(f"{path.name}.bga-replay-worker.lock")
    lock_context = replay_worker_lock(lock_path) if apply else nullcontext()
    db_uri = path.as_uri() + ("?mode=rw" if apply else "?mode=ro")
    with lock_context, sqlite3.connect(db_uri, uri=True, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        columns = {row[1] for row in conn.execute("PRAGMA table_info(game_replays)")}
        missing = sorted(REQUIRED_COLUMNS - columns)
        if missing:
            raise RuntimeError("game_replays is missing columns: " + ", ".join(missing))
        if apply:
            conn.execute("BEGIN IMMEDIATE")
        logs_column = "logs_json" if "logs_json" in columns else "NULL AS logs_json"
        rows = conn.execute(
            f"""
            SELECT game_id, events_json, players_json, carcassonne_lab_url, {logs_column},
                   CASE WHEN {AVAILABLE_LEASE_SQL} THEN 0 ELSE 1 END AS leased
            FROM game_replays
            WHERE carcassonne_lab_url IS NOT NULL
              AND trim(carcassonne_lab_url) <> ''
            ORDER BY game_id
            """
        ).fetchall()
        summary["candidates"] = len(rows)
        for row in rows:
            if row["leased"]:
                summary["deferred"] += 1
                continue
            try:
                if row["logs_json"] and str(row["logs_json"]).strip():
                    logs = _json_objects(row["logs_json"], "logs_json")
                    events = normalize_replay_events(logs)
                    inferred = 0
                    summary["raw_log_rows"] += 1
                else:
                    events = _json_objects(row["events_json"], "events_json")
                    events, inferred = restore_saved_discards(events)
                players = _json_objects(row["players_json"], "players_json")
                url = build_carcassonne_lab_url(events, players)
                if not url:
                    raise ValueError("Cannot encode the saved replay events and players")
                old_path, separator, query = row["carcassonne_lab_url"].partition("?")
                if not separator:
                    raise ValueError("Existing URL has no player query parameters")
                new_path = url.partition("?")[0]
                if new_path.rsplit("/", 1)[-1] != old_path.rsplit("/", 1)[-1]:
                    raise IncompleteDrawHistoryError("Saved placements differ from the existing URL")
                url = new_path + "?" + query
            except IncompleteDrawHistoryError as exc:
                summary["insufficient_data"] += 1
                summary["errors"].append({
                    "game_id": row["game_id"], "reason": "insufficient_saved_data", "error": str(exc),
                })
                continue
            except (TypeError, ValueError) as exc:
                summary["failed"] += 1
                summary["errors"].append({
                    "game_id": row["game_id"], "reason": "invalid_saved_data", "error": str(exc),
                })
                continue
            summary["inferred_discards"] += inferred
            summary["recovered_legacy_rows"] += int(inferred > 0)
            if url == row["carcassonne_lab_url"]:
                summary["unchanged"] += 1
                continue
            summary["would_update"] += 1
            if apply:
                conn.execute(
                    """
                    UPDATE game_replays
                    SET carcassonne_lab_url = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE game_id = ?
                    """,
                    (url, row["game_id"]),
                )
                summary["updated"] += 1
    if summary["failed"] or summary["insufficient_data"] or summary["deferred"]:
        summary["status"] = "partial"
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild populated game_replays.carcassonne_lab_url using saved events_json "
            "and players_json only. No BGA connection. Dry-run is the default."
        )
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or os.getenv("DB_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite (defaults to AUTH_SQLITE_PATH, DB_PATH, or data/auth.sqlite)",
    )
    parser.add_argument("--apply", action="store_true", help="Commit the URL updates in one transaction")
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    try:
        summary = update_existing_carcassonne_lab_urls(args.db_path, apply=args.apply)
    except (OSError, RuntimeError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
