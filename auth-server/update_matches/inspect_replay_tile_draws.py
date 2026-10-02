from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None


def find_repeated_player_draws(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Find maximal pickTile runs per player, reset only by that player's playTile."""
    pending: dict[str, list[int]] = {}
    matches: list[dict[str, Any]] = []
    names = {
        str(event["player_id"]).strip(): event["player_name"]
        for event in events
        if event.get("player_id") is not None and event.get("player_name")
    }
    unattributed = 0

    def finish(player_id: str, indexes: list[int], placement: dict[str, Any] | None) -> None:
        if len(indexes) < 2:
            return
        picks = [events[index] for index in indexes]
        matches.append({
            "player_id": player_id,
            "player_name": names.get(player_id),
            "pick_count": len(picks),
            "first_seq": picks[0].get("seq"),
            "last_seq": picks[-1].get("seq"),
            "pick_events": picks,
            "events": events[indexes[0]:indexes[-1] + 1],
            "following_playTile": placement,
            "_first_index": indexes[0],
        })

    for index, event in enumerate(events):
        event_type = event.get("type")
        if event_type not in {"pickTile", "playTile"}:
            continue
        player_id = str(event.get("player_id") if event.get("player_id") is not None else "").strip()
        if not player_id:
            unattributed += 1
            continue
        if event_type == "pickTile":
            pending.setdefault(player_id, []).append(index)
        else:
            finish(player_id, pending.pop(player_id, []), event)
    for player_id, indexes in pending.items():
        finish(player_id, indexes, None)
    matches.sort(key=lambda match: match["_first_index"])
    for match in matches:
        del match["_first_index"]
    return matches, unattributed


def inspect_replay_tile_draws(
    *,
    db_path: str | Path | None = None,
    events_file: str | Path | None = None,
    table_id: str | None = None,
    player_id: str | None = None,
    with_url: bool = False,
) -> dict[str, Any]:
    if (db_path is None) == (events_file is None):
        raise ValueError("Specify exactly one of db_path or events_file")
    if events_file is not None and (table_id is not None or with_url):
        raise ValueError("--table-id and --with-url require a database")
    source = Path(events_file if events_file is not None else db_path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"Source not found: {source}")
    if events_file is not None:
        rows = [{
            "game_id": None, "bga_table_id": None, "status": None,
            "carcassonne_lab_url": None, "events_json": source.read_text(encoding="utf-8"),
        }]
    else:
        with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            conditions = ["events_json IS NOT NULL", "trim(events_json) <> ''"]
            parameters: list[Any] = []
            if table_id is not None:
                conditions.append("bga_table_id = ?")
                parameters.append(str(table_id).strip())
            if with_url:
                conditions.append("carcassonne_lab_url IS NOT NULL AND trim(carcassonne_lab_url) <> ''")
            rows = conn.execute(
                "SELECT game_id, bga_table_id, status, carcassonne_lab_url, events_json "
                "FROM game_replays WHERE " + " AND ".join(conditions) + " ORDER BY game_id",
                parameters,
            ).fetchall()
    report: dict[str, Any] = {
        "status": "ok",
        "source": str(source),
        "replays_scanned": len(rows),
        "replays_with_matches": 0,
        "matches_found": 0,
        "unattributed_tile_events": 0,
        "bga_table_ids": [],
        "matches": [],
        "errors": [],
    }
    table_ids: set[str] = set()
    for row in rows:
        try:
            events = json.loads(row["events_json"])
            if not isinstance(events, list) or not all(isinstance(event, dict) for event in events):
                raise ValueError("events_json must be an array of event objects")
            matches, unattributed = find_repeated_player_draws(events)
        except (TypeError, ValueError) as exc:
            report["errors"].append({
                "game_id": row["game_id"], "bga_table_id": row["bga_table_id"], "error": str(exc),
            })
            continue
        report["unattributed_tile_events"] += unattributed
        if player_id is not None:
            matches = [match for match in matches if match["player_id"] == str(player_id).strip()]
        if not matches:
            continue
        report["replays_with_matches"] += 1
        if row["bga_table_id"] is not None:
            table_ids.add(str(row["bga_table_id"]))
        for match in matches:
            report["matches"].append({
                "game_id": row["game_id"],
                "bga_table_id": row["bga_table_id"],
                "replay_status": row["status"],
                "carcassonne_lab_url": row["carcassonne_lab_url"],
                **match,
            })
    report["bga_table_ids"] = sorted(table_ids)
    report["matches_found"] = len(report["matches"])
    if report["errors"]:
        report["status"] = "partial"
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect 2+ pickTile events by one player without that player's playTile between them. Read-only; no BGA connection.",
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--db-path", help="SQLite path; defaults to AUTH_SQLITE_PATH, DB_PATH, or data/auth.sqlite")
    source.add_argument("--events-file", help="JSON file containing a saved events_json array")
    parser.add_argument("--table-id", help="Inspect one BGA table in the database")
    parser.add_argument("--player-id", help="Show matches for one player")
    parser.add_argument("--with-url", action="store_true", help="Only inspect database rows with a populated CarcassonneLab URL")
    args = parser.parse_args()
    if args.events_file is None and args.db_path is None:
        args.db_path = os.getenv("AUTH_SQLITE_PATH") or os.getenv("DB_PATH") or str(
            Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"
        )
    return args


def main() -> int:
    load_dotenv()
    args = parse_args()
    try:
        report = inspect_replay_tile_draws(
            db_path=args.db_path, events_file=args.events_file, table_id=args.table_id,
            player_id=args.player_id, with_url=args.with_url,
        )
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
