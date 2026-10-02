from __future__ import annotations

import argparse
import json
import os
import sqlite3
from collections import Counter
from contextlib import closing
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None


def find_tile_count_mismatches(events: Any) -> list[dict[str, Any]]:
    """Compare pickTile and playTile counts separately for each tile_type."""
    if not isinstance(events, list) or not all(isinstance(event, dict) for event in events):
        raise ValueError("events_json must be an array of event objects")
    event_types: dict[int | str, Counter[str]] = {}
    for event in events:
        event_type = event.get("type")
        if event_type not in ("pickTile", "playTile"):
            continue
        tile_type = event.get("tile_type")
        if tile_type is None:
            continue
        if isinstance(tile_type, bool) or not isinstance(tile_type, (int, str)):
            raise ValueError("tile_type must be an integer, a string, or null")
        event_types.setdefault(tile_type, Counter())[event_type] += 1
    return [
        {
            "tile_type": tile_type,
            "pick_count": counts["pickTile"],
            "play_count": counts["playTile"],
            "difference": counts["pickTile"] - counts["playTile"],
        }
        for tile_type, counts in event_types.items()
        if counts["pickTile"] != counts["playTile"]
    ]


def inspect_discarded_replay_tiles(
    *, db_path: str | Path | None = None, events_file: str | Path | None = None,
    table_id: str | None = None,
) -> dict[str, Any]:
    if (db_path is None) == (events_file is None):
        raise ValueError("Specify exactly one of db_path or events_file")
    if events_file is not None and table_id is not None:
        raise ValueError("--table-id requires a database")
    source = Path(events_file if events_file is not None else db_path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"Source not found: {source}")
    report: dict[str, Any] = {
        "status": "ok",
        "source": str(source),
        "replays_scanned": 0,
        "replays_with_tile_count_mismatches": 0,
        "bga_table_ids": [],
        "matches": [],
        "errors": [],
    }

    def inspect_row(row: Any) -> None:
        report["replays_scanned"] += 1
        try:
            mismatches = find_tile_count_mismatches(json.loads(row["events_json"]))
        except (TypeError, ValueError) as exc:
            report["errors"].append({
                "game_id": row["game_id"], "bga_table_id": row["bga_table_id"], "error": str(exc),
            })
            return
        if not mismatches:
            return
        bga_table_id = row["bga_table_id"]
        report["matches"].append({
            "game_id": row["game_id"],
            "bga_table_id": bga_table_id,
            "bga_url": f"https://boardgamearena.com/table?table={bga_table_id}" if bga_table_id else None,
            "carcassonne_lab_url": row["carcassonne_lab_url"],
            "tile_count_mismatches": mismatches,
        })

    if events_file is not None:
        inspect_row({
            "game_id": None, "bga_table_id": None, "carcassonne_lab_url": None,
            "events_json": source.read_text(encoding="utf-8"),
        })
    else:
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            sql = (
                "SELECT game_id, bga_table_id, carcassonne_lab_url, events_json "
                "FROM game_replays WHERE status = 'ready'"
            )
            parameters: list[str] = []
            if table_id is not None:
                sql += " AND bga_table_id = ?"
                parameters.append(str(table_id).strip())
            for row in conn.execute(sql + " ORDER BY game_id", parameters):
                inspect_row(row)
    report["replays_with_tile_count_mismatches"] = len(report["matches"])
    report["bga_table_ids"] = sorted({
        str(match["bga_table_id"]) for match in report["matches"] if match["bga_table_id"] is not None
    })
    if report["errors"]:
        report["status"] = "partial"
    return report


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Find ready game_replays with unequal pickTile/playTile counts per tile_type. Read-only; no BGA requests.",
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--db-path", help="SQLite path; defaults to AUTH_SQLITE_PATH, DB_PATH, or data/auth.sqlite")
    source.add_argument("--events-file", help="Inspect a saved events_json array instead of the database")
    parser.add_argument("--table-id", help="Inspect one ready BGA table in the database")
    args = parser.parse_args()
    if args.events_file is None and args.db_path is None:
        args.db_path = os.getenv("AUTH_SQLITE_PATH") or os.getenv("DB_PATH") or str(
            Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"
        )
    try:
        report = inspect_discarded_replay_tiles(
            db_path=args.db_path, events_file=args.events_file, table_id=args.table_id,
        )
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
