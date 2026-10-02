from __future__ import annotations

import argparse
import json
import os
import sqlite3
from contextlib import closing, nullcontext
from pathlib import Path
from typing import Any

from .game_replay import enqueue_fresh_game_replay
from .inspect_discarded_replay_tiles import find_tile_count_mismatches, load_dotenv
from .replay_worker import ReplayWorkerAlreadyRunningError, replay_worker_lock


REQUIRED_COLUMNS = {
    "game_id", "bga_table_id", "status", "events_json", "carcassonne_lab_url",
    "retry_reason", "queue_class", "queued_at", "next_attempt_at",
    "historical_batch_id", "history_request_count", "color_refresh_count",
    "color_source", "archive_requested_at", "last_account_label", "lease_owner",
    "lease_until", "last_error", "updated_at",
}


def _load_reviewed_games(report_file: Path) -> tuple[dict[str, str], int]:
    report = json.loads(report_file.read_text(encoding="utf-8"))
    if not isinstance(report, dict) or not isinstance(report.get("matches"), list):
        raise ValueError("Report must contain a matches array from inspect_discarded_replay_tiles.py")
    games: dict[str, str] = {}
    for match in report["matches"]:
        if not isinstance(match, dict):
            raise ValueError("Each report match must be an object")
        game_id = str(match.get("game_id") or "").strip()
        table_id = str(match.get("bga_table_id") or "").strip()
        if not game_id or not table_id.isdigit():
            raise ValueError("Each report match must have a game_id and numeric bga_table_id")
        if game_id in games and games[game_id] != table_id:
            raise ValueError(f"Conflicting bga_table_id values for game {game_id}")
        games[game_id] = table_id
    errors = report.get("errors", [])
    if not isinstance(errors, list):
        raise ValueError("Report errors must be an array")
    return games, len(errors)


def requeue_discarded_replay_tiles(
    db_path: str | Path, report_file: str | Path, *, apply: bool = False,
) -> dict[str, Any]:
    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"SQLite database not found: {path}")
    report_path = Path(report_file).expanduser().resolve()
    games, source_errors = _load_reviewed_games(report_path)
    summary: dict[str, Any] = {
        "status": "ok", "mode": "apply" if apply else "dry-run",
        "db_path": str(path), "report_file": str(report_path),
        "queue_class": "fresh", "report_games": len(games),
        "source_report_errors": source_errors,
        "eligible": 0, "queued": 0, "skipped": 0, "items": [], "errors": [],
    }
    lock_path = path.with_name(f"{path.name}.bga-replay-worker.lock")
    lock_context = replay_worker_lock(lock_path) if apply else nullcontext()
    mode = "rw" if apply else "ro"
    with lock_context, closing(sqlite3.connect(path.as_uri() + f"?mode={mode}", uri=True, timeout=30)) as conn:
        conn.row_factory = sqlite3.Row
        with conn:
            # Hold the write lock before selecting so validation and updates are atomic.
            conn.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(game_replays)")}
            missing = sorted(REQUIRED_COLUMNS - columns)
            if missing:
                raise ValueError("game_replays schema is missing columns: " + ", ".join(missing))
            for game_id, table_id in games.items():
                item: dict[str, Any] = {"game_id": game_id, "bga_table_id": table_id}
                summary["items"].append(item)
                row = conn.execute(
                    """
                    SELECT gr.*, g.id AS existing_game_id,
                           g.bga_table_id AS game_table_id, g.deleted_at,
                           (COALESCE(trim(gr.lease_owner), '') <> '' AND (
                               datetime(gr.lease_until) IS NULL
                               OR datetime(gr.lease_until) > datetime('now')
                           )) AS active_lease
                    FROM game_replays gr
                    LEFT JOIN games g ON g.id = gr.game_id
                    WHERE gr.game_id = ?
                    """,
                    (game_id,),
                ).fetchone()
                reason = None
                if row is None or row["existing_game_id"] is None:
                    reason = "not_found"
                elif str(row["bga_table_id"]) != table_id or str(row["game_table_id"]) != table_id:
                    reason = "table_id_mismatch"
                elif row["deleted_at"] is not None:
                    reason = "deleted_game"
                elif row["status"] != "ready":
                    reason = "not_ready"
                elif row["active_lease"]:
                    reason = "active_lease"
                else:
                    try:
                        mismatches = find_tile_count_mismatches(json.loads(row["events_json"]))
                    except (TypeError, ValueError) as exc:
                        reason = "invalid_events_json"
                        summary["errors"].append({"game_id": game_id, "error": str(exc)})
                    else:
                        if not mismatches:
                            reason = "counts_already_equal"
                        else:
                            item["tile_count_mismatches"] = mismatches
                if reason is not None:
                    item["action"] = "skip"
                    item["reason"] = reason
                    summary["skipped"] += 1
                    continue
                summary["eligible"] += 1
                item["action"] = "requeue_fresh"
                if apply:
                    enqueue_fresh_game_replay(conn, game_id=game_id, bga_table_id=table_id)
                    # The standard fresh helper waits five minutes; this refresh is due now.
                    conn.execute(
                        "UPDATE game_replays SET next_attempt_at = CURRENT_TIMESTAMP WHERE game_id = ?",
                        (game_id,),
                    )
                    summary["queued"] += 1
    if summary["errors"] or source_errors:
        summary["status"] = "partial"
    return summary


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Requeue reviewed tile-count mismatches as pending fresh replay requests; dry-run by default.",
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or os.getenv("DB_PATH") or str(
            Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"
        ),
        help="SQLite path; defaults to AUTH_SQLITE_PATH, DB_PATH, or data/auth.sqlite",
    )
    parser.add_argument("--report-file", required=True, help="Reviewed JSON report from inspect_discarded_replay_tiles.py")
    parser.add_argument("--apply", action="store_true", help="Apply the fresh queue updates in one transaction")
    args = parser.parse_args()
    try:
        summary = requeue_discarded_replay_tiles(args.db_path, args.report_file, apply=args.apply)
    except ReplayWorkerAlreadyRunningError as exc:
        print(json.dumps({"status": "locked", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
