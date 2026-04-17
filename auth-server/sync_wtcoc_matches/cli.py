from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from .client import DEFAULT_WTCOC_API_TOKEN, WtcocApiClient
from .service import WtcocSyncService
from .sqlite_repository import SqliteWtcocRepository


class _TimestampedStream:
    def __init__(self, wrapped) -> None:
        self._wrapped = wrapped
        self._buffer = ""

    def write(self, data) -> int:
        text = str(data)
        if not text:
            return 0
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._emit(line)
        return len(text)

    def flush(self) -> None:
        if self._buffer:
            self._emit(self._buffer)
            self._buffer = ""
        self._wrapped.flush()

    def _emit(self, line: str) -> None:
        ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
        self._wrapped.write(f"[{ts}] {line}\n")

    def isatty(self) -> bool:
        return bool(getattr(self._wrapped, "isatty", lambda: False)())

    @property
    def encoding(self):
        return getattr(self._wrapped, "encoding", "utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch WTCOC data and analyze mapping readiness for auth-server.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--tournament-id",
        default=os.getenv("WTCOC_TOURNAMENT_ID", "WTCOC-2026"),
        help="Target tournaments.id value in auth-server.",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("WTCOC_API_TOKEN", DEFAULT_WTCOC_API_TOKEN),
        help="WTCOC API token.",
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("WTCOC_API_BASE_URL"),
        help="Override WTCOC API base URL.",
    )
    parser.add_argument(
        "--match-id",
        help="Optional external WTCOC match id filter, for example 1.",
    )
    parser.add_argument(
        "--skip-playoff",
        action="store_true",
        help="Do not call the playoff endpoint.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Upsert mapped WTCOC matches and duels into SQLite.",
    )
    parser.add_argument(
        "--actor-id",
        default=os.getenv("WTCOC_SYNC_ACTOR_ID", "1"),
        help="created_by/updated_by value for script writes. Default: 1",
    )
    return parser.parse_args()


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def _configure_stream_logging() -> None:
    if not isinstance(sys.stdout, _TimestampedStream):
        sys.stdout = _TimestampedStream(sys.stdout)
    if not isinstance(sys.stderr, _TimestampedStream):
        sys.stderr = _TimestampedStream(sys.stderr)


def _run_update_ratings_for_missing_planned_duels() -> dict:
    auth_server_dir = Path(__file__).resolve().parents[1]
    command = [
        sys.executable,
        str(auth_server_dir / "run_update_ratings.py"),
        "--planned-missing-ratings",
    ]
    completed = subprocess.run(
        command,
        cwd=str(auth_server_dir),
        check=True,
        capture_output=True,
        text=True,
    )
    stdout = str(completed.stdout or "").strip()
    if not stdout:
        return {"ok": True, "mode": "planned_missing_ratings", "output": None}
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {"ok": True, "mode": "planned_missing_ratings", "output": stdout}


def main() -> int:
    _configure_stream_logging()
    load_dotenv()
    args = parse_args()
    repository = SqliteWtcocRepository(args.db_path)
    client = WtcocApiClient(
        token=args.token,
        base_url=args.base_url or None or "https://www.carcassonne.cat/wtcoc/api",
    )
    service = WtcocSyncService(repository=repository, client=client)
    summary = service.build_apply_payload(
        tournament_id=args.tournament_id,
        include_playoff=not args.skip_playoff,
        external_match_id=args.match_id,
    )
    output = {
        "fetched_at_utc": summary.get("fetched_at_utc"),
        "sources": summary.get("sources", []),
        "apply_preview": summary.get("apply_preview", {}),
    }
    if args.apply:
        apply_payload = summary.pop("apply_payload")
        apply_result = repository.upsert_matches_and_duels(
            tournament_id=args.tournament_id,
            actor_id=args.actor_id,
            matches=apply_payload["matches"],
            duels=apply_payload["duels"],
        )
        output["apply_result"] = apply_result
        changed_duel_ids = list(apply_result.get("changed_duel_ids") or [])
        if changed_duel_ids:
            output["ratings_update"] = {
                "triggered": True,
                "reason": "wtcoc_duels_changed",
                "changed_duel_ids_count": len(changed_duel_ids),
                "result": _run_update_ratings_for_missing_planned_duels(),
            }
        else:
            output["ratings_update"] = {
                "triggered": False,
                "reason": "no_wtcoc_duel_changes",
                "changed_duel_ids_count": 0,
            }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
