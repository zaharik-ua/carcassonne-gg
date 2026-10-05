from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None


CSV_COLUMNS = (
    "Game ID", "Tournament", "Edition", "Round", "PlayerId1", "PlayerId2",
    "PlayerName1", "PlayerName2", "Date", "URL",
)

EXPORT_QUERY = """
    SELECT
      gr.bga_table_id AS "Game ID",
      t.category AS "Tournament",
      t.short_title AS "Edition",
      CASE WHEN lower(trim(d.source_type)) = 'challenge'
        THEN cp.short_name
        ELSE m.round_name
      END AS "Round",
      d.player_1_id AS "PlayerId1",
      d.player_2_id AS "PlayerId2",
      p1.bga_nickname AS "PlayerName1",
      p2.bga_nickname AS "PlayerName2",
      COALESCE(NULLIF(trim(d.time_utc), ''), m.time_utc) AS "Date",
      gr.carcassonne_lab_url AS "URL"
    FROM game_replays gr
    LEFT JOIN games g ON g.id = gr.game_id
    LEFT JOIN duels d ON d.id = g.duel_id
    LEFT JOIN matches m ON m.id = d.match_id
    LEFT JOIN challenge_periods cp ON cp.id = d.challenge_period_id
      AND lower(trim(d.source_type)) = 'challenge'
    LEFT JOIN tournaments t ON t.id = COALESCE(
      NULLIF(trim(d.tournament_id), ''),
      NULLIF(trim(m.tournament_id), ''),
      NULLIF(trim(cp.rivals_tournament_id), '')
    )
    LEFT JOIN profiles p1 ON p1.id = d.player_1_id
    LEFT JOIN profiles p2 ON p2.id = d.player_2_id
    WHERE lower(trim(gr.status)) = 'ready'
    ORDER BY gr.bga_table_id, gr.game_id
"""


def export_game_replays(db_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    """Replace a CSV snapshot with one row per ready replay; never modify the DB."""
    source = Path(db_path).expanduser().resolve()
    destination = Path(output_path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"Database not found: {source}")
    if destination == source or (
        destination.exists() and destination.samefile(source)
    ):
        raise ValueError("CSV output must not overwrite the database")
    if destination.suffix.lower() != ".csv":
        raise ValueError("Output path must have a .csv extension")

    temporary_path: Path | None = None
    rows_exported = 0
    try:
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as conn:
            # Execute before opening the output so schema errors preserve the previous CSV.
            cursor = conn.execute(EXPORT_QUERY)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with NamedTemporaryFile(
                mode="w", encoding="utf-8-sig", newline="",
                dir=destination.parent, prefix=f".{destination.name}.",
                suffix=".tmp", delete=False,
            ) as handle:
                temporary_path = Path(handle.name)
                writer = csv.writer(handle)
                writer.writerow(CSV_COLUMNS)
                for row in cursor:
                    writer.writerow(row)
                    rows_exported += 1
        temporary_path.replace(destination)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    return {
        "status": "ok", "db_path": str(source),
        "output_path": str(destination), "rows_exported": rows_exported,
    }


def main() -> int:
    load_dotenv()
    server_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Export every ready game replay to CSV. Replaces the CSV; database is read-only.",
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or os.getenv("DB_PATH")
        or str(server_dir / "data" / "auth.sqlite"),
        help="SQLite path (defaults to AUTH_SQLITE_PATH, DB_PATH, or data/auth.sqlite)",
    )
    parser.add_argument(
        "--output", default=str(server_dir / "export" / "game_replays.csv"),
        help="Destination CSV (defaults to auth-server/export/game_replays.csv)",
    )
    args = parser.parse_args()
    try:
        summary = export_game_replays(args.db_path, args.output)
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
