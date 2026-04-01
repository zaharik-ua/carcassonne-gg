from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

from update_profile_bga_data.service import ProfileBgaDataUpdateService
from update_profile_bga_data.sqlite_repository import SqliteProfileBgaDataRepository


class ProfileBgaDataBatchService:
    def __init__(self, *, db_path: str, include_removed: bool = False) -> None:
        self.db_path = str(Path(db_path).resolve())
        self.include_removed = bool(include_removed)
        self.repository = SqliteProfileBgaDataRepository(self.db_path)
        self.single_service = ProfileBgaDataUpdateService(repository=self.repository)

    def run(self, *, limit: int, player_ids: list[str] | None = None) -> dict:
        normalized_player_ids = [
            str(player_id).strip()
            for player_id in (player_ids or [])
            if str(player_id).strip()
        ]
        target_ids = normalized_player_ids or self._fetch_player_ids(limit=max(1, int(limit)))

        summary: dict[str, object] = {
            "ok": True,
            "db_path": self.db_path,
            "requested": len(target_ids),
            "processed": 0,
            "updated": 0,
            "removed": 0,
            "unchanged": 0,
            "failed": 0,
            "results": [],
        }

        for index, player_id in enumerate(target_ids, start=1):
            try:
                result = self.single_service.run_for_player(player_id)
            except Exception as exc:
                result = {
                    "ok": False,
                    "player_id": player_id,
                    "status": "error",
                    "message": str(exc),
                }

            summary["processed"] = int(summary["processed"]) + 1
            status = str(result.get("status") or "").strip().lower()
            if result.get("ok"):
                if status == "removed":
                    summary["removed"] = int(summary["removed"]) + 1
                elif result.get("updated"):
                    summary["updated"] = int(summary["updated"]) + 1
                else:
                    summary["unchanged"] = int(summary["unchanged"]) + 1
            else:
                summary["failed"] = int(summary["failed"]) + 1
                summary["ok"] = False

            summary["results"].append({
                "index": index,
                "player_id": player_id,
                "ok": bool(result.get("ok")),
                "status": result.get("status"),
                "updated": bool(result.get("updated")),
                "message": result.get("message"),
            })

        return summary

    def run_all(self, *, limit: int, stop_after_consecutive_failures: int = 5) -> dict:
        batch_limit = max(1, int(limit))
        failure_limit = max(1, int(stop_after_consecutive_failures))
        processed_ids: set[str] = set()
        skipped_failed_ids: set[str] = set()
        consecutive_failed_players = 0
        summary: dict[str, object] = {
            "ok": True,
            "db_path": self.db_path,
            "mode": "all",
            "batch_limit": batch_limit,
            "stop_after_consecutive_failures": failure_limit,
            "batches": 0,
            "requested": 0,
            "processed": 0,
            "updated": 0,
            "removed": 0,
            "unchanged": 0,
            "failed": 0,
            "stopped_early": False,
            "stop_reason": "",
            "processed_player_ids": 0,
            "skipped_failed_player_ids": [],
            "results": [],
        }

        while True:
            target_ids = self._fetch_player_ids(
                limit=batch_limit,
                exclude_ids=processed_ids | skipped_failed_ids,
            )
            if not target_ids:
                break

            batch_summary = self.run(limit=batch_limit, player_ids=target_ids)
            summary["batches"] = int(summary["batches"]) + 1
            summary["requested"] = int(summary["requested"]) + int(batch_summary.get("requested", 0))
            summary["processed"] = int(summary["processed"]) + int(batch_summary.get("processed", 0))
            summary["updated"] = int(summary["updated"]) + int(batch_summary.get("updated", 0))
            summary["removed"] = int(summary["removed"]) + int(batch_summary.get("removed", 0))
            summary["unchanged"] = int(summary["unchanged"]) + int(batch_summary.get("unchanged", 0))
            summary["failed"] = int(summary["failed"]) + int(batch_summary.get("failed", 0))
            if not batch_summary.get("ok"):
                summary["ok"] = False

            batch_results = list(batch_summary.get("results") or [])
            summary["results"].append({
                "batch": int(summary["batches"]),
                "requested": int(batch_summary.get("requested", 0)),
                "processed": int(batch_summary.get("processed", 0)),
                "updated": int(batch_summary.get("updated", 0)),
                "removed": int(batch_summary.get("removed", 0)),
                "unchanged": int(batch_summary.get("unchanged", 0)),
                "failed": int(batch_summary.get("failed", 0)),
                "results": batch_results,
            })

            batch_processed = int(batch_summary.get("processed", 0))
            batch_failed = int(batch_summary.get("failed", 0))
            batch_success = max(0, batch_processed - batch_failed)
            print(
                f"[batch {int(summary['batches'])}] {batch_success} of {batch_processed} completed successfully.",
                file=sys.stderr,
                flush=True,
            )

            for item in batch_results:
                player_id = str(item.get("player_id") or "").strip()
                if player_id:
                    processed_ids.add(player_id)
                if item.get("ok"):
                    consecutive_failed_players = 0
                    continue

                consecutive_failed_players += 1
                if player_id:
                    skipped_failed_ids.add(player_id)

                if consecutive_failed_players >= failure_limit:
                    summary["ok"] = False
                    summary["stopped_early"] = True
                    summary["stop_reason"] = (
                        f"Stopped after {consecutive_failed_players} consecutive failed players."
                    )
                    summary["processed_player_ids"] = len(processed_ids)
                    summary["skipped_failed_player_ids"] = sorted(skipped_failed_ids)
                    print(
                        f"[batch {int(summary['batches'])}] stopped early: {summary['stop_reason']}",
                        file=sys.stderr,
                        flush=True,
                    )
                    return summary

            summary["processed_player_ids"] = len(processed_ids)
            summary["skipped_failed_player_ids"] = sorted(skipped_failed_ids)

        print(
            (
                f"[done] processed {int(summary['processed'])} player(s) in {int(summary['batches'])} batch(es); "
                f"updated={int(summary['updated'])}, removed={int(summary['removed'])}, "
                f"unchanged={int(summary['unchanged'])}, failed={int(summary['failed'])}"
            ),
            file=sys.stderr,
            flush=True,
        )
        return summary

    def _fetch_player_ids(self, *, limit: int, exclude_ids: set[str] | None = None) -> list[str]:
        where_parts = [
            "deleted_at IS NULL",
            "trim(COALESCE(id, '')) <> ''",
            "trim(COALESCE(bga_nickname, '')) <> ''",
            "trim(COALESCE(id, '')) GLOB '[0-9]*'",
        ]
        params: list[object] = []
        normalized_exclude_ids = sorted({
            str(player_id).strip()
            for player_id in (exclude_ids or set())
            if str(player_id).strip()
        })

        if not self.include_removed:
            where_parts.append("COALESCE(NULLIF(trim(status), ''), 'Active') <> 'Removed'")
        if normalized_exclude_ids:
            placeholders = ", ".join("?" for _ in normalized_exclude_ids)
            where_parts.append(f"trim(id) NOT IN ({placeholders})")
            params.extend(normalized_exclude_ids)

        params.append(int(limit))
        sql = f"""
            SELECT trim(id) AS player_id
            FROM profiles
            WHERE {' AND '.join(where_parts)}
            ORDER BY
              CASE WHEN updated_at IS NULL OR trim(updated_at) = '' THEN 0 ELSE 1 END ASC,
              datetime(COALESCE(updated_at, '1970-01-01 00:00:00')) ASC,
              rowid ASC
            LIMIT ?
        """

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()

        return [str(row["player_id"]).strip() for row in rows if str(row["player_id"]).strip()]
