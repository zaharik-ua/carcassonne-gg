from __future__ import annotations

import fcntl
import os
import socket
import sqlite3
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .game_replay import (
    COLOR_SOURCE_BGA,
    COLOR_SOURCE_FALLBACK,
    ArchiveMissingError,
    GameReplayError,
    ReplayAccessError,
    ReplayBudgetExceededError,
    ReplayLimitError,
    TemporaryReplayError,
    ensure_game_replays_schema,
    fetch_and_store_game_replay,
)


WORKER_MODES = frozenset({"fresh", "historical", "archive-follow-up"})
RETRY_REASONS = frozenset({"initial", "archive", "colors"})
DEFAULT_RUN_LIMIT = 3
MAX_RUN_LIMIT = 3
DEFAULT_LEASE_SECONDS = 300
COLOR_RETRY_DELAY_MINUTES = 15
TEMPORARY_RETRY_DELAY_MINUTES = 15
DEFAULT_ARCHIVE_RETRY_DELAY_MINUTES = 2
MAX_ARCHIVE_RETRY_DELAY_MINUTES = 60
ARCHIVE_RETRY_DELAY_SETTING_KEY = "bga_replay_archive_retry_minutes"

ReplayFetcher = Callable[..., dict[str, Any]]


class ReplayWorkerAlreadyRunningError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReplayQueueJob:
    game_id: str
    bga_table_id: str
    queue_class: str
    retry_reason: str
    available_status: str
    color_source: str | None
    color_refresh_count: int
    archive_requested_at: str | None
    lease_owner: str


def enqueue_historical_game_replay(
    db_path: str | Path,
    game_id: str,
    *,
    historical_batch_id: str | None = None,
    force: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    normalized_game_id = str(game_id or "").strip()
    if not normalized_game_id:
        raise ValueError("games.id must not be empty")
    queued_at = _format_timestamp(now or datetime.now(timezone.utc))
    path = Path(db_path).expanduser()

    with sqlite3.connect(path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        ensure_game_replays_schema(conn)
        conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        try:
            game = conn.execute(
                """
                SELECT id, bga_table_id
                FROM games
                WHERE id = ? AND trim(COALESCE(deleted_at, '')) = ''
                LIMIT 1
                """,
                (normalized_game_id,),
            ).fetchone()
            if game is None:
                raise ValueError(f"Game not found: games.id={normalized_game_id}")
            table_id = str(game["bga_table_id"] or "").strip()
            if not table_id or not table_id.isdigit():
                raise ValueError(
                    f"Game {normalized_game_id} has invalid bga_table_id={table_id!r}"
                )

            replay = conn.execute(
                """
                SELECT *
                FROM game_replays
                WHERE game_id = ?
                """,
                (normalized_game_id,),
            ).fetchone()

            if replay is None:
                conn.execute(
                    """
                    INSERT INTO game_replays (
                      game_id, bga_table_id, status, retry_reason, queue_class,
                      queued_at, next_attempt_at, historical_batch_id,
                      history_request_count, color_refresh_count, updated_at
                    )
                    VALUES (?, ?, 'pending', 'initial', 'historical', ?, ?, ?, 0, 0, ?)
                    """,
                    (
                        normalized_game_id,
                        table_id,
                        queued_at,
                        queued_at,
                        _optional_text(historical_batch_id),
                        queued_at,
                    ),
                )
                action = "queued"
                retry_reason = "initial"
                status = "pending"
            else:
                status = str(replay["status"] or "pending")
                color_source = _optional_text(replay["color_source"])
                retry_reason = _optional_text(replay["retry_reason"])
                queue_class = str(replay["queue_class"] or "fresh")
                color_refresh_count = int(replay["color_refresh_count"] or 0)

                if status == "ready" and color_source == COLOR_SOURCE_BGA and not force:
                    action = "ready_bga"
                elif queue_class == "fresh" and retry_reason and not force:
                    action = "already_queued_fresh"
                elif queue_class == "historical" and retry_reason and not force:
                    action = "already_queued"
                elif (
                    status == "ready"
                    and color_source == COLOR_SOURCE_FALLBACK
                    and color_refresh_count >= 1
                    and not force
                ):
                    action = "manual_required"
                else:
                    has_ready_replay = status == "ready"
                    retry_reason = "colors" if has_ready_replay else "initial"
                    next_status = "ready" if has_ready_replay else "pending"
                    conn.execute(
                        """
                        UPDATE game_replays
                        SET bga_table_id = ?,
                            status = ?,
                            retry_reason = ?,
                            queue_class = 'historical',
                            queued_at = ?,
                            next_attempt_at = ?,
                            historical_batch_id = ?,
                            history_request_count = CASE WHEN ? THEN 0 ELSE history_request_count END,
                            color_refresh_count = CASE WHEN ? THEN 0 ELSE color_refresh_count END,
                            archive_requested_at = CASE WHEN ? THEN NULL ELSE archive_requested_at END,
                            lease_owner = NULL,
                            lease_until = NULL,
                            last_error = CASE WHEN ? THEN NULL ELSE last_error END,
                            updated_at = ?
                        WHERE game_id = ?
                        """,
                        (
                            table_id,
                            next_status,
                            retry_reason,
                            queued_at,
                            queued_at,
                            _optional_text(historical_batch_id),
                            int(force),
                            int(force),
                            int(force),
                            int(force),
                            queued_at,
                            normalized_game_id,
                        ),
                    )
                    action = "queued"
                    status = next_status

            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return {
        "game_id": normalized_game_id,
        "bga_table_id": table_id,
        "status": status,
        "retry_reason": retry_reason,
        "queue_class": "historical" if action == "queued" else (
            str(replay["queue_class"] or "fresh") if replay is not None else "historical"
        ),
        "action": action,
    }


def run_replay_worker(
    db_path: str | Path,
    *,
    queue_class: str,
    limit: int = DEFAULT_RUN_LIMIT,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    lease_owner: str | None = None,
    account_labels: list[str] | None = None,
    replay_fetcher: ReplayFetcher = fetch_and_store_game_replay,
    now: datetime | None = None,
) -> dict[str, Any]:
    worker_mode = _normalize_worker_mode(queue_class)
    handles_historical = worker_mode in {"historical", "archive-follow-up"}
    if not 1 <= int(limit) <= MAX_RUN_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_RUN_LIMIT}")
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")

    path = Path(db_path).expanduser()
    run_now = _as_utc(now or datetime.now(timezone.utc))
    owner = lease_owner or _default_lease_owner()
    (
        configured_accounts,
        standby_accounts,
        standby_guard_accounts,
    ) = _resolve_account_configuration(account_labels)
    historical_accounts_used: set[str] = set()
    processed_game_ids: set[str] = set()

    summary: dict[str, Any] = {
        "status": "ok",
        "queue_class": worker_mode,
        "due": count_due_replays(path, queue_class=worker_mode, now=run_now),
        "processed": 0,
        "requests": 0,
        "ready_bga_colors": 0,
        "ready_fallback_colors": 0,
        "archives_requested": 0,
        "deferred": 0,
        "manual_required": 0,
        "stopped_by_budget": False,
        "stopped_by_bga_limit": False,
        "remaining": 0,
        "errors": [],
    }

    while summary["processed"] < int(limit):
        if handles_historical and has_due_fresh_replay(
            path, now=run_now
        ):
            _stop_summary(summary, "fresh_priority")
            break

        available_accounts = configured_accounts
        if handles_historical:
            available_accounts = [
                label
                for label in configured_accounts
                if label not in historical_accounts_used
            ]
            if not available_accounts:
                _stop_summary(summary, "historical_accounts_exhausted")
                break

        job = _claim_due_replay(
            path,
            worker_mode=worker_mode,
            lease_owner=owner,
            lease_seconds=lease_seconds,
            excluded_game_ids=processed_game_ids,
            now=run_now,
        )
        if job is None:
            break

        processed_game_ids.add(job.game_id)
        summary["processed"] += 1

        if _finish_recovered_job(path, job=job, now=run_now, summary=summary):
            continue

        if handles_historical and has_due_fresh_replay(
            path, now=run_now
        ):
            _release_job(path, job=job, now=run_now, keep_due=True)
            _stop_summary(summary, "fresh_priority")
            break

        try:
            result = replay_fetcher(
                str(path),
                job.game_id,
                force=job.retry_reason == "colors",
                request_class=job.queue_class,
                account_labels=available_accounts,
                standby_account_labels=standby_accounts,
                standby_guard_account_labels=standby_guard_accounts,
                color_refresh=job.retry_reason == "colors",
                preserve_existing_fallback=job.retry_reason == "colors",
            )
            summary["requests"] += 1
            account_label = _optional_text(result.get("account_label"))
            if job.queue_class == "historical" and account_label:
                historical_accounts_used.add(account_label)
            _complete_successful_job(
                path,
                job=job,
                color_source=str(result.get("color_source") or COLOR_SOURCE_FALLBACK),
                now=run_now,
                summary=summary,
            )
        except ReplayBudgetExceededError as exc:
            _release_job(path, job=job, now=run_now, keep_due=True)
            summary["stopped_by_budget"] = True
            summary["errors"].append(_error_summary(job, exc))
            _stop_summary(summary, "budget")
            break
        except ReplayLimitError as exc:
            summary["requests"] += 1
            _remember_historical_account(path, job, historical_accounts_used)
            _release_job(path, job=job, now=run_now, keep_due=True)
            summary["stopped_by_bga_limit"] = True
            summary["errors"].append(_error_summary(job, exc))
            _stop_summary(summary, "replay_limit")
            break
        except ArchiveMissingError as exc:
            summary["requests"] += 1
            _remember_historical_account(path, job, historical_accounts_used)
            if (
                job.retry_reason in {"initial", "colors"}
                and exc.archive_requested
            ):
                _defer_archive_job(path, job=job, now=run_now, error=str(exc))
                summary["archives_requested"] += 1
                summary["deferred"] += 1
            else:
                _mark_job_manual_required(path, job=job, now=run_now, error=str(exc))
                summary["manual_required"] += 1
            summary["errors"].append(_error_summary(job, exc))
        except ReplayAccessError as exc:
            summary["requests"] += 1
            _remember_historical_account(path, job, historical_accounts_used)
            _mark_job_manual_required(path, job=job, now=run_now, error=str(exc))
            summary["manual_required"] += 1
            summary["errors"].append(_error_summary(job, exc))
        except TemporaryReplayError as exc:
            summary["requests"] += 1
            _remember_historical_account(path, job, historical_accounts_used)
            _defer_same_reason(path, job=job, now=run_now, error=str(exc))
            summary["deferred"] += 1
            summary["errors"].append(_error_summary(job, exc))
        except GameReplayError as exc:
            summary["requests"] += 1
            _remember_historical_account(path, job, historical_accounts_used)
            _mark_job_manual_required(path, job=job, now=run_now, error=str(exc))
            summary["manual_required"] += 1
            summary["errors"].append(_error_summary(job, exc))
        except Exception as exc:
            summary["requests"] += 1
            _remember_historical_account(path, job, historical_accounts_used)
            _defer_same_reason(path, job=job, now=run_now, error=str(exc))
            summary["deferred"] += 1
            summary["errors"].append(_error_summary(job, exc))

    summary["remaining"] = count_due_replays(
        path,
        queue_class=worker_mode,
        now=run_now,
    )
    return summary


def count_due_replays(
    db_path: str | Path,
    *,
    queue_class: str,
    now: datetime | None = None,
) -> int:
    worker_mode = _normalize_worker_mode(queue_class)
    stored_queue_class = (
        "historical" if worker_mode == "archive-follow-up" else worker_mode
    )
    archive_follow_up_sql = (
        "AND gr.archive_requested_at IS NOT NULL"
        if worker_mode == "archive-follow-up"
        else ""
    )
    at = _format_timestamp(now or datetime.now(timezone.utc))
    with sqlite3.connect(Path(db_path).expanduser()) as conn:
        ensure_game_replays_schema(conn)
        return int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM game_replays gr
                JOIN games g ON g.id = gr.game_id
                WHERE gr.queue_class = ?
                  AND gr.retry_reason IN ('initial', 'archive', 'colors')
                  {archive_follow_up_sql}
                  AND gr.next_attempt_at IS NOT NULL
                  AND datetime(gr.next_attempt_at) <= datetime(?)
                  AND gr.status IN ('pending', 'ready', 'fetching', 'error')
                  AND trim(COALESCE(g.deleted_at, '')) = ''
                """,
                (stored_queue_class, at),
            ).fetchone()[0]
        )


def has_due_fresh_replay(
    db_path: str | Path,
    *,
    now: datetime | None = None,
) -> bool:
    return count_due_replays(db_path, queue_class="fresh", now=now) > 0


@contextmanager
def replay_worker_lock(lock_path: str | Path) -> Iterator[None]:
    path = Path(lock_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ReplayWorkerAlreadyRunningError(
                f"Replay worker lock is already held: {path}"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _claim_due_replay(
    db_path: Path,
    *,
    worker_mode: str,
    lease_owner: str,
    lease_seconds: int,
    excluded_game_ids: set[str],
    now: datetime,
) -> ReplayQueueJob | None:
    at = _format_timestamp(now)
    lease_until = _format_timestamp(now + timedelta(seconds=lease_seconds))
    stored_queue_class = (
        "historical" if worker_mode == "archive-follow-up" else worker_mode
    )
    archive_follow_up_sql = (
        "AND gr.archive_requested_at IS NOT NULL"
        if worker_mode == "archive-follow-up"
        else ""
    )
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        ensure_game_replays_schema(conn)
        conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        try:
            excluded_sql = ""
            params: list[Any] = [stored_queue_class, at, at]
            if excluded_game_ids:
                excluded_sql = (
                    "AND gr.game_id NOT IN ("
                    + ",".join("?" for _ in excluded_game_ids)
                    + ")"
                )
                params.extend(sorted(excluded_game_ids))
            row = conn.execute(
                f"""
                SELECT
                  gr.game_id,
                  gr.bga_table_id,
                  gr.status,
                  gr.queue_class,
                  gr.retry_reason,
                  gr.color_source,
                  gr.color_refresh_count,
                  gr.archive_requested_at
                FROM game_replays gr
                JOIN games g ON g.id = gr.game_id
                WHERE gr.queue_class = ?
                  AND gr.retry_reason IN ('initial', 'archive', 'colors')
                  {archive_follow_up_sql}
                  AND gr.next_attempt_at IS NOT NULL
                  AND datetime(gr.next_attempt_at) <= datetime(?)
                  AND gr.status IN ('pending', 'ready', 'fetching', 'error')
                  AND trim(COALESCE(g.deleted_at, '')) = ''
                  AND (
                    gr.lease_owner IS NULL
                    OR trim(gr.lease_owner) = ''
                    OR gr.lease_until IS NULL
                    OR datetime(gr.lease_until) <= datetime(?)
                  )
                  {excluded_sql}
                ORDER BY
                  CASE
                    WHEN gr.retry_reason = 'archive' THEN 0
                    WHEN gr.retry_reason = 'colors'
                      AND gr.archive_requested_at IS NOT NULL THEN 0
                    ELSE 1
                  END,
                  datetime(gr.next_attempt_at),
                  datetime(COALESCE(gr.queued_at, gr.created_at)),
                  gr.game_id
                LIMIT 1
                """,
                params,
            ).fetchone()
            if row is None:
                conn.commit()
                return None

            available_status = (
                "ready"
                if str(row["status"] or "") == "ready"
                or str(row["retry_reason"] or "") == "colors"
                or str(row["color_source"] or "") in {
                    COLOR_SOURCE_BGA,
                    COLOR_SOURCE_FALLBACK,
                }
                else "pending"
            )
            conn.execute(
                """
                UPDATE game_replays
                SET lease_owner = ?,
                    lease_until = ?,
                    status = CASE WHEN ? = 'ready' THEN 'ready' ELSE 'fetching' END,
                    updated_at = ?
                WHERE game_id = ?
                """,
                (lease_owner, lease_until, available_status, at, row["game_id"]),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return ReplayQueueJob(
        game_id=str(row["game_id"]),
        bga_table_id=str(row["bga_table_id"]),
        queue_class=str(row["queue_class"]),
        retry_reason=str(row["retry_reason"]),
        available_status=available_status,
        color_source=_optional_text(row["color_source"]),
        color_refresh_count=int(row["color_refresh_count"] or 0),
        archive_requested_at=_optional_text(row["archive_requested_at"]),
        lease_owner=lease_owner,
    )


def _finish_recovered_job(
    db_path: Path,
    *,
    job: ReplayQueueJob,
    now: datetime,
    summary: dict[str, Any],
) -> bool:
    if job.retry_reason in {"initial", "archive"} and job.color_source in {
        COLOR_SOURCE_BGA,
        COLOR_SOURCE_FALLBACK,
    }:
        _complete_successful_job(
            db_path,
            job=job,
            color_source=job.color_source,
            now=now,
            summary=summary,
        )
        return True
    if job.retry_reason == "colors" and (
        job.color_source == COLOR_SOURCE_BGA or job.color_refresh_count >= 1
    ):
        _complete_successful_job(
            db_path,
            job=job,
            color_source=job.color_source or COLOR_SOURCE_FALLBACK,
            now=now,
            summary=summary,
        )
        return True
    return False


def _complete_successful_job(
    db_path: Path,
    *,
    job: ReplayQueueJob,
    color_source: str,
    now: datetime,
    summary: dict[str, Any],
) -> None:
    at = _format_timestamp(now)
    normalized_color_source = (
        COLOR_SOURCE_BGA
        if color_source == COLOR_SOURCE_BGA
        else COLOR_SOURCE_FALLBACK
    )
    schedule_color_refresh = (
        normalized_color_source == COLOR_SOURCE_FALLBACK
        and job.retry_reason != "colors"
    )
    next_attempt_at = (
        _format_timestamp(now + timedelta(minutes=COLOR_RETRY_DELAY_MINUTES))
        if schedule_color_refresh
        else None
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE game_replays
            SET status = 'ready',
                color_source = ?,
                color_refresh_count = CASE
                  WHEN ? = 'colors' THEN MAX(color_refresh_count, 1)
                  ELSE color_refresh_count
                END,
                retry_reason = ?,
                next_attempt_at = ?,
                lease_owner = NULL,
                lease_until = NULL,
                last_error = NULL,
                updated_at = ?
            WHERE game_id = ? AND lease_owner = ?
            """,
            (
                normalized_color_source,
                job.retry_reason,
                "colors" if schedule_color_refresh else None,
                next_attempt_at,
                at,
                job.game_id,
                job.lease_owner,
            ),
        )
        conn.commit()
    if normalized_color_source == COLOR_SOURCE_BGA:
        summary["ready_bga_colors"] += 1
    else:
        summary["ready_fallback_colors"] += 1
        if schedule_color_refresh:
            summary["deferred"] += 1


def _defer_archive_job(
    db_path: Path,
    *,
    job: ReplayQueueJob,
    now: datetime,
    error: str,
) -> None:
    is_ready_color_refresh = (
        job.retry_reason == "colors" and job.available_status == "ready"
    )
    _update_job_state(
        db_path,
        job=job,
        status="ready" if is_ready_color_refresh else "pending",
        retry_reason="colors" if is_ready_color_refresh else "archive",
        next_attempt_at=now + timedelta(
            minutes=_archive_retry_delay_minutes(db_path)
        ),
        now=now,
        last_error=error,
        mark_archive_requested=True,
    )


def _defer_same_reason(
    db_path: Path,
    *,
    job: ReplayQueueJob,
    now: datetime,
    error: str,
) -> None:
    _update_job_state(
        db_path,
        job=job,
        status=job.available_status,
        retry_reason=job.retry_reason,
        next_attempt_at=now + timedelta(minutes=TEMPORARY_RETRY_DELAY_MINUTES),
        now=now,
        last_error=error,
    )


def _mark_job_manual_required(
    db_path: Path,
    *,
    job: ReplayQueueJob,
    now: datetime,
    error: str,
) -> None:
    _update_job_state(
        db_path,
        job=job,
        status="ready" if job.available_status == "ready" else "error",
        retry_reason=None,
        next_attempt_at=None,
        now=now,
        last_error=error,
    )


def _release_job(
    db_path: Path,
    *,
    job: ReplayQueueJob,
    now: datetime,
    keep_due: bool,
) -> None:
    _update_job_state(
        db_path,
        job=job,
        status=job.available_status,
        retry_reason=job.retry_reason,
        next_attempt_at=now if keep_due else None,
        now=now,
        preserve_last_error=True,
    )


def _update_job_state(
    db_path: Path,
    *,
    job: ReplayQueueJob,
    status: str,
    retry_reason: str | None,
    next_attempt_at: datetime | None,
    now: datetime,
    last_error: str | None = None,
    preserve_last_error: bool = False,
    mark_archive_requested: bool = False,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE game_replays
            SET status = ?,
                retry_reason = ?,
                next_attempt_at = ?,
                lease_owner = NULL,
                lease_until = NULL,
                archive_requested_at = CASE
                  WHEN ? THEN COALESCE(archive_requested_at, ?)
                  ELSE archive_requested_at
                END,
                last_error = CASE WHEN ? THEN last_error ELSE ? END,
                updated_at = ?
            WHERE game_id = ? AND lease_owner = ?
            """,
            (
                status,
                retry_reason,
                _format_timestamp(next_attempt_at) if next_attempt_at else None,
                int(mark_archive_requested),
                _format_timestamp(now),
                int(preserve_last_error),
                last_error,
                _format_timestamp(now),
                job.game_id,
                job.lease_owner,
            ),
        )
        conn.commit()


def _remember_historical_account(
    db_path: Path,
    job: ReplayQueueJob,
    used: set[str],
) -> None:
    if job.queue_class != "historical":
        return
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT last_account_label FROM game_replays WHERE game_id = ?",
            (job.game_id,),
        ).fetchone()
    if row and _optional_text(row[0]):
        used.add(str(row[0]))


def _error_summary(job: ReplayQueueJob, exc: Exception) -> dict[str, str]:
    return {
        "game_id": job.game_id,
        "bga_table_id": job.bga_table_id,
        "retry_reason": job.retry_reason,
        "error": str(exc) or exc.__class__.__name__,
    }


def _stop_summary(summary: dict[str, Any], reason: str) -> None:
    summary["status"] = "stopped"
    summary["stop_reason"] = reason


def _resolve_account_configuration(
    account_labels: list[str] | None,
) -> tuple[list[str], list[str], list[str]]:
    if account_labels is not None:
        labels = [str(label).strip() for label in account_labels if str(label).strip()]
        return labels, [], []
    from .bga_login import get_bga_credentials

    credentials = get_bga_credentials()
    labels = [credential.label for credential in credentials]
    standby = [credential.label for credential in credentials if credential.replay_standby]
    guards = [credential.label for credential in credentials if not credential.replay_standby]
    return labels, standby, guards


def _normalize_worker_mode(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in WORKER_MODES:
        raise ValueError(
            f"queue_class must be one of {', '.join(sorted(WORKER_MODES))}"
        )
    return normalized


def _default_lease_owner() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _archive_retry_delay_minutes(db_path: Path) -> int:
    try:
        with sqlite3.connect(db_path, timeout=5) as conn:
            row = conn.execute(
                """
                SELECT setting_value
                FROM system_settings
                WHERE setting_key = ?
                LIMIT 1
                """,
                (ARCHIVE_RETRY_DELAY_SETTING_KEY,),
            ).fetchone()
    except sqlite3.Error:
        return DEFAULT_ARCHIVE_RETRY_DELAY_MINUTES

    raw_value = row[0] if row else None
    try:
        value = int(str(raw_value).strip())
    except (TypeError, ValueError):
        return DEFAULT_ARCHIVE_RETRY_DELAY_MINUTES
    if not 1 <= value <= MAX_ARCHIVE_RETRY_DELAY_MINUTES:
        return DEFAULT_ARCHIVE_RETRY_DELAY_MINUTES
    return value


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return _as_utc(value).strftime("%Y-%m-%d %H:%M:%S.%f")
