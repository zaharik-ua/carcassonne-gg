from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from .game_replay import (
    COLOR_SOURCE_BGA,
    COLOR_SOURCE_FALLBACK,
    ReplayBudgetExceededError,
    ReplayLimitError,
    fetch_and_store_game_replay_with_budget,
)


ReplayFetcher = Callable[..., dict[str, Any]]


def process_manual_replay_games(
    db_path: str | Path,
    games: Sequence,
    *,
    force: bool = False,
    include_pending: bool = False,
    include_errors: bool = False,
    include_fallback: bool = False,
    max_requests: int = 3,
    max_failed_games: int = 1,
    replay_fetcher: ReplayFetcher = fetch_and_store_game_replay_with_budget,
) -> dict[str, Any]:
    if max_requests < 1:
        raise ValueError("max_requests must be at least 1")
    if max_failed_games < 1:
        raise ValueError("max_failed_games must be at least 1")

    path = Path(db_path).expanduser()
    summary: dict[str, Any] = {
        "status": "ok",
        "games_found": len(games),
        "processed": 0,
        "requests": 0,
        "ready": 0,
        "cached": 0,
        "deferred": 0,
        "failed": 0,
        "remaining": 0,
        "errors": [],
    }

    for game_index, game in enumerate(games):
        game_id = _text(game["game_id"])
        duel_id = _text(game["duel_id"])
        bga_table_id = _text(game["bga_table_id"])
        replay_status = _text(game["replay_status"]).lower()
        color_source = _text(game["color_source"]).lower()

        if replay_status == "ready" and color_source == COLOR_SOURCE_BGA and not force:
            summary["processed"] += 1
            summary["ready"] += 1
            summary["cached"] += 1
            continue

        should_request = force or _manual_state_is_included(
            replay_status=replay_status,
            color_source=color_source,
            include_pending=include_pending,
            include_errors=include_errors,
            include_fallback=include_fallback,
        )
        if not should_request:
            summary["deferred"] += 1
            continue

        if summary["requests"] >= max_requests:
            summary["remaining"] = len(games) - game_index
            summary["status"] = "stopped"
            summary["stop_reason"] = "max_requests"
            break

        summary["processed"] += 1
        if not bga_table_id:
            summary["failed"] += 1
            summary["errors"].append(
                {
                    "game_id": game_id,
                    "duel_id": duel_id,
                    "error": "Game has no bga_table_id",
                }
            )
            if summary["failed"] >= max_failed_games:
                summary["remaining"] = len(games) - game_index - 1
                summary["status"] = "stopped"
                summary["stop_reason"] = "failed_games"
                break
            continue

        try:
            result = replay_fetcher(
                str(path),
                game_id,
                force=force or replay_status == "ready",
                request_class="manual",
            )
            summary["requests"] += 1
            summary["ready"] += 1
            if bool(result.get("cached")):
                summary["cached"] += 1
        except ReplayBudgetExceededError as exc:
            summary["errors"].append(_error_row(game_id, duel_id, bga_table_id, exc))
            summary["remaining"] = len(games) - game_index
            summary["status"] = "stopped"
            summary["stop_reason"] = "budget"
            break
        except ReplayLimitError as exc:
            summary["requests"] += 1
            summary["failed"] += 1
            summary["errors"].append(_error_row(game_id, duel_id, bga_table_id, exc))
            summary["remaining"] = len(games) - game_index - 1
            summary["status"] = "stopped"
            summary["stop_reason"] = "replay_limit"
            break
        except Exception as exc:
            summary["requests"] += 1
            summary["failed"] += 1
            summary["errors"].append(_error_row(game_id, duel_id, bga_table_id, exc))
            if summary["failed"] >= max_failed_games:
                summary["remaining"] = len(games) - game_index - 1
                summary["status"] = "stopped"
                summary["stop_reason"] = "failed_games"
                break

    if summary["status"] == "ok" and (summary["failed"] or summary["deferred"]):
        summary["status"] = "partial"
    return summary


def _manual_state_is_included(
    *,
    replay_status: str,
    color_source: str,
    include_pending: bool,
    include_errors: bool,
    include_fallback: bool,
) -> bool:
    if replay_status == "ready":
        return include_fallback and color_source != COLOR_SOURCE_BGA
    if replay_status == "error":
        return include_errors
    return include_pending


def _error_row(
    game_id: str,
    duel_id: str,
    bga_table_id: str,
    exc: Exception,
) -> dict[str, str]:
    return {
        "game_id": game_id,
        "duel_id": duel_id,
        "bga_table_id": bga_table_id,
        "error": str(exc) or exc.__class__.__name__,
    }


def _text(value: object) -> str:
    return str(value or "").strip()
