from __future__ import annotations
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from .config import (
    DEBUG_LOG,
    HTTP_WORKERS,
    REQUEST_TIMEOUT_SECONDS,
    empty_tables_batch_failure_threshold,
)
from .http_session import (
    _throttle,
    current_account_label,
    get_http_session,
    request_json,
    rotate_http_session,
    snapshot_session,
)
from .models import MatchTable, MatchUpdateRequest, MatchUpdateResult
from .player_id import get_player_id

EMPTY_TABLES_RETRY_MESSAGE = "BGA returned empty tables; duel left unchanged for retry"
EMPTY_TABLES_ROTATION_MESSAGE = "BGA returned empty tables after account rotation; keeping duel pending for retry"
EMPTY_TABLES_BATCH_MESSAGE = "Mass empty tables anomaly from BGA; skipped entire batch for retry"


def get_games_batch(batch: list[MatchUpdateRequest]) -> list[MatchUpdateResult]:
    try:
        start = time.time()
        results = fetch_games(batch)
        duration = time.time() - start
        if duration > 30:
            print(f"⚠️ fetch_games took {duration:.1f}s (>30s)", flush=True)
        return results
    except Exception as exc:
        print(f"🔥 Fatal error in get_games_batch(): {exc}", flush=True)
        traceback.print_exc()
        return [MatchUpdateResult(status="error", message=str(exc))]


def _make_session(cookies: dict, headers: dict) -> requests.Session:
    session = requests.Session()
    session.headers.update(headers)
    session.cookies.update(cookies)
    return session


def _unwrap_payload(payload: object) -> dict | None:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = payload["data"]
    return payload if isinstance(payload, dict) else None


def _payload_tables(payload: object) -> list:
    normalized = _unwrap_payload(payload)
    if normalized is None:
        return []
    tables = normalized.get("tables", [])
    return tables if isinstance(tables, list) else []


def _is_empty_tables_payload(payload: object) -> bool:
    normalized = _unwrap_payload(payload)
    if normalized is None:
        return False
    tables = normalized.get("tables", [])
    return isinstance(tables, list) and len(tables) == 0


def _should_retry_empty_tables(item: MatchUpdateRequest) -> bool:
    return item.target in {"finished_pending", "empty_finished", "manual", "manual_duel", "ongoing"}


def fetch_games(batch: list[MatchUpdateRequest]) -> list[MatchUpdateResult]:
    prepared: list[MatchUpdateRequest] = []
    for item in batch:
        end_date_bga = int(item.end_date) if item.end_date is not None else int(item.start_date) + 86400
        if item.stat and item.end_date is None:
            end_date_bga = int(item.start_date) + 86400

        if item.player0_id is None:
            item.player0_id = get_player_id(item.player0)
        if item.player1_id is None:
            item.player1_id = get_player_id(item.player1)

        item.start_date = int(item.start_date)
        item.gtw = int(item.gtw or 2)
        item.end_date = end_date_bga
        prepared.append(item)

    print(f"🎯 Fetching {len(prepared)} matchups in batch...", flush=True)
    if DEBUG_LOG and prepared:
        print(f"🧪 Batch preview: {prepared[0]}", flush=True)

    cookies, headers, token = snapshot_session()

    def _fetch_item(item: MatchUpdateRequest) -> dict:
        def _run_request(session_cookies: dict, session_headers: dict, request_token: str) -> dict:
            session = _make_session(session_cookies, session_headers)
            finished_flag = 0
            try:
                finished_flag = int(item.extra.get("finished", 0))
            except Exception:
                finished_flag = 0
            params = {
                "game_id": item.game_id,
                "player": item.player0_id,
                "opponent_id": item.player1_id,
                "start_date": item.start_date,
                "end_date": item.end_date,
                "finished": finished_flag,
                "updateStats": 1,
            }
            request_headers = {
                "X-Requested-With": "XMLHttpRequest",
                "X-Request-Token": request_token,
            }
            _throttle()
            response = session.get(
                "https://boardgamearena.com/gamestats/gamestats/getGames.html",
                params=params,
                headers=request_headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code in (401, 403):
                get_http_session(force_refresh=True)
                new_cookies, new_headers, new_token = snapshot_session()
                session = _make_session(new_cookies, new_headers)
                request_headers["X-Request-Token"] = new_token
                _throttle()
                response = session.get(
                    "https://boardgamearena.com/gamestats/gamestats/getGames.html",
                    params=params,
                    headers=request_headers,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            response.raise_for_status()
            return response.json()

        payload = _run_request(cookies, headers, token)
        if _should_retry_empty_tables(item) and _is_empty_tables_payload(payload):
            active_account = current_account_label() or "unknown"
            print(
                f"⚠️ Empty tables from BGA for duel {item.match_id}; rotating account "
                f"(target={item.target}, account={active_account})",
                flush=True,
            )
            rotate_http_session(reason=f"empty_tables_duel_{item.match_id}")
            retry_cookies, retry_headers, retry_token = snapshot_session()
            retry_payload = _run_request(retry_cookies, retry_headers, retry_token)
            if _is_empty_tables_payload(retry_payload):
                return {
                    "status": "error",
                    "message": EMPTY_TABLES_ROTATION_MESSAGE,
                }
            payload = retry_payload

        return {"status": "success", "data": payload}

    results: list[dict | None] = [None] * len(prepared)
    with ThreadPoolExecutor(max_workers=min(HTTP_WORKERS, max(1, len(prepared)))) as executor:
        future_to_index = {executor.submit(_fetch_item, item): i for i, item in enumerate(prepared)}
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                results[index] = future.result()
            except Exception as exc:
                results[index] = {"status": "error", "message": str(exc)}

    enriched_batch: list[MatchUpdateResult] = []

    for i, result in enumerate(results):
        item = prepared[i]
        wins0 = 0
        wins1 = 0

        if result is None or result.get("status") != "success":
            message = "Unknown error" if result is None else result.get("message", "Unknown error")
            enriched_batch.append(MatchUpdateResult(status="error", message=message))
            continue

        payload = _unwrap_payload(result.get("data", {}))
        if not isinstance(payload, dict):
            enriched_batch.append(MatchUpdateResult(status="error", message="Unexpected payload format"))
            continue

        tables = _payload_tables(payload)
        if tables is None:
            enriched_batch.append(MatchUpdateResult(status="error", message="Missing tables in payload"))
            continue
        if len(tables) == 0:
            enriched_batch.append(
                MatchUpdateResult(
                    status="error",
                    message=EMPTY_TABLES_RETRY_MESSAGE,
                )
            )
            continue

        enriched_tables: list[MatchTable] = []

        def _ts(table: dict, key: str) -> int:
            try:
                return int(table.get(key, 0) or 0)
            except Exception:
                return 0

        for table in sorted(tables, key=lambda entry: (_ts(entry, "end"), _ts(entry, "start"))):
            try:
                table_id = table.get("table_id")
                if table.get("scores") is None:
                    continue

                table_end = int(table.get("end", 0))
                if table_end < item.start_date:
                    continue

                players = table["players"].split(",")
                index0 = 0 if str(players[0]) == str(item.player0_id) else 1
                index1 = 1 - index0

                scores = table.get("scores", "?").split(",")
                ranks = table.get("ranks", "?").split(",")
                score0 = scores[index0] if len(scores) > index0 else "?"
                score1 = scores[index1] if len(scores) > index1 else "?"
                rank0 = ranks[index0] if len(ranks) > index0 else "?"
                rank1 = ranks[index1] if len(ranks) > index1 else "?"
                if rank0 == "1" and rank1 == "2":
                    rank1 = "0"
                elif rank0 == "2" and rank1 == "1":
                    rank0 = "0"

                table_status = "Finished"
                if table.get("concede") == "1":
                    table_status = "Conceded"

                penalties_payload = request_json("/table/table/tableinfos.html", params={"id": table_id})
                penalties = penalties_payload.get("data", {}).get("result", {}).get("penalties", {})

                def has_active_clock_penalty(player_penalty: dict[str, object]) -> int:
                    clock = str(player_penalty.get("clock", ""))
                    clock_cancelled = str(player_penalty.get("clock_cancelled", ""))
                    return 1 if clock == "1" and clock_cancelled != "1" else 0

                clock0 = has_active_clock_penalty(penalties.get(str(item.player0_id), {}))
                clock1 = has_active_clock_penalty(penalties.get(str(item.player1_id), {}))

                try:
                    if clock0 and float(score0) > float(score1):
                        rank0, rank1 = "0", "1"
                    if clock1 and float(score1) > float(score0):
                        rank0, rank1 = "1", "0"
                except Exception as exc:
                    print(f"⚠️ Failed to compare scores in table {table_id}: {exc}", flush=True)

                enriched_tables.append(
                    MatchTable(
                        id=table_id,
                        url=f"https://boardgamearena.com/table?table={table_id}",
                        score0=score0,
                        score1=score1,
                        rank0=rank0,
                        rank1=rank1,
                        timestamp=int(table["start"]),
                        player0_clock=clock0,
                        player1_clock=clock1,
                        status=table_status,
                    )
                )

                if rank0 == "1":
                    wins0 += 1
                elif rank1 == "1":
                    wins1 += 1

                if not item.stat and (wins0 >= item.gtw or wins1 >= item.gtw):
                    break

            except Exception as exc:
                print(f"❌ Table parse error in batch {i}: {exc}", flush=True)
                traceback.print_exc()

        players_url = (
            "https://boardgamearena.com/gamestats"
            f"?player={item.player0_id}&opponent_id={item.player1_id}&game_id={item.game_id}"
            f"&start_date={item.start_date}&end_date={item.end_date}"
        )

        enriched_batch.append(
            MatchUpdateResult(
                status="success",
                player0_id=item.player0_id,
                player1_id=item.player1_id,
                wins0=wins0,
                wins1=wins1,
                players_url=players_url,
                tables=enriched_tables,
            )
        )

    empty_tables_failures = sum(
        1
        for result in enriched_batch
        if result.status == "error"
        and str(result.message or "") in {EMPTY_TABLES_RETRY_MESSAGE, EMPTY_TABLES_ROTATION_MESSAGE}
    )
    batch_failure_threshold = empty_tables_batch_failure_threshold(len(prepared))
    if empty_tables_failures >= batch_failure_threshold:
        print(
            f"🚨 Mass empty tables anomaly detected: {empty_tables_failures}/{len(prepared)} "
            f"(threshold={batch_failure_threshold}). Skipping all updates in this batch.",
            flush=True,
        )
        return [
            MatchUpdateResult(status="error", message=EMPTY_TABLES_BATCH_MESSAGE)
            for _ in prepared
        ]

    return enriched_batch
