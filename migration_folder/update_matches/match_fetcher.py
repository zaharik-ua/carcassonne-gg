from __future__ import annotations

import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from .config import DEBUG_LOG, HTTP_WORKERS, REQUEST_TIMEOUT_SECONDS
from .http_session import _throttle, get_http_session, request_json, snapshot_session
from .models import MatchTable, MatchUpdateRequest, MatchUpdateResult
from .player_id import get_player_id


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
    sess = requests.Session()
    sess.headers.update(headers)
    sess.cookies.update(cookies)
    return sess


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
    if DEBUG_LOG:
        print(f"🧪 Batch preview: {prepared[0] if prepared else None}", flush=True)

    cookies, headers, token = snapshot_session()

    def _fetch_item(item: MatchUpdateRequest) -> dict:
        sess = _make_session(cookies, headers)
        params = {
            "game_id": item.game_id,
            "player": item.player0_id,
            "opponent_id": item.player1_id,
            "start_date": item.start_date,
            "end_date": item.end_date,
            "finished": 0,
            "updateStats": 1,
        }
        request_headers = {
            "X-Requested-With": "XMLHttpRequest",
            "X-Request-Token": token,
        }
        _throttle()
        resp = sess.get(
            "https://boardgamearena.com/gamestats/gamestats/getGames.html",
            params=params,
            headers=request_headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if resp.status_code in (401, 403):
            get_http_session(force_refresh=True)
            new_cookies, new_headers, new_token = snapshot_session()
            sess = _make_session(new_cookies, new_headers)
            request_headers["X-Request-Token"] = new_token
            _throttle()
            resp = sess.get(
                "https://boardgamearena.com/gamestats/gamestats/getGames.html",
                params=params,
                headers=request_headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        resp.raise_for_status()
        return {"status": "success", "data": resp.json()}

    results: list[dict | None] = [None] * len(prepared)
    with ThreadPoolExecutor(max_workers=min(HTTP_WORKERS, max(1, len(prepared)))) as executor:
        future_to_index = {
            executor.submit(_fetch_item, item): i for i, item in enumerate(prepared)
        }
        for future in as_completed(future_to_index):
            i = future_to_index[future]
            try:
                results[i] = future.result()
            except Exception as exc:
                results[i] = {"status": "error", "message": str(exc)}

    enriched_batch: list[MatchUpdateResult] = []

    for i, result in enumerate(results):
        item = prepared[i]
        player0_id = item.player0_id
        player1_id = item.player1_id
        start_date = item.start_date
        gtw = int(item.gtw or 2)
        stat = bool(item.stat)
        wins0 = 0
        wins1 = 0

        if result is None or result.get("status") != "success":
            message = "Unknown error" if result is None else result.get("message", "Unknown error")
            enriched_batch.append(MatchUpdateResult(status="error", message=message))
            continue

        payload = result.get("data", {})
        if isinstance(payload, dict) and "data" in payload and isinstance(payload.get("data"), dict):
            payload = payload.get("data", {})

        if not isinstance(payload, dict):
            enriched_batch.append(MatchUpdateResult(status="error", message="Unexpected payload format"))
            continue

        tables = payload.get("tables", [])
        if tables is None:
            enriched_batch.append(MatchUpdateResult(status="error", message="Missing tables in payload"))
            continue

        enriched_tables: list[MatchTable] = []
        flags = ""

        def _ts(table: dict, key: str) -> int:
            try:
                return int(table.get(key, 0) or 0)
            except Exception:
                return 0

        tables_sorted = sorted(tables, key=lambda t: (_ts(t, "end"), _ts(t, "start")))

        for table in tables_sorted:
            try:
                table_id = table.get("table_id")
                if table.get("scores") is None:
                    continue

                table_end = int(table.get("end", 0))
                if table_end < start_date:
                    continue

                players = table["players"].split(",")
                index0 = 0 if str(players[0]) == str(player0_id) else 1
                index1 = 1 - index0

                scores = table.get("scores", "?").split(",")
                ranks = table.get("ranks", "?").split(",")

                score0 = scores[index0] if len(scores) > index0 else "?"
                score1 = scores[index1] if len(scores) > index1 else "?"
                rank0 = ranks[index0] if len(ranks) > index0 else "?"
                rank1 = ranks[index1] if len(ranks) > index1 else "?"

                if table.get("concede") == "1":
                    flags += " 🏳️"
                if table.get("arena_win"):
                    flags += " 🏟️"

                penalties_payload = request_json(
                    "/table/table/tableinfos.html",
                    params={"id": table_id},
                )
                penalties = penalties_payload.get("data", {}).get("result", {}).get("penalties", {})

                clock0 = penalties.get(str(player0_id), {}).get("clock") == "1"
                clock1 = penalties.get(str(player1_id), {}).get("clock") == "1"
                if clock0 or clock1:
                    flags += " ⌛"

                try:
                    if clock0 and float(score0) > float(score1):
                        rank0, rank1 = "2", "1"
                    if clock1 and float(score1) > float(score0):
                        rank0, rank1 = "1", "2"
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
                    )
                )

                if (wins0 < gtw and wins1 < gtw) or stat:
                    if rank0 == "1":
                        wins0 += 1
                    elif rank1 == "1":
                        wins1 += 1

                if not stat and (wins0 >= gtw or wins1 >= gtw):
                    break

            except Exception as exc:
                print(f"❌ Table parse error in batch {i}: {exc}", flush=True)
                traceback.print_exc()

        players_url = (
            "https://boardgamearena.com/gamestats"
            f"?player={player0_id}&opponent_id={player1_id}&game_id={item.game_id}"
            f"&start_date={start_date}&end_date={item.end_date}"
        )

        enriched_batch.append(
            MatchUpdateResult(
                status="success",
                player0_id=player0_id,
                player1_id=player1_id,
                wins0=wins0,
                wins1=wins1,
                players_url=players_url,
                flags=flags.strip(),
                tables=enriched_tables,
            )
        )

    return enriched_batch
