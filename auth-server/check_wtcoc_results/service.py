from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sync_wtcoc_matches.client import WtcocApiClient

from .sqlite_repository import LocalDuelResult, LocalMatchResult, SqliteWtcocResultsRepository


COMPLETED_TEXT_STATUSES = {
    "tancat",
    "closed",
    "done",
    "finished",
    "finalized",
    "finalitzat",
}


@dataclass(frozen=True)
class ApiMatchResult:
    source: str
    external_match_id: str
    status: str | None
    team_1_name: str | None
    team_2_name: str | None
    dw1: int | None
    dw2: int | None
    gw1: int | None
    gw2: int | None
    duels: list[dict[str, Any]]


class WtcocResultsCheckService:
    def __init__(self, *, repository: SqliteWtcocResultsRepository, client: WtcocApiClient) -> None:
        self.repository = repository
        self.client = client

    def build_report(
        self,
        *,
        tournament_id: str,
        include_playoff: bool = True,
        external_match_id: str | None = None,
    ) -> dict[str, Any]:
        normalized_tournament_id = str(tournament_id or "").strip()
        if not normalized_tournament_id:
            raise ValueError("tournament_id is required")

        responses = [self.client.fetch_calendar()]
        if include_playoff:
            responses.append(self.client.fetch_playoff())

        api_matches: list[ApiMatchResult] = []
        source_summaries: list[dict[str, Any]] = []
        for response in responses:
            payload = response.payload
            raw_matches = payload.get("matches")
            matches = raw_matches if isinstance(raw_matches, list) else []
            filtered_matches: list[dict[str, Any]] = []
            completed_matches = 0
            for match in matches:
                if not isinstance(match, dict):
                    continue
                raw_external_match_id = str(match.get("idMatch") or "").strip()
                if external_match_id and raw_external_match_id != str(external_match_id).strip():
                    continue
                filtered_matches.append(match)
                if _is_completed_api_match(match):
                    completed_matches += 1
                    api_matches.append(_normalize_api_match(source=response.source, match=match))

            source_summaries.append(
                {
                    "source": response.source,
                    "response": str(payload.get("response") or "").strip() or None,
                    "description": payload.get("description"),
                    "matches_count": len(filtered_matches),
                    "completed_matches_count": completed_matches,
                }
            )

        local_matches = self.repository.load_linked_matches(normalized_tournament_id)
        local_duels = self.repository.load_duels_by_match_ids(
            [item.match_id for item in local_matches.values() if item.match_id]
        )

        mismatches: list[dict[str, Any]] = []
        checked_matches = 0
        for api_match in api_matches:
            checked_matches += 1
            mismatch = self._compare_match(
                api_match=api_match,
                local_match=local_matches.get(_build_link_key(api_match.source, api_match.external_match_id)),
                local_duels=local_duels,
            )
            if mismatch is not None:
                mismatches.append(mismatch)

        return {
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "tournament_id": normalized_tournament_id,
            "checked_matches_count": checked_matches,
            "mismatches_count": len(mismatches),
            "sources": source_summaries,
            "mismatches": mismatches,
        }

    def _compare_match(
        self,
        *,
        api_match: ApiMatchResult,
        local_match: LocalMatchResult | None,
        local_duels: dict[str, list[LocalDuelResult]],
    ) -> dict[str, Any] | None:
        header = {
            "source": api_match.source,
            "external_match_id": api_match.external_match_id,
            "teams": [api_match.team_1_name, api_match.team_2_name],
            "api_status": api_match.status,
        }
        if local_match is None:
            return {
                **header,
                "issue": "missing_db_match",
                "api_match": _serialize_scoreline(
                    status=api_match.status,
                    dw1=api_match.dw1,
                    dw2=api_match.dw2,
                    gw1=api_match.gw1,
                    gw2=api_match.gw2,
                ),
            }

        match_differences = _collect_score_differences(
            expected={
                "dw1": api_match.dw1,
                "dw2": api_match.dw2,
                "gw1": api_match.gw1,
                "gw2": api_match.gw2,
            },
            actual={
                "dw1": local_match.dw1,
                "dw2": local_match.dw2,
                "gw1": local_match.gw1,
                "gw2": local_match.gw2,
            },
        )

        duel_rows = local_duels.get(local_match.match_id, [])
        db_duels_by_number = {
            duel.duel_number: duel
            for duel in duel_rows
            if duel.duel_number is not None
        }
        duel_mismatches: list[dict[str, Any]] = []
        seen_duel_numbers: set[int] = set()

        for api_duel in api_match.duels:
            duel_number = _extract_duel_number(api_duel.get("name"))
            expected_dw1 = _to_int_or_none(api_duel.get("localResult"))
            expected_dw2 = _to_int_or_none(api_duel.get("visitorResult"))
            if duel_number is None:
                continue
            if not _has_named_players(api_duel):
                continue
            seen_duel_numbers.add(duel_number)
            db_duel = db_duels_by_number.get(duel_number)
            if db_duel is None:
                duel_mismatches.append(
                    {
                        "duel_number": duel_number,
                        "issue": "missing_db_duel",
                        "api_duel": {
                            "name": api_duel.get("name"),
                            "players": [api_duel.get("nameLocalPlayer"), api_duel.get("nameVisitorPlayer")],
                            "status": "Done",
                            "dw1": expected_dw1,
                            "dw2": expected_dw2,
                        },
                    }
                )
                continue

            differences = _collect_score_differences(
                expected={"dw1": expected_dw1, "dw2": expected_dw2},
                actual={"dw1": db_duel.dw1, "dw2": db_duel.dw2},
            )
            if differences:
                duel_mismatches.append(
                    {
                        "duel_number": duel_number,
                        "duel_id": db_duel.id,
                        "issue": "score_mismatch",
                        "players": [api_duel.get("nameLocalPlayer"), api_duel.get("nameVisitorPlayer")],
                        "differences": differences,
                        "api_duel": {"status": "Done", "dw1": expected_dw1, "dw2": expected_dw2},
                        "db_duel": _serialize_scoreline(status=db_duel.status, dw1=db_duel.dw1, dw2=db_duel.dw2),
                    }
                )

        extra_db_duels = [
            {
                "duel_number": duel.duel_number,
                "duel_id": duel.id,
                "db_duel": _serialize_scoreline(status=duel.status, dw1=duel.dw1, dw2=duel.dw2),
            }
            for duel in duel_rows
            if duel.duel_number is not None and duel.duel_number not in seen_duel_numbers
        ]

        if not match_differences and not duel_mismatches and not extra_db_duels:
            return None

        return {
            **header,
            "issue": "result_mismatch",
            "match_id": local_match.match_id,
            "match_differences": match_differences,
            "api_match": _serialize_scoreline(
                status="Done",
                dw1=api_match.dw1,
                dw2=api_match.dw2,
                gw1=api_match.gw1,
                gw2=api_match.gw2,
            ),
            "db_match": _serialize_scoreline(
                status=local_match.status,
                dw1=local_match.dw1,
                dw2=local_match.dw2,
                gw1=local_match.gw1,
                gw2=local_match.gw2,
            ),
            "duel_mismatches": duel_mismatches,
            "extra_db_duels": extra_db_duels,
        }


def _normalize_api_match(*, source: str, match: dict[str, Any]) -> ApiMatchResult:
    duels = match.get("duels")
    duel_rows = duels if isinstance(duels, list) else []
    return ApiMatchResult(
        source=source,
        external_match_id=str(match.get("idMatch") or "").strip(),
        status=str(match.get("status") or "").strip() or None,
        team_1_name=str(match.get("nameLocalTeam") or "").strip() or None,
        team_2_name=str(match.get("nameVisitorTeam") or "").strip() or None,
        dw1=_to_int_or_none(match.get("localResult")),
        dw2=_to_int_or_none(match.get("visitorResult")),
        gw1=sum(_to_int_or_none(duel.get("localResult")) or 0 for duel in duel_rows),
        gw2=sum(_to_int_or_none(duel.get("visitorResult")) or 0 for duel in duel_rows),
        duels=[duel for duel in duel_rows if isinstance(duel, dict)],
    )


def _is_completed_api_match(match: dict[str, Any]) -> bool:
    raw_status = str(match.get("status") or "").strip().lower()
    if raw_status in COMPLETED_TEXT_STATUSES:
        return True

    if not str(match.get("localResult") or "").strip() or not str(match.get("visitorResult") or "").strip():
        return False
    if _has_real_identifier(match.get("idTeamWin")) or _has_real_identifier(match.get("idTeamLost")):
        return True

    duels = match.get("duels")
    if not isinstance(duels, list):
        return False
    return any(
        isinstance(duel, dict)
        and (_has_real_identifier(duel.get("idPlayerWin")) or _has_real_identifier(duel.get("idPlayerLost")))
        for duel in duels
    )


def _collect_score_differences(*, expected: dict[str, Any], actual: dict[str, Any]) -> list[str]:
    differences: list[str] = []
    for field_name, expected_value in expected.items():
        actual_value = actual.get(field_name)
        if actual_value != expected_value:
            differences.append(field_name)
    return differences


def _serialize_scoreline(
    *,
    status: str | None,
    dw1: int | None,
    dw2: int | None,
    gw1: int | None = None,
    gw2: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "dw1": dw1,
        "dw2": dw2,
    }
    if gw1 is not None or gw2 is not None:
        payload["gw1"] = gw1
        payload["gw2"] = gw2
    return payload


def _build_link_key(source: str, external_match_id: str) -> str:
    return f"{str(source or '').strip().lower()}:{str(external_match_id or '').strip()}"


def _extract_duel_number(value: Any) -> int | None:
    raw = str(value or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    return int(digits) if digits else None


def _has_named_players(duel: dict[str, Any]) -> bool:
    local_name = str(duel.get("nameLocalPlayer") or "").strip()
    visitor_name = str(duel.get("nameVisitorPlayer") or "").strip()
    return bool(local_name and visitor_name)


def _has_real_identifier(value: Any) -> bool:
    raw = str(value or "").strip()
    return raw not in {"", "0"}


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None
