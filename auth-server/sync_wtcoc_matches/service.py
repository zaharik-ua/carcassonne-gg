from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .client import WtcocApiClient
from .sqlite_repository import SqliteWtcocRepository, TeamMapping


ZERO_DATE = "0000-00-00 00:00:00"
UNCONFIRMED_MATCH_STATUSES = {"", "10", "01", "11", "12", "21"}


@dataclass(frozen=True)
class _AssociationResolver:
    by_code: dict[str, TeamMapping]
    by_name: dict[str, TeamMapping]

    @classmethod
    def from_rows(cls, rows: list[TeamMapping]) -> "_AssociationResolver":
        by_code: dict[str, TeamMapping] = {}
        by_name: dict[str, TeamMapping] = {}
        for row in rows:
            normalized_code = _normalize_token(row.code)
            normalized_name = _normalize_token(row.name)
            if normalized_code:
                by_code[normalized_code] = row
            if normalized_name:
                by_name[normalized_name] = row
        return cls(by_code=by_code, by_name=by_name)

    def resolve(self, raw_value: str) -> AssociationMapping | None:
        normalized = _normalize_token(raw_value)
        if not normalized:
            return None
        return self.by_code.get(normalized) or self.by_name.get(normalized)


class WtcocSyncService:
    def __init__(self, *, repository: SqliteWtcocRepository, client: WtcocApiClient) -> None:
        self.repository = repository
        self.client = client

    def build_apply_payload(
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

        resolver = _AssociationResolver.from_rows(self.repository.load_team_mappings())

        source_summaries: list[dict[str, Any]] = []
        matches_payload: list[dict[str, Any]] = []
        duels_payload: list[dict[str, Any]] = []
        unresolved_matches: list[dict[str, Any]] = []

        for response in responses:
            payload = response.payload
            response_status = str(payload.get("response") or "").strip() or None
            matches = payload.get("matches")
            if not isinstance(matches, list):
                matches = []
            filtered_matches: list[dict[str, Any]] = []
            for match in matches:
                if not isinstance(match, dict):
                    continue
                if external_match_id and str(match.get("idMatch") or "").strip() != str(external_match_id).strip():
                    continue
                filtered_matches.append(match)
            source_summaries.append(
                {
                    "source": response.source,
                    "response": response_status,
                    "description": payload.get("description"),
                    "matches_count": len(filtered_matches),
                }
            )
            for match in filtered_matches:
                if not isinstance(match, dict):
                    continue
                match_preview = self._normalize_match_preview(
                    tournament_id=normalized_tournament_id,
                    source=response.source,
                    match=match,
                    resolver=resolver,
                )
                if not match_preview["team_1"] or not match_preview["team_2"]:
                    unresolved_matches.append(
                        {
                            "match_id": match_preview["match_id"],
                            "team_1_name": match_preview["team_1_name"],
                            "team_2_name": match_preview["team_2_name"],
                        }
                    )
                    continue
                matches_payload.append(
                    {
                        "id": match_preview["match_id"],
                        "tournament_id": normalized_tournament_id,
                        "time_utc": match_preview["time_utc"],
                        "lineup_type": "Open",
                        "lineup_deadline_h": None,
                        "lineup_deadline_utc": None,
                        "number_of_duels": 5,
                        "team_1": match_preview["team_1"],
                        "team_2": match_preview["team_2"],
                    }
                )
                if not _can_import_duels(match_preview["status"]):
                    continue
                duels = match.get("duels")
                if not isinstance(duels, list):
                    duels = []
                for duel in duels:
                    if not isinstance(duel, dict):
                        continue
                    duel_preview = self._normalize_duel_preview(
                        tournament_id=normalized_tournament_id,
                        duel=duel,
                        match_preview=match_preview,
                    )
                    duels_payload.append(
                        {
                            "id": duel_preview["duel_id"],
                            "tournament_id": normalized_tournament_id,
                            "match_id": match_preview["match_id"],
                            "duel_number": duel_preview["duel_number"],
                            "duel_format": "Bo3",
                            "time_utc": match_preview["time_utc"],
                            "custom_time": None,
                            "player_1_id": duel_preview["player_1_wtcoc_id"],
                            "player_2_id": duel_preview["player_2_wtcoc_id"],
                        }
                    )

        return {
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "sources": source_summaries,
            "apply_preview": {
                "matches_ready": len(matches_payload),
                "duels_ready": len(duels_payload),
                "matches_skipped_without_team_mapping": unresolved_matches,
            },
            "apply_payload": {
            "matches": matches_payload,
            "duels": duels_payload,
            },
        }

    def _normalize_match_preview(
        self,
        *,
        tournament_id: str,
        source: str,
        match: dict[str, Any],
        resolver: _AssociationResolver,
    ) -> dict[str, Any]:
        raw_match_id = str(match.get("idMatch") or "").strip()
        match_status = str(match.get("status") or "").strip() or None
        local_team_name = str(match.get("nameLocalTeam") or "").strip()
        visitor_team_name = str(match.get("nameVisitorTeam") or "").strip()
        local_team = resolver.resolve(local_team_name)
        visitor_team = resolver.resolve(visitor_team_name)
        match_time = _normalize_match_time(match_status, match.get("date"))
        duels = match.get("duels")
        duel_count = len(duels) if isinstance(duels, list) else 0
        return {
            "source": source,
            "external_match_id": raw_match_id,
            "match_id": _build_match_id(tournament_id=tournament_id, source=source, external_match_id=raw_match_id),
            "tournament_id": tournament_id,
            "status": match_status,
            "time_utc": match_time,
            "team_1": local_team.code if local_team else None,
            "team_2": visitor_team.code if visitor_team else None,
            "team_1_name": local_team_name or None,
            "team_2_name": visitor_team_name or None,
            "number_of_duels": duel_count or None,
        }

    def _normalize_duel_preview(
        self,
        *,
        tournament_id: str,
        duel: dict[str, Any],
        match_preview: dict[str, Any],
    ) -> dict[str, Any]:
        duel_number = _extract_duel_number(duel.get("name"))
        return {
            "match_id": match_preview["match_id"],
            "duel_number": duel_number,
            "duel_id": _build_duel_id(match_preview["match_id"], duel_number),
            "player_1_wtcoc_id": _non_zero_string(duel.get("idLocalPlayer")),
            "player_2_wtcoc_id": _non_zero_string(duel.get("idVisitorPlayer")),
            "tournament_id": tournament_id,
        }

def _normalize_api_datetime(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw or raw == ZERO_DATE:
        return None
    return raw.replace(" ", "T") + "Z"


def _normalize_match_time(status: Any, value: Any) -> str | None:
    raw_status = str(status or "").strip()
    if not raw_status or raw_status in {"10", "01", "11"}:
        return None
    return _normalize_api_datetime(value)


def _can_import_duels(status: Any) -> bool:
    raw = str(status or "").strip()
    if raw in UNCONFIRMED_MATCH_STATUSES:
        return False
    if len(raw) != 2 or not raw.isdigit():
        return False
    return all(ch >= "2" for ch in raw)


def _normalize_token(value: str) -> str:
    return str(value or "").strip().upper()


def _build_match_id(*, tournament_id: str, source: str, external_match_id: str) -> str:
    suffix = "PO" if source == "playoff" else "M"
    normalized_external = str(external_match_id or "").strip() or "UNKNOWN"
    return f"{tournament_id}-{suffix}{normalized_external}"


def _build_duel_id(match_id: str, duel_number: int | None) -> str | None:
    if not match_id or duel_number is None:
        return None
    return f"{match_id}-D{duel_number}"


def _extract_duel_number(value: Any) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    return int(digits) if digits else None


def _non_zero_string(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw or raw == "0":
        return None
    return raw
