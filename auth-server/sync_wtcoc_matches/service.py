from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .client import WtcocApiClient
from .sqlite_repository import ProfileMapping, SqliteWtcocRepository, TeamMapping


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

    def resolve(self, raw_value: str) -> TeamMapping | None:
        normalized = _normalize_token(raw_value)
        if not normalized:
            return None
        return self.by_code.get(normalized) or self.by_name.get(normalized)


@dataclass(frozen=True)
class _PlayerResolver:
    by_nickname: dict[str, ProfileMapping]
    by_name: dict[str, ProfileMapping]

    @classmethod
    def from_rows(cls, rows: list[ProfileMapping]) -> "_PlayerResolver":
        nickname_groups: dict[str, list[ProfileMapping]] = {}
        name_groups: dict[str, list[ProfileMapping]] = {}
        for row in rows:
            normalized_nickname = _normalize_token(row.bga_nickname or "")
            normalized_name = _normalize_token(row.name or "")
            if normalized_nickname:
                nickname_groups.setdefault(normalized_nickname, []).append(row)
            if normalized_name:
                name_groups.setdefault(normalized_name, []).append(row)
        return cls(
            by_nickname={
                key: values[0]
                for key, values in nickname_groups.items()
                if len(values) == 1 and str(values[0].id or "").strip()
            },
            by_name={
                key: values[0]
                for key, values in name_groups.items()
                if len(values) == 1 and str(values[0].id or "").strip()
            },
        )

    def resolve(self, raw_value: str) -> str | None:
        normalized = _normalize_token(raw_value)
        if not normalized:
            return None
        row = self.by_nickname.get(normalized) or self.by_name.get(normalized)
        return str(row.id or "").strip() or None if row else None


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
        player_resolver = _PlayerResolver.from_rows(self.repository.load_profile_mappings())
        stored_links = {
            _build_wtcoc_link_key(source=link.source, external_match_id=link.external_match_id): link
            for link in self.repository.load_wtcoc_match_links(normalized_tournament_id)
        }
        default_fallback_date_iso = datetime.now(timezone.utc).isoformat()

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
                raw_external_match_id = str(match.get("idMatch") or "").strip()
                stored_link = stored_links.get(
                    _build_wtcoc_link_key(source=response.source, external_match_id=raw_external_match_id)
                )
                match_preview = self._normalize_match_preview(
                    tournament_id=normalized_tournament_id,
                    source=response.source,
                    match=match,
                    resolver=resolver,
                    fallback_date_iso=stored_link.fallback_date_iso if stored_link else default_fallback_date_iso,
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
                        "source": match_preview["source"],
                        "external_match_id": match_preview["external_match_id"],
                        "fallback_date_iso": match_preview["fallback_date_iso"],
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
                    if not _has_named_players(duel):
                        continue
                    duel_preview = self._normalize_duel_preview(
                        tournament_id=normalized_tournament_id,
                        duel=duel,
                        match_preview=match_preview,
                        player_resolver=player_resolver,
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
                            "player_1_id": duel_preview["player_1_id"],
                            "player_2_id": duel_preview["player_2_id"],
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
        fallback_date_iso: str | None,
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
        resolved_fallback_date_iso = str(fallback_date_iso or "").strip() or None
        return {
            "source": source,
            "external_match_id": raw_match_id,
            "fallback_date_iso": resolved_fallback_date_iso,
            "match_id": _build_generated_match_id(
                time_utc=match_time,
                team_1=local_team.code if local_team else None,
                team_2=visitor_team.code if visitor_team else None,
                fallback_date_iso=resolved_fallback_date_iso,
            ) or _build_legacy_match_id(
                tournament_id=tournament_id,
                source=source,
                external_match_id=raw_match_id,
            ),
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
        player_resolver: _PlayerResolver,
    ) -> dict[str, Any]:
        duel_number = _extract_duel_number(duel.get("name"))
        return {
            "match_id": match_preview["match_id"],
            "duel_number": duel_number,
            "duel_id": _build_duel_id(match_preview["match_id"], duel_number),
            "player_1_id": player_resolver.resolve(str(duel.get("nameLocalPlayer") or "").strip()),
            "player_2_id": player_resolver.resolve(str(duel.get("nameVisitorPlayer") or "").strip()),
            "tournament_id": tournament_id,
        }

def _normalize_api_datetime(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw or raw == ZERO_DATE:
        return None
    return raw.replace(" ", "T") + "Z"


def _normalize_match_time(status: Any, value: Any) -> str | None:
    raw_status = str(status or "").strip()
    if not raw_status or raw_status in {"10", "01"}:
        return None
    return _normalize_api_datetime(value)


def _can_import_duels(status: Any) -> bool:
    raw = str(status or "").strip()
    if raw in UNCONFIRMED_MATCH_STATUSES:
        return False
    if len(raw) != 2 or not raw.isdigit():
        return False
    return all(ch >= "2" for ch in raw)


def _has_named_players(duel: dict[str, Any]) -> bool:
    local_name = str(duel.get("nameLocalPlayer") or "").strip()
    visitor_name = str(duel.get("nameVisitorPlayer") or "").strip()
    return bool(local_name and visitor_name)


def _normalize_token(value: str) -> str:
    return str(value or "").strip().upper()


def _build_wtcoc_link_key(*, source: str, external_match_id: str) -> str | None:
    normalized_source = str(source or "").strip().lower()
    normalized_external = str(external_match_id or "").strip()
    if not normalized_source or not normalized_external:
        return None
    return f"{normalized_source}:{normalized_external}"


def _build_legacy_match_id(*, tournament_id: str, source: str, external_match_id: str) -> str:
    suffix = "PO" if source == "playoff" else "M"
    normalized_external = str(external_match_id or "").strip() or "UNKNOWN"
    return f"{tournament_id}-{suffix}{normalized_external}"


def _build_generated_match_id(
    *,
    time_utc: str | None,
    team_1: str | None,
    team_2: str | None,
    fallback_date_iso: str | None = None,
) -> str:
    normalized_team_1 = str(team_1 or "").strip().upper()
    normalized_team_2 = str(team_2 or "").strip().upper()
    if not normalized_team_1 or not normalized_team_2:
        return ""
    raw_iso = str(time_utc or "").strip() or str(fallback_date_iso or "").strip()
    date_part = raw_iso[:10].replace("-", "")
    if not _is_generated_match_date_part(date_part):
        return f"{normalized_team_1}{normalized_team_2}"
    return f"{date_part}{normalized_team_1}{normalized_team_2}"


def _is_generated_match_date_part(value: str) -> bool:
    raw = str(value or "").strip()
    return len(raw) == 8 and raw.isdigit()


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
