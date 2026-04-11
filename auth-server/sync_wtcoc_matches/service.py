from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .client import WtcocApiClient
from .sqlite_repository import AssociationMapping, SqliteWtcocRepository


ZERO_DATE = "0000-00-00 00:00:00"


@dataclass(frozen=True)
class _AssociationResolver:
    by_code: dict[str, AssociationMapping]
    by_name: dict[str, AssociationMapping]

    @classmethod
    def from_rows(cls, rows: list[AssociationMapping]) -> "_AssociationResolver":
        by_code: dict[str, AssociationMapping] = {}
        by_name: dict[str, AssociationMapping] = {}
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

    def analyze(
        self,
        *,
        tournament_id: str,
        include_playoff: bool = True,
        external_match_id: str | None = None,
        sample_limit: int = 3,
    ) -> dict[str, Any]:
        normalized_tournament_id = str(tournament_id or "").strip()
        if not normalized_tournament_id:
            raise ValueError("tournament_id is required")

        responses = [self.client.fetch_calendar()]
        if include_playoff:
            responses.append(self.client.fetch_playoff())

        db_summary = self.repository.load_db_summary(tournament_id=normalized_tournament_id)
        resolver = _AssociationResolver.from_rows(self.repository.load_association_mappings())

        source_summaries: list[dict[str, Any]] = []
        all_matches: list[dict[str, Any]] = []
        for response in responses:
            payload = response.payload
            response_code = str(payload.get("code") or "").strip() or None
            response_status = str(payload.get("response") or "").strip() or None
            matches = payload.get("matches")
            if not isinstance(matches, list):
                matches = []
            filtered = []
            for match in matches:
                if not isinstance(match, dict):
                    continue
                if external_match_id and str(match.get("idMatch") or "").strip() != str(external_match_id).strip():
                    continue
                filtered.append(match)
            source_summaries.append(
                {
                    "source": response.source,
                    "response": response_status,
                    "code": response_code,
                    "description": payload.get("description"),
                    "matches_count": len(filtered),
                }
            )
            for match in filtered:
                all_matches.append({"source": response.source, "payload": match})

        totals = {
            "matches": len(all_matches),
            "duels": 0,
            "games": 0,
            "matches_with_time": 0,
            "matches_without_time": 0,
            "matches_with_resolved_teams": 0,
            "matches_without_resolved_teams": 0,
            "duels_with_players": 0,
            "duels_without_players": 0,
            "duels_with_bgaurl": 0,
            "duels_with_any_result": 0,
        }
        unresolved_team_names: set[str] = set()
        normalized_match_samples: list[dict[str, Any]] = []
        normalized_duel_samples: list[dict[str, Any]] = []
        first_match_with_players: dict[str, Any] | None = None

        for wrapped in all_matches:
            source = str(wrapped["source"])
            match = wrapped["payload"]
            normalized_match = self._normalize_match_preview(
                tournament_id=normalized_tournament_id,
                source=source,
                match=match,
                resolver=resolver,
            )
            normalized_match_samples.append(normalized_match)
            totals["matches_with_time"] += 1 if normalized_match["time_utc"] else 0
            totals["matches_without_time"] += 0 if normalized_match["time_utc"] else 1
            if normalized_match["team_1"] and normalized_match["team_2"]:
                totals["matches_with_resolved_teams"] += 1
            else:
                totals["matches_without_resolved_teams"] += 1
                if not normalized_match["team_1"]:
                    unresolved_team_names.add(str(match.get("nameLocalTeam") or "").strip())
                if not normalized_match["team_2"]:
                    unresolved_team_names.add(str(match.get("nameVisitorTeam") or "").strip())

            duels = match.get("duels")
            if not isinstance(duels, list):
                duels = []
            for duel in duels:
                if not isinstance(duel, dict):
                    continue
                totals["duels"] += 1
                games = duel.get("games")
                if isinstance(games, list):
                    totals["games"] += len(games)
                normalized_duel = self._normalize_duel_preview(
                    tournament_id=normalized_tournament_id,
                    source=source,
                    match=match,
                    duel=duel,
                    match_preview=normalized_match,
                )
                normalized_duel_samples.append(normalized_duel)
                if normalized_duel["player_1_name"] or normalized_duel["player_2_name"]:
                    totals["duels_with_players"] += 1
                    if first_match_with_players is None:
                        first_match_with_players = {
                            "match": normalized_match,
                            "duel": normalized_duel,
                        }
                else:
                    totals["duels_without_players"] += 1
                if normalized_duel["bgaurl"]:
                    totals["duels_with_bgaurl"] += 1
                if normalized_duel["has_any_result"]:
                    totals["duels_with_any_result"] += 1

        gaps = self._build_gaps(totals=totals, unresolved_team_names=unresolved_team_names)
        return {
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "tournament_id": normalized_tournament_id,
            "filter_external_match_id": str(external_match_id).strip() if external_match_id else None,
            "sources": source_summaries,
            "db": db_summary,
            "totals": totals,
            "gaps": gaps,
            "samples": {
                "matches": normalized_match_samples[:sample_limit],
                "duels": normalized_duel_samples[:sample_limit],
                "first_match_with_players": first_match_with_players,
            },
            "mapping_notes": {
                "matches": {
                    "ready_fields": [
                        "id",
                        "tournament_id",
                        "team_1/team_2 after association name->code resolution",
                        "number_of_duels",
                        "dw1/dw2 from localResult/visitorResult when results appear",
                    ],
                    "missing_or_unstable_fields": [
                        "time_utc is missing when API date is 0000-00-00 00:00:00",
                        "lineup_type is not present in WTCOC API",
                        "lineup_deadline_h / lineup_deadline_utc are not present in WTCOC API",
                    ],
                },
                "duels": {
                    "ready_fields": [
                        "id",
                        "match_id",
                        "duel_number",
                        "dw1/dw2 from localResult/visitorResult when results appear",
                    ],
                    "missing_or_unstable_fields": [
                        "player_1_id / player_2_id are absent until WTCOC lineups are published",
                        "duel_format is not present in WTCOC API",
                        "time_utc per duel is not present in WTCOC API",
                        "WTCOC player ids are not confirmed to match profiles.id in auth-server",
                    ],
                },
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
        local_team_name = str(match.get("nameLocalTeam") or "").strip()
        visitor_team_name = str(match.get("nameVisitorTeam") or "").strip()
        local_team = resolver.resolve(local_team_name)
        visitor_team = resolver.resolve(visitor_team_name)
        match_time = _normalize_api_datetime(match.get("date"))
        duels = match.get("duels")
        duel_count = len(duels) if isinstance(duels, list) else 0
        return {
            "source": source,
            "external_match_id": raw_match_id,
            "match_id": _build_match_id(tournament_id=tournament_id, source=source, external_match_id=raw_match_id),
            "tournament_id": tournament_id,
            "time_utc": match_time,
            "team_1": local_team.code if local_team else None,
            "team_2": visitor_team.code if visitor_team else None,
            "team_1_name": local_team_name or None,
            "team_2_name": visitor_team_name or None,
            "number_of_duels": duel_count or None,
            "status_raw": str(match.get("status") or "").strip() or None,
            "dw1_raw": _to_int_or_none(match.get("localResult")),
            "dw2_raw": _to_int_or_none(match.get("visitorResult")),
            "round_number": _to_int_or_none(match.get("numberRound")),
            "group_name": str(match.get("nameGroup") or "").strip() or None,
            "round_start_utc": _normalize_api_datetime(match.get("initalDateRound")),
            "round_finish_utc": _normalize_api_datetime(match.get("finishDateRound")),
        }

    def _normalize_duel_preview(
        self,
        *,
        tournament_id: str,
        source: str,
        match: dict[str, Any],
        duel: dict[str, Any],
        match_preview: dict[str, Any],
    ) -> dict[str, Any]:
        duel_number = _extract_duel_number(duel.get("name"))
        games = duel.get("games")
        if not isinstance(games, list):
            games = []
        return {
            "source": source,
            "external_match_id": str(match.get("idMatch") or "").strip() or None,
            "match_id": match_preview["match_id"],
            "duel_number": duel_number,
            "duel_id": _build_duel_id(match_preview["match_id"], duel_number),
            "player_1_wtcoc_id": _non_zero_string(duel.get("idLocalPlayer")),
            "player_2_wtcoc_id": _non_zero_string(duel.get("idVisitorPlayer")),
            "player_1_name": str(duel.get("nameLocalPlayer") or "").strip() or None,
            "player_2_name": str(duel.get("nameVisitorPlayer") or "").strip() or None,
            "bgaurl": str(duel.get("bgaurl") or "").strip() or None,
            "dw1_raw": _to_int_or_none(duel.get("localResult")),
            "dw2_raw": _to_int_or_none(duel.get("visitorResult")),
            "has_any_result": any(_game_has_any_result(game) for game in games if isinstance(game, dict)),
            "missing_for_db": [
                label
                for label, is_missing in (
                    ("duel_format", True),
                    ("time_utc", True),
                    ("player_1_id", not bool(_non_zero_string(duel.get("idLocalPlayer")))),
                    ("player_2_id", not bool(_non_zero_string(duel.get("idVisitorPlayer")))),
                )
                if is_missing
            ],
            "tournament_id": tournament_id,
        }

    @staticmethod
    def _build_gaps(*, totals: dict[str, int], unresolved_team_names: set[str]) -> list[str]:
        gaps: list[str] = []
        if totals["matches_without_time"] > 0:
            gaps.append(
                f"WTCOC API does not provide usable match time for {totals['matches_without_time']} of {totals['matches']} matches."
            )
        if unresolved_team_names:
            names = ", ".join(sorted(name for name in unresolved_team_names if name)[:10])
            gaps.append(f"Association mapping is missing for these team names: {names}.")
        if totals["duels_without_players"] > 0:
            gaps.append(
                f"WTCOC API does not provide player assignments for {totals['duels_without_players']} of {totals['duels']} duels yet."
            )
        gaps.append("WTCOC API does not provide duel_format, so duels cannot be fully mapped into auth-server yet.")
        gaps.append("WTCOC API does not provide duel-specific time_utc, so duels cannot be scheduled precisely yet.")
        return gaps


def _normalize_api_datetime(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw or raw == ZERO_DATE:
        return None
    return raw.replace(" ", "T") + "Z"


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


def _to_int_or_none(value: Any) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _non_zero_string(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw or raw == "0":
        return None
    return raw


def _game_has_any_result(game: dict[str, Any]) -> bool:
    for key in ("localResult", "visitorResult", "idPlayerWin", "idPlayerLost", "lostByTime", "lostByNotPresented"):
        raw = str(game.get(key) or "").strip()
        if raw and raw != "0":
            return True
    return False
