from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse

from sync_wtcoc_matches.client import WtcocApiClient

from .sqlite_repository import AssociationMapping, SqliteWtcocPlayersRepository


@dataclass(frozen=True)
class _AssociationResolver:
    by_name: dict[str, AssociationMapping]
    aliases: dict[str, str]

    @classmethod
    def from_rows(cls, rows: list[AssociationMapping]) -> "_AssociationResolver":
        by_name: dict[str, AssociationMapping] = {}
        for row in rows:
            normalized = _normalize_token(row.name)
            if normalized:
                by_name[normalized] = row
        return cls(
            by_name=by_name,
            aliases={
                "BELARUS": "TUTEJŠYJA",
            },
        )

    def resolve(self, raw_value: str) -> AssociationMapping | None:
        normalized = _normalize_token(raw_value)
        if not normalized:
            return None
        alias_target = self.aliases.get(normalized)
        if alias_target:
            return self.by_name.get(alias_target)
        return self.by_name.get(normalized)


class WtcocPlayersSyncService:
    def __init__(self, *, repository: SqliteWtcocPlayersRepository, client: WtcocApiClient) -> None:
        self.repository = repository
        self.client = client

    def build_import_plan(self, *, sample_limit: int = 10) -> dict[str, Any]:
        response = self.client.fetch_players()
        payload = response.payload
        players = payload.get("players")
        if not isinstance(players, list):
            players = []

        db_summary = self.repository.load_summary()
        existing_profile_ids = self.repository.load_existing_profile_ids()
        resolver = _AssociationResolver.from_rows(self.repository.load_association_mappings())

        duplicate_groups: dict[str, list[dict[str, Any]]] = {}
        parsed_candidates: list[dict[str, Any]] = []
        invalid_bga_link_rows: list[dict[str, Any]] = []
        unresolved_association_rows: list[dict[str, Any]] = []
        existing_profile_rows: list[dict[str, Any]] = []

        for player in players:
            if not isinstance(player, dict):
                continue
            bga_player_id = _extract_bga_player_id(player.get("bgaLink"))
            if not bga_player_id:
                invalid_bga_link_rows.append(_preview_row(player, reason="invalid_bga_link"))
                continue

            association = resolver.resolve(str(player.get("team") or "").strip())
            if association is None:
                unresolved_association_rows.append(_preview_row(player, bga_player_id=bga_player_id, reason="association_not_found"))
                continue

            normalized = {
                "id": bga_player_id,
                "bga_nickname": str(player.get("nick") or "").strip() or None,
                "association": association.id,
                "association_name": association.name,
                "team": str(player.get("team") or "").strip() or None,
                "wtcoc_player_id": str(player.get("playerId") or "").strip() or None,
                "bga_link": str(player.get("bgaLink") or "").strip() or None,
                "email": _build_placeholder_email(bga_player_id),
                "premium": str(player.get("premium") or "").strip() or None,
                "captain": str(player.get("captain") or "").strip() or None,
                "disqualified": str(player.get("disqualified") or "").strip() or None,
                "active": str(player.get("active") or "").strip() or None,
            }
            duplicate_groups.setdefault(bga_player_id, []).append(normalized)

        ready_to_create: list[dict[str, Any]] = []
        duplicate_bga_id_rows: list[dict[str, Any]] = []
        duplicate_bga_id_map = {
            bga_id: rows
            for bga_id, rows in duplicate_groups.items()
            if len(rows) > 1
        }
        for bga_id, rows in duplicate_groups.items():
            if len(rows) > 1:
                duplicate_bga_id_rows.extend(rows)
                continue
            candidate = rows[0]
            parsed_candidates.append(candidate)
            if candidate["id"] in existing_profile_ids:
                existing_profile_rows.append(candidate)
                continue
            ready_to_create.append(candidate)

        return {
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": {
                "endpoint": "players",
                "response": str(payload.get("response") or "").strip() or None,
                "code": str(payload.get("code") or "").strip() or None,
                "description": payload.get("description"),
                "players_count": len(players),
            },
            "db": db_summary,
            "totals": {
                "players_received": len(players),
                "players_with_valid_bga_id": sum(len(rows) for rows in duplicate_groups.values()),
                "players_with_invalid_bga_link": len(invalid_bga_link_rows),
                "players_with_unresolved_association": len(unresolved_association_rows),
                "players_with_duplicate_bga_id_in_api": len(duplicate_bga_id_rows),
                "duplicate_bga_id_groups_in_api": len(duplicate_bga_id_map),
                "players_already_in_profiles": len(existing_profile_rows),
                "profiles_ready_to_create": len(ready_to_create),
            },
            "gaps": _build_gaps(
                invalid_bga_link_rows=invalid_bga_link_rows,
                unresolved_association_rows=unresolved_association_rows,
                duplicate_bga_id_map=duplicate_bga_id_map,
            ),
            "samples": {
                "profiles_ready_to_create": ready_to_create[:max(1, int(sample_limit))],
                "already_existing_profiles": existing_profile_rows[:max(1, int(sample_limit))],
                "unresolved_associations": unresolved_association_rows[:max(1, int(sample_limit))],
                "duplicate_bga_id_groups": [
                    rows[:max(1, int(sample_limit))]
                    for _, rows in list(duplicate_bga_id_map.items())[:max(1, int(sample_limit))]
                ],
            },
            "apply_preview": {
                "profiles_ready": len(ready_to_create),
                "sample_profile": ready_to_create[0] if ready_to_create else None,
            },
            "apply_payload": {
                "profiles": ready_to_create,
            },
        }


def _normalize_token(value: str) -> str:
    return str(value or "").strip().upper()


def _extract_bga_player_id(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    player_id = parse_qs(parsed.query).get("id", [None])[0]
    normalized = str(player_id or "").strip()
    return normalized if normalized.isdigit() else None


def _build_placeholder_email(player_id: str) -> str:
    return f"wtcoc-bga-{player_id}@placeholder.invalid"


def _preview_row(player: dict[str, Any], *, bga_player_id: str | None = None, reason: str) -> dict[str, Any]:
    return {
        "reason": reason,
        "id": bga_player_id,
        "wtcoc_player_id": str(player.get("playerId") or "").strip() or None,
        "bga_nickname": str(player.get("nick") or "").strip() or None,
        "team": str(player.get("team") or "").strip() or None,
        "bga_link": str(player.get("bgaLink") or "").strip() or None,
    }


def _build_gaps(
    *,
    invalid_bga_link_rows: list[dict[str, Any]],
    unresolved_association_rows: list[dict[str, Any]],
    duplicate_bga_id_map: dict[str, list[dict[str, Any]]],
) -> list[str]:
    gaps: list[str] = []
    if invalid_bga_link_rows:
        gaps.append(f"WTCOC API returned {len(invalid_bga_link_rows)} players with invalid bgaLink.")
    if unresolved_association_rows:
        sample_names = ", ".join(
            sorted({str(item.get('team') or '').strip() for item in unresolved_association_rows if str(item.get('team') or '').strip()})[:10]
        )
        gaps.append(f"Association mapping is missing for these WTCOC team names in associations.name: {sample_names}.")
    if duplicate_bga_id_map:
        sample_ids = ", ".join(sorted(list(duplicate_bga_id_map.keys()))[:10])
        gaps.append(f"WTCOC API contains duplicate BGA player ids; these rows are skipped until resolved: {sample_ids}.")
    return gaps
