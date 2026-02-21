from __future__ import annotations

import hashlib
from typing import Any

import pandas as pd


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _stable_lineup_id(
    *,
    provider: str,
    fixture_id: int | None,
    team_id: int | None,
    player_id: int | None,
    lineup_type_id: int | None,
    formation_field: Any,
    formation_position: Any,
    jersey_number: int | None,
) -> int:
    # Keep a deterministic fallback key for rows where the provider omits lineup_id.
    seed = "|".join(
        [
            provider,
            str(fixture_id or ""),
            str(team_id or ""),
            str(player_id or ""),
            str(lineup_type_id or ""),
            str(formation_field or ""),
            str(formation_position or ""),
            str(jersey_number or ""),
        ]
    )
    digest = hashlib.md5(seed.encode("utf-8")).hexdigest()
    return int(digest[:15], 16)


def build_fixture_lineups_dataframe(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        if payload.get("errors"):
            continue
        provider = str(payload.get("provider") or "unknown")
        for lineup in payload.get("response", []) or []:
            team = lineup.get("team") or {}
            player = lineup.get("player") or {}
            position = lineup.get("position") or {}
            fixture_id = _as_int(lineup.get("fixture_id"))
            team_id = _as_int(team.get("id"))
            player_id = _as_int(player.get("id"))
            lineup_type_id = _as_int(lineup.get("lineup_type_id"))
            jersey_number = _as_int(lineup.get("jersey_number"))
            formation_field = lineup.get("formation_field")
            formation_position = _as_int(lineup.get("formation_position"))
            lineup_id = _as_int(lineup.get("lineup_id"))
            if lineup_id is None:
                lineup_id = _stable_lineup_id(
                    provider=provider,
                    fixture_id=fixture_id,
                    team_id=team_id,
                    player_id=player_id,
                    lineup_type_id=lineup_type_id,
                    formation_field=formation_field,
                    formation_position=formation_position,
                    jersey_number=jersey_number,
                )
            rows.append(
                {
                    "provider": provider,
                    "fixture_id": fixture_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "lineup_id": lineup_id,
                    "position_id": _as_int(position.get("id")),
                    "position_name": position.get("name"),
                    "lineup_type_id": lineup_type_id,
                    "formation_field": formation_field,
                    "formation_position": formation_position,
                    "jersey_number": jersey_number,
                    "details": lineup.get("details") or [],
                    "payload": lineup,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Nenhuma linha de lineups gerada a partir dos payloads raw.")
    df = df.dropna(subset=["fixture_id", "team_id", "lineup_id"]).copy()
    df = df.drop_duplicates(subset=["provider", "fixture_id", "team_id", "lineup_id"], keep="last").copy()
    return df
