from __future__ import annotations

from typing import Any

import pandas as pd


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_head_to_head_fixtures_dataframe(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        if payload.get("errors"):
            continue
        provider = str(payload.get("provider") or "unknown")
        source_params = payload.get("source_params") or {}
        pair_team_id = _as_int(source_params.get("pair_team_id")) or _as_int(source_params.get("team_id"))
        pair_opponent_id = _as_int(source_params.get("pair_opponent_id")) or _as_int(source_params.get("opponent_id"))

        for row in payload.get("response", []) or []:
            fixture = row.get("fixture") or {}
            league = row.get("league") or {}
            teams = row.get("teams") or {}
            goals = row.get("goals") or {}
            rows.append(
                {
                    "provider": provider,
                    "pair_team_id": pair_team_id,
                    "pair_opponent_id": pair_opponent_id,
                    "fixture_id": _as_int(fixture.get("id")),
                    "league_id": _as_int(league.get("id")),
                    "season_id": _as_int(league.get("season_id")) or _as_int(league.get("season")),
                    "match_date": fixture.get("date"),
                    "home_team_id": _as_int((teams.get("home") or {}).get("id")),
                    "away_team_id": _as_int((teams.get("away") or {}).get("id")),
                    "home_goals": _as_int(goals.get("home")),
                    "away_goals": _as_int(goals.get("away")),
                    "payload": row,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Nenhuma linha de head_to_head_fixtures gerada a partir dos payloads raw.")
    df = df.dropna(subset=["pair_team_id", "pair_opponent_id", "fixture_id"]).copy()
    df = df.drop_duplicates(
        subset=["provider", "pair_team_id", "pair_opponent_id", "fixture_id"],
        keep="last",
    ).copy()
    return df
