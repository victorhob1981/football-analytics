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


def _detail_map(details: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for detail in details:
        detail_type = detail.get("type") or {}
        key = str(detail_type.get("developer_name") or "").strip().upper()
        if not key:
            continue
        value = _as_int(detail.get("value"))
        if value is not None:
            out[key] = value
    return out


def build_standings_snapshots_dataframe(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        if payload.get("errors"):
            continue
        provider = str(payload.get("provider") or "unknown")
        for standing in payload.get("response", []) or []:
            details = _detail_map(standing.get("details") or [])
            team = standing.get("participant") or {}
            rows.append(
                {
                    "provider": provider,
                    "league_id": _as_int(standing.get("league_id")),
                    "season_id": _as_int(standing.get("season_id")),
                    "stage_id": _as_int(standing.get("stage_id")) or 0,
                    "round_id": _as_int(standing.get("round_id")) or 0,
                    "team_id": _as_int(standing.get("participant_id")) or _as_int(team.get("id")),
                    "position": _as_int(standing.get("position")),
                    "points": _as_int(standing.get("points")) or details.get("TOTAL_POINTS"),
                    "games_played": details.get("OVERALL_MATCHES"),
                    "won": details.get("OVERALL_WINS"),
                    "draw": details.get("OVERALL_DRAWS"),
                    "lost": details.get("OVERALL_LOST"),
                    "goals_for": details.get("OVERALL_SCORED"),
                    "goals_against": details.get("OVERALL_CONCEDED"),
                    "goal_diff": details.get("OVERALL_GOAL_DIFFERENCE"),
                    "payload": standing,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Nenhuma linha de standings snapshots gerada a partir dos payloads raw.")
    df = df.dropna(subset=["league_id", "season_id", "team_id"]).copy()
    df = df.drop_duplicates(subset=["provider", "season_id", "stage_id", "round_id", "team_id"], keep="last").copy()
    return df

