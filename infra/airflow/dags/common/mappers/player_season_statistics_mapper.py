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


def build_player_season_statistics_dataframe(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        if payload.get("errors"):
            continue
        provider = str(payload.get("provider") or "unknown")
        for row in payload.get("response", []) or []:
            player = row.get("player") or {}
            team = row.get("team") or {}
            season_info = row.get("season") or {}
            position = row.get("position") or {}
            rows.append(
                {
                    "provider": provider,
                    "player_id": _as_int(player.get("id")),
                    "season_id": _as_int(season_info.get("id")),
                    "team_id": _as_int(team.get("id")) or 0,
                    "league_id": _as_int(season_info.get("league_id")),
                    "season_name": season_info.get("name"),
                    "position_name": position.get("name"),
                    "statistics": row.get("statistics") or [],
                    "payload": row,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Nenhuma linha de player_season_statistics gerada a partir dos payloads raw.")
    df = df.dropna(subset=["player_id", "season_id"]).copy()
    df["team_id"] = df["team_id"].fillna(0).astype("Int64")
    df = df.drop_duplicates(subset=["provider", "player_id", "season_id", "team_id"], keep="last").copy()
    return df

