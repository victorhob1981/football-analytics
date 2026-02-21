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


def build_team_sidelined_dataframe(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        if payload.get("errors"):
            continue
        provider = str(payload.get("provider") or "unknown")
        for row in payload.get("response", []) or []:
            team = row.get("team") or {}
            player = row.get("player") or {}
            rows.append(
                {
                    "provider": provider,
                    "sidelined_id": _as_int(row.get("sidelined_id")),
                    "team_id": _as_int(team.get("id")),
                    "player_id": _as_int(player.get("id")),
                    "season_id": _as_int(row.get("season_id")),
                    "category": row.get("category"),
                    "type_id": _as_int(row.get("type_id")),
                    "start_date": row.get("start_date"),
                    "end_date": row.get("end_date"),
                    "games_missed": _as_int(row.get("games_missed")),
                    "completed": row.get("completed"),
                    "payload": row,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Nenhuma linha de team_sidelined gerada a partir dos payloads raw.")
    df = df.dropna(subset=["sidelined_id", "team_id", "player_id"]).copy()
    df = df.drop_duplicates(subset=["provider", "sidelined_id"], keep="last").copy()
    return df

