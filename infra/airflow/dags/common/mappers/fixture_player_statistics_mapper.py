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


def build_fixture_player_statistics_dataframe(payloads: list[dict[str, Any]]) -> pd.DataFrame:
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
                    "fixture_id": _as_int(row.get("fixture_id")),
                    "team_id": _as_int(team.get("id")),
                    "player_id": _as_int(player.get("id")),
                    "statistics": row.get("statistics") or [],
                    "payload": row,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Nenhuma linha de fixture_player_statistics gerada a partir dos payloads raw.")
    df = df.dropna(subset=["fixture_id", "team_id", "player_id"]).copy()
    df = df.drop_duplicates(subset=["provider", "fixture_id", "team_id", "player_id"], keep="last").copy()
    return df

