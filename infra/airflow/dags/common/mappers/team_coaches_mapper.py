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


def build_team_coaches_dataframe(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        if payload.get("errors"):
            continue
        provider = str(payload.get("provider") or "unknown")
        for row in payload.get("response", []) or []:
            team = row.get("team") or {}
            rows.append(
                {
                    "provider": provider,
                    "coach_tenure_id": _as_int(row.get("coach_tenure_id")),
                    "team_id": _as_int(team.get("id")),
                    "coach_id": _as_int(row.get("coach_id")),
                    "position_id": _as_int(row.get("position_id")),
                    "active": row.get("active"),
                    "temporary": row.get("temporary"),
                    "start_date": row.get("start"),
                    "end_date": row.get("end"),
                    "payload": row,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Nenhuma linha de team_coaches gerada a partir dos payloads raw.")
    df = df.dropna(subset=["coach_tenure_id", "team_id", "coach_id"]).copy()
    df = df.drop_duplicates(subset=["provider", "coach_tenure_id"], keep="last").copy()
    return df

