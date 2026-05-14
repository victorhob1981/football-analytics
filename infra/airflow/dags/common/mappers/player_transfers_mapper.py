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


def build_player_transfers_dataframe(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        if payload.get("errors"):
            continue
        provider = str(payload.get("provider") or "unknown")
        for row in payload.get("response", []) or []:
            player = row.get("player") or {}
            rows.append(
                {
                    "provider": provider,
                    "transfer_id": _as_int(row.get("transfer_id")),
                    "player_id": _as_int(player.get("id")),
                    "from_team_id": _as_int(row.get("from_team_id")),
                    "to_team_id": _as_int(row.get("to_team_id")),
                    "transfer_date": row.get("date"),
                    "completed": row.get("completed"),
                    "career_ended": row.get("career_ended"),
                    "type_id": _as_int(row.get("type_id")),
                    "position_id": _as_int(row.get("position_id")),
                    "amount": row.get("amount"),
                    "payload": row,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Nenhuma linha de player_transfers gerada a partir dos payloads raw.")
    df = df.dropna(subset=["transfer_id", "player_id"]).copy()
    df = df.drop_duplicates(subset=["provider", "transfer_id"], keep="last").copy()
    return df

