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


def build_coaches_dataframe(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        if payload.get("errors"):
            continue
        provider = str(payload.get("provider") or "unknown")
        for row in payload.get("response", []) or []:
            rows.append(
                {
                    "provider": provider,
                    "coach_id": _as_int(row.get("coach_id")),
                    "coach_name": row.get("coach_name"),
                    "image_path": row.get("image_path"),
                    "payload": row,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("Nenhuma linha de coaches gerada a partir dos payloads raw.")
    df = df.dropna(subset=["coach_id"]).copy()
    df = df.drop_duplicates(subset=["provider", "coach_id"], keep="last").copy()
    return df
