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


def normalize_time_elapsed(value: Any) -> tuple[int | None, bool]:
    parsed = _as_int(value)
    if parsed is None:
        return None, False
    if parsed < 0:
        return None, True
    return parsed, False


def _raw_component(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _event_id(
    fixture_id: int,
    time_elapsed_raw: Any,
    time_extra_raw: Any,
    team_id: int | None,
    event_type: str | None,
    detail: str | None,
    player_id: int | None,
    assist_id: int | None,
    comments: str | None,
    provider_event_id: Any = None,
) -> str:
    if provider_event_id is not None and str(provider_event_id).strip():
        raw = "|".join(["provider_event", str(fixture_id), str(provider_event_id).strip()])
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    raw = "|".join(
        [
            str(fixture_id),
            _raw_component(time_elapsed_raw),
            _raw_component(time_extra_raw),
            str(team_id or ""),
            str(event_type or ""),
            str(detail or ""),
            str(player_id or ""),
            str(assist_id or ""),
            str(comments or ""),
        ]
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _payload_fixture_id(payload: dict[str, Any]) -> int | None:
    source_params = payload.get("source_params", {}) or {}
    fixture_raw = source_params.get("fixture")
    if fixture_raw is None:
        return None
    try:
        return int(fixture_raw)
    except (TypeError, ValueError):
        return None


def _flatten_events_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if payload.get("errors"):
        return []
    fixture_id = _payload_fixture_id(payload)
    if fixture_id is None:
        return []

    response_rows = payload.get("response", []) or []
    if not isinstance(response_rows, list):
        return []

    rows: list[dict[str, Any]] = []
    for event in response_rows:
        time_info = (event or {}).get("time") or {}
        team = (event or {}).get("team") or {}
        player = (event or {}).get("player") or {}
        assist = (event or {}).get("assist") or {}

        raw_time_elapsed = time_info.get("elapsed")
        time_elapsed, is_time_elapsed_anomalous = normalize_time_elapsed(raw_time_elapsed)
        raw_time_extra = time_info.get("extra")
        time_extra = _as_int(time_info.get("extra"))
        team_id = _as_int(team.get("id"))
        player_id = _as_int(player.get("id"))
        assist_id = _as_int(assist.get("id"))
        event_type = (event or {}).get("type")
        detail = (event or {}).get("detail")
        comments = (event or {}).get("comments")
        provider_event_id = (event or {}).get("provider_event_id") or (event or {}).get("id")

        rows.append(
            {
                "event_id": _event_id(
                    fixture_id,
                    raw_time_elapsed,
                    raw_time_extra,
                    team_id,
                    event_type,
                    detail,
                    player_id,
                    assist_id,
                    comments,
                    provider_event_id=provider_event_id,
                ),
                "fixture_id": fixture_id,
                "time_elapsed": time_elapsed,
                "time_extra": time_extra,
                "is_time_elapsed_anomalous": is_time_elapsed_anomalous,
                "team_id": team_id,
                "team_name": team.get("name"),
                "player_id": player_id,
                "player_name": player.get("name"),
                "assist_id": assist_id,
                "assist_name": assist.get("name"),
                "type": event_type,
                "detail": detail,
                "comments": comments,
            }
        )
    return rows


def _assert_no_conflicting_event_id_collisions(df: pd.DataFrame) -> None:
    duplicate_ids = df[df.duplicated(subset=["event_id"], keep=False)]
    if duplicate_ids.empty:
        return

    signature_columns = [
        "fixture_id",
        "time_elapsed",
        "time_extra",
        "team_id",
        "team_name",
        "player_id",
        "player_name",
        "assist_id",
        "assist_name",
        "type",
        "detail",
        "comments",
        "is_time_elapsed_anomalous",
    ]
    signatures = (
        duplicate_ids[signature_columns]
        .astype("string")
        .fillna("")
        .agg("|".join, axis=1)
    )
    signature_counts = signatures.groupby(duplicate_ids["event_id"]).nunique()
    conflicting_ids = signature_counts[signature_counts > 1]
    if not conflicting_ids.empty:
        sample_ids = ", ".join(str(value) for value in conflicting_ids.index[:5])
        raise RuntimeError(
            "Colisao de event_id detectada com eventos distintos. "
            f"event_ids={sample_ids}"
        )


def build_match_events_dataframe(payloads: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for payload in payloads:
        rows.extend(_flatten_events_payload(payload))

    if not rows:
        raise RuntimeError("Nenhuma linha de match events gerada a partir dos payloads raw.")

    df = pd.DataFrame(rows)
    for col in ["fixture_id", "time_elapsed", "time_extra", "team_id", "player_id", "assist_id"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "is_time_elapsed_anomalous" not in df.columns:
        df["is_time_elapsed_anomalous"] = False
    df["is_time_elapsed_anomalous"] = df["is_time_elapsed_anomalous"].fillna(False).astype(bool)

    text_cols = ["event_id", "team_name", "player_name", "assist_name", "type", "detail", "comments"]
    for col in text_cols:
        df[col] = df[col].astype("string")

    _assert_no_conflicting_event_id_collisions(df)
    df = df.drop_duplicates(subset=["event_id"], keep="last").copy()
    return df
