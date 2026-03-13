from __future__ import annotations

from typing import Any

import pandas as pd


MAPPER_DISCARD_THRESHOLD = 0.10
LOADER_DISCARD_THRESHOLD = 0.05


def _append_discard_stat(
    df: pd.DataFrame,
    *,
    channel: str,
    context_label: str,
    operation: str,
    reason: str,
    before: int,
    after: int,
    threshold: float,
    subset: list[str],
) -> pd.DataFrame:
    stats = list(df.attrs.get("discard_stats", []))
    dropped = before - after
    rate = (dropped / before) if before > 0 else 0.0
    stats.append(
        {
            "channel": channel,
            "context_label": context_label,
            "operation": operation,
            "reason": reason,
            "before": before,
            "after": after,
            "dropped": dropped,
            "discard_rate": rate,
            "threshold": threshold,
            "subset": list(subset),
        }
    )
    df.attrs["discard_stats"] = stats
    return df


def _log_discard(
    *,
    channel: str,
    context_label: str,
    operation: str,
    reason: str,
    before: int,
    after: int,
    threshold: float,
    subset: list[str],
) -> tuple[int, float]:
    dropped = before - after
    rate = (dropped / before) if before > 0 else 0.0
    if dropped > 0:
        print(
            f"[{channel}:{context_label}] {dropped}/{before} rows descartadas "
            f"({rate:.1%}) | operation={operation} | reason={reason} "
            f"| subset={subset} | threshold={threshold:.0%}"
        )
    return dropped, rate


def _raise_if_threshold_exceeded(
    *,
    channel: str,
    context_label: str,
    operation: str,
    reason: str,
    dropped: int,
    before: int,
    rate: float,
    threshold: float,
) -> None:
    if dropped > 0 and rate > threshold:
        raise RuntimeError(
            f"[{channel}:{context_label}] Taxa de descarte {rate:.1%} excede threshold de "
            f"{threshold:.0%} | operation={operation} | reason={reason} | dropped={dropped}/{before}"
        )


def dropna_with_threshold(
    df: pd.DataFrame,
    *,
    subset: list[str],
    threshold: float = MAPPER_DISCARD_THRESHOLD,
    context_label: str,
    reason: str = "required_key_null",
    channel: str = "mapper",
) -> pd.DataFrame:
    before = len(df)
    result = df.dropna(subset=subset).copy()
    dropped, rate = _log_discard(
        channel=channel,
        context_label=context_label,
        operation="dropna",
        reason=reason,
        before=before,
        after=len(result),
        threshold=threshold,
        subset=subset,
    )
    result = _append_discard_stat(
        result,
        channel=channel,
        context_label=context_label,
        operation="dropna",
        reason=reason,
        before=before,
        after=len(result),
        threshold=threshold,
        subset=subset,
    )
    _raise_if_threshold_exceeded(
        channel=channel,
        context_label=context_label,
        operation="dropna",
        reason=reason,
        dropped=dropped,
        before=before,
        rate=rate,
        threshold=threshold,
    )
    return result


def drop_duplicates_with_threshold(
    df: pd.DataFrame,
    *,
    subset: list[str],
    threshold: float = MAPPER_DISCARD_THRESHOLD,
    context_label: str,
    reason: str = "duplicate_grain",
    keep: str = "last",
    channel: str = "mapper",
) -> pd.DataFrame:
    before = len(df)
    result = df.drop_duplicates(subset=subset, keep=keep).copy()
    dropped, rate = _log_discard(
        channel=channel,
        context_label=context_label,
        operation="drop_duplicates",
        reason=reason,
        before=before,
        after=len(result),
        threshold=threshold,
        subset=subset,
    )
    result = _append_discard_stat(
        result,
        channel=channel,
        context_label=context_label,
        operation="drop_duplicates",
        reason=reason,
        before=before,
        after=len(result),
        threshold=threshold,
        subset=subset,
    )
    _raise_if_threshold_exceeded(
        channel=channel,
        context_label=context_label,
        operation="drop_duplicates",
        reason=reason,
        dropped=dropped,
        before=before,
        rate=rate,
        threshold=threshold,
    )
    return result


def filter_with_threshold(
    df: pd.DataFrame,
    *,
    keep_mask: pd.Series,
    threshold: float = LOADER_DISCARD_THRESHOLD,
    context_label: str,
    reason: str,
    subset: list[str],
    channel: str = "loader",
) -> pd.DataFrame:
    before = len(df)
    result = df[keep_mask].copy()
    dropped, rate = _log_discard(
        channel=channel,
        context_label=context_label,
        operation="row_filter",
        reason=reason,
        before=before,
        after=len(result),
        threshold=threshold,
        subset=subset,
    )
    result = _append_discard_stat(
        result,
        channel=channel,
        context_label=context_label,
        operation="row_filter",
        reason=reason,
        before=before,
        after=len(result),
        threshold=threshold,
        subset=subset,
    )
    _raise_if_threshold_exceeded(
        channel=channel,
        context_label=context_label,
        operation="row_filter",
        reason=reason,
        dropped=dropped,
        before=before,
        rate=rate,
        threshold=threshold,
    )
    return result


def latest_discard_stat(df: pd.DataFrame) -> dict[str, Any] | None:
    stats = df.attrs.get("discard_stats", [])
    if not stats:
        return None
    return stats[-1]
