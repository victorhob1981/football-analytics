from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import pytest


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from common.discard_guardrails import drop_duplicates_with_threshold, dropna_with_threshold, filter_with_threshold


def test_dropna_with_threshold_small_discard_passes_and_records_metadata(capsys):
    df = pd.DataFrame(
        {
            "fixture_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, None],
            "team_id": [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
        }
    )

    result = dropna_with_threshold(df, subset=["fixture_id", "team_id"], context_label="statistics")

    assert len(result) == 10
    assert result.attrs["discard_stats"][-1]["dropped"] == 1
    assert result.attrs["discard_stats"][-1]["discard_rate"] < 0.10
    assert "[mapper:statistics] 1/11 rows descartadas" in capsys.readouterr().out


def test_drop_duplicates_with_threshold_small_discard_passes_and_records_metadata(capsys):
    df = pd.DataFrame(
        {
            "provider": ["sportmonks"] * 11,
            "fixture_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1010],
            "team_id": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            "lineup_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10],
        }
    )

    result = drop_duplicates_with_threshold(
        df,
        subset=["provider", "fixture_id", "team_id", "lineup_id"],
        context_label="lineups",
    )

    assert len(result) == 10
    assert result.attrs["discard_stats"][-1]["reason"] == "duplicate_grain"
    assert "[mapper:lineups] 1/11 rows descartadas" in capsys.readouterr().out


def test_dropna_with_threshold_high_discard_fails():
    df = pd.DataFrame(
        {
            "fixture_id": [1, 2, None, None, 5],
            "team_id": [10, 20, 30, None, 50],
        }
    )

    with pytest.raises(RuntimeError, match="Taxa de descarte 40.0% excede threshold de 10%"):
        dropna_with_threshold(df, subset=["fixture_id", "team_id"], context_label="statistics")


def test_filter_with_threshold_high_discard_fails_for_loader():
    df = pd.DataFrame(
        {
            "fixture_id": [1, None, None, 4],
            "team_id": [11, 12, None, 14],
        }
    )

    with pytest.raises(RuntimeError, match="Taxa de descarte 50.0% excede threshold de 5%"):
        filter_with_threshold(
            df,
            keep_mask=df["fixture_id"].notna(),
            context_label="raw.match_statistics",
            reason="null_conflict_key",
            subset=["fixture_id", "team_id"],
            channel="loader",
        )
