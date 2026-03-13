from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from common.mappers.lineups_mapper import build_fixture_lineups_dataframe


def test_build_fixture_lineups_dataframe_keeps_null_player_id_and_dedupes_by_lineup_id():
    base_row = {
        "fixture_id": 1001,
        "team": {"id": 10},
        "player": {"id": None, "name": "Sem ID"},
        "position": {"id": 24, "name": "Goalkeeper"},
        "lineup_type_id": 11,
        "formation_field": "1:1",
        "formation_position": 1,
        "details": [],
    }
    payload = {
        "provider": "sportmonks",
        "response": [
            {**base_row, "lineup_id": 999001 + idx, "jersey_number": idx}
            for idx in range(1, 11)
        ]
        + [
            {**base_row, "lineup_id": 999010, "jersey_number": 12},
        ],
    }

    df = build_fixture_lineups_dataframe([payload])

    assert len(df) == 10
    row = df.loc[df["lineup_id"] == 999010].iloc[0]
    assert pd.isna(row["player_id"])
    assert int(row["jersey_number"]) == 12
    assert df.attrs["discard_stats"][-1]["dropped"] == 1


def test_build_fixture_lineups_dataframe_generates_stable_fallback_lineup_id():
    payload = {
        "provider": "sportmonks",
        "response": [
            {
                "fixture_id": 2002,
                "team": {"id": 20},
                "player": {"id": 300},
                "lineup_id": None,
                "position": {"id": 26, "name": "Midfielder"},
                "lineup_type_id": 11,
                "formation_field": "3:2",
                "formation_position": 7,
                "jersey_number": 8,
                "details": [],
            }
        ],
    }

    df_first = build_fixture_lineups_dataframe([payload])
    df_second = build_fixture_lineups_dataframe([payload])

    assert len(df_first) == 1
    assert len(df_second) == 1
    first_lineup_id = int(df_first.iloc[0]["lineup_id"])
    second_lineup_id = int(df_second.iloc[0]["lineup_id"])
    assert first_lineup_id > 0
    assert first_lineup_id == second_lineup_id
