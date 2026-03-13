from __future__ import annotations

from pathlib import Path
import sys


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from common.mappers.head_to_head_mapper import build_head_to_head_fixtures_dataframe


def test_build_head_to_head_fixtures_dataframe_dedupes_by_natural_grain():
    unique_rows = [
        {
            "fixture": {"id": 1000 + idx, "date": "2024-07-01 20:00:00"},
            "league": {"id": 648, "season_id": 23265},
            "teams": {"home": {"id": 10}, "away": {"id": 11}},
            "goals": {"home": 2, "away": 1},
        }
        for idx in range(1, 11)
    ]
    payload = {
        "provider": "sportmonks",
        "source_params": {"pair_team_id": 10, "pair_opponent_id": 11},
        "response": unique_rows + [unique_rows[-1]],
    }

    df = build_head_to_head_fixtures_dataframe([payload])

    assert len(df) == 10
    row = df.loc[df["fixture_id"] == 1010].iloc[0]
    assert int(row["pair_team_id"]) == 10
    assert int(row["pair_opponent_id"]) == 11
    assert int(row["fixture_id"]) == 1010
    assert int(row["league_id"]) == 648
    assert int(row["season_id"]) == 23265
    assert int(row["home_goals"]) == 2
    assert int(row["away_goals"]) == 1
    assert df.attrs["discard_stats"][-1]["dropped"] == 1
