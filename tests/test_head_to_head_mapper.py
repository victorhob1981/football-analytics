from __future__ import annotations

from pathlib import Path
import sys


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from common.mappers.head_to_head_mapper import build_head_to_head_fixtures_dataframe


def test_build_head_to_head_fixtures_dataframe_dedupes_by_natural_grain():
    payload = {
        "provider": "sportmonks",
        "source_params": {"pair_team_id": 10, "pair_opponent_id": 11},
        "response": [
            {
                "fixture": {"id": 1001, "date": "2024-07-01 20:00:00"},
                "league": {"id": 648, "season_id": 23265},
                "teams": {"home": {"id": 10}, "away": {"id": 11}},
                "goals": {"home": 2, "away": 1},
            },
            {
                "fixture": {"id": 1001, "date": "2024-07-01 20:00:00"},
                "league": {"id": 648, "season_id": 23265},
                "teams": {"home": {"id": 10}, "away": {"id": 11}},
                "goals": {"home": 2, "away": 1},
            },
        ],
    }

    df = build_head_to_head_fixtures_dataframe([payload])

    assert len(df) == 1
    row = df.iloc[0]
    assert int(row["pair_team_id"]) == 10
    assert int(row["pair_opponent_id"]) == 11
    assert int(row["fixture_id"]) == 1001
    assert int(row["league_id"]) == 648
    assert int(row["season_id"]) == 23265
    assert int(row["home_goals"]) == 2
    assert int(row["away_goals"]) == 1
