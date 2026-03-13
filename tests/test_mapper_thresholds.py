from __future__ import annotations

from pathlib import Path
import sys

import pytest


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from common.mappers.player_season_statistics_mapper import build_player_season_statistics_dataframe
from common.mappers.statistics_mapper import build_statistics_dataframe


def test_build_statistics_dataframe_fails_when_required_key_drop_exceeds_threshold():
    payloads = [
        {
            "provider": "sportmonks",
            "source_params": {"fixture": 1000 + idx},
            "response": [
                {
                    "team": {"id": None if idx in {4, 5} else idx, "name": f"Team {idx}"},
                    "statistics": [{"type": "Ball Possession", "value": "50%"}],
                }
            ],
        }
        for idx in range(1, 11)
    ]

    with pytest.raises(RuntimeError, match="Taxa de descarte 20.0% excede threshold de 10%"):
        build_statistics_dataframe(payloads)


def test_build_player_season_statistics_dataframe_fails_when_required_key_drop_exceeds_threshold():
    payload = {
        "provider": "sportmonks",
        "response": [
            {
                "player": {"id": None if idx in {4, 5} else 1000 + idx},
                "team": {"id": 200 + idx},
                "season": {"id": 17160, "league_id": 301, "name": "2020/2021"},
                "position": {"name": "Midfielder"},
                "statistics": [],
            }
            for idx in range(1, 11)
        ],
    }

    with pytest.raises(RuntimeError, match="Taxa de descarte 20.0% excede threshold de 10%"):
        build_player_season_statistics_dataframe([payload])
