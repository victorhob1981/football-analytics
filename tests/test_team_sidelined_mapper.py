from __future__ import annotations

from pathlib import Path
import sys


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))


from common.mappers.team_sidelined_mapper import (  # noqa: E402
    TEAM_SIDELINED_COLUMNS,
    build_team_sidelined_dataframe,
)


def test_build_team_sidelined_dataframe_accepts_empty_provider_responses() -> None:
    payloads = [
        {"provider": "sportmonks", "response": []},
        {"provider": "sportmonks", "response": []},
    ]

    df = build_team_sidelined_dataframe(payloads)

    assert df.empty
    assert list(df.columns) == TEAM_SIDELINED_COLUMNS
