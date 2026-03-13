from __future__ import annotations

from pathlib import Path
import sys

import pytest


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from common.runtime import resolve_runtime_params


def test_runtime_accepts_single_year_operational_season():
    runtime = resolve_runtime_params(
        {
            "params": {},
            "dag_run": type(
                "DagRun",
                (),
                {"conf": {"provider": "sportmonks", "league_id": 648, "season": 2024}},
            )(),
        }
    )

    assert runtime["season"] == 2024
    assert runtime["season_label"] is None
    assert runtime["provider_season_id"] is None


def test_runtime_accepts_canonical_season_label_input():
    runtime = resolve_runtime_params(
        {
            "params": {},
            "dag_run": type(
                "DagRun",
                (),
                {"conf": {"provider": "sportmonks", "league_id": 8, "season": "2020_21"}},
            )(),
        }
    )

    assert runtime["season"] == 2020
    assert runtime["season_label"] == "2020_21"
    assert runtime["provider_season_id"] is None


def test_runtime_accepts_canonical_cross_year_input_with_provider_season_id():
    runtime = resolve_runtime_params(
        {
            "params": {},
            "dag_run": type(
                "DagRun",
                (),
                {
                    "conf": {
                        "provider": "sportmonks",
                        "league_id": 8,
                        "season": 2020,
                        "season_label": "2020_21",
                        "provider_season_id": 17160,
                    }
                },
            )(),
        }
    )

    assert runtime["season"] == 2020
    assert runtime["season_label"] == "2020_21"
    assert runtime["provider_season_id"] == 17160


def test_runtime_rejects_implausible_operational_season_year():
    with pytest.raises(ValueError, match="Use o ano da temporada"):
        resolve_runtime_params(
            {
                "params": {},
                "dag_run": type(
                    "DagRun",
                    (),
                    {"conf": {"provider": "sportmonks", "league_id": 8, "season": 202021}},
                )(),
            }
        )


def test_runtime_rejects_inconsistent_season_and_season_label():
    with pytest.raises(ValueError):
        resolve_runtime_params(
            {
                "params": {},
                "dag_run": type(
                    "DagRun",
                    (),
                    {"conf": {"provider": "sportmonks", "league_id": 8, "season": 2021, "season_label": "2020_21"}},
                )(),
            }
        )
