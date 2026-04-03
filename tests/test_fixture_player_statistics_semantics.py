from __future__ import annotations

from pathlib import Path
import sys
import types

import pandas as pd
import pytest


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))


def _install_airflow_stub() -> None:
    if "airflow.operators.python" in sys.modules:
        return

    airflow_module = types.ModuleType("airflow")
    operators_module = types.ModuleType("airflow.operators")
    python_module = types.ModuleType("airflow.operators.python")

    class _DummyDAG:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _DummyPythonOperator:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    airflow_module.DAG = _DummyDAG
    python_module.PythonOperator = _DummyPythonOperator
    python_module.get_current_context = lambda: {}
    operators_module.python = python_module
    airflow_module.operators = operators_module

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.operators"] = operators_module
    sys.modules["airflow.operators.python"] = python_module


def _install_boto3_stub() -> None:
    if "boto3" in sys.modules:
        return

    boto3_module = types.ModuleType("boto3")
    boto3_module.client = lambda *_args, **_kwargs: None
    sys.modules["boto3"] = boto3_module


def _install_sqlalchemy_stub() -> None:
    if "sqlalchemy" in sys.modules:
        return

    sqlalchemy_module = types.ModuleType("sqlalchemy")
    sqlalchemy_module.create_engine = lambda *_args, **_kwargs: None
    sqlalchemy_module.text = lambda sql: sql
    sys.modules["sqlalchemy"] = sqlalchemy_module


_install_airflow_stub()
_install_boto3_stub()
_install_sqlalchemy_stub()

from common.services.warehouse_service import _enrich_fixture_player_statistics_semantics


def test_enrich_fixture_player_statistics_semantics_remaps_unambiguous_fixture_team_drift() -> None:
    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "fixture_id": 19098756,
                "team_id": 6188,
                "player_id": 101,
                "statistics": "[]",
                "payload": "{}",
                "ingested_run": "run-1",
            },
            {
                "provider": "sportmonks",
                "fixture_id": 19098756,
                "team_id": 710,
                "player_id": 202,
                "statistics": "[]",
                "payload": "{}",
                "ingested_run": "run-1",
            },
        ]
    )
    scope_df = pd.DataFrame(
        [
            {
                "fixture_id": 19098756,
                "provider": "sportmonks",
                "provider_league_id": 651,
                "competition_key": "brasileirao_b",
                "season_label": "2024",
                "provider_season_id": 23291,
                "home_team_id": 275822,
                "away_team_id": 710,
            }
        ]
    )

    enriched = _enrich_fixture_player_statistics_semantics(load_df, scope_df=scope_df, run_id="run-2")

    assert set(enriched["team_id"].tolist()) == {275822, 710}
    assert enriched.loc[enriched["player_id"] == 101, "team_id"].iloc[0] == 275822


def test_enrich_fixture_player_statistics_semantics_fills_provider_from_scope() -> None:
    load_df = pd.DataFrame(
        [
            {
                "provider": pd.NA,
                "fixture_id": 19098756,
                "team_id": 710,
                "player_id": 202,
                "statistics": "[]",
                "payload": "{}",
                "ingested_run": "run-3",
            }
        ]
    )
    scope_df = pd.DataFrame(
        [
            {
                "fixture_id": 19098756,
                "provider": "sportmonks",
                "provider_league_id": 651,
                "competition_key": "brasileirao_b",
                "season_label": "2024",
                "provider_season_id": 23291,
                "home_team_id": 275822,
                "away_team_id": 710,
            }
        ]
    )

    enriched = _enrich_fixture_player_statistics_semantics(load_df, scope_df=scope_df, run_id="run-4")

    assert enriched.iloc[0]["provider"] == "sportmonks"


def test_enrich_fixture_player_statistics_semantics_rejects_unresolved_fixture_team_drift() -> None:
    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "fixture_id": 19098756,
                "team_id": 6188,
                "player_id": 101,
                "statistics": "[]",
                "payload": "{}",
                "ingested_run": "run-5",
            },
            {
                "provider": "sportmonks",
                "fixture_id": 19098756,
                "team_id": 999999,
                "player_id": 202,
                "statistics": "[]",
                "payload": "{}",
                "ingested_run": "run-5",
            },
        ]
    )
    scope_df = pd.DataFrame(
        [
            {
                "fixture_id": 19098756,
                "provider": "sportmonks",
                "provider_league_id": 651,
                "competition_key": "brasileirao_b",
                "season_label": "2024",
                "provider_season_id": 23291,
                "home_team_id": 275822,
                "away_team_id": 710,
            }
        ]
    )

    with pytest.raises(RuntimeError, match="fixture_player_statistics"):
        _enrich_fixture_player_statistics_semantics(load_df, scope_df=scope_df, run_id="run-6")

