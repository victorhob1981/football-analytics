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

from common.services.warehouse_service import EVENTS_TARGET_COLUMNS, _enrich_match_events_semantics


def test_enrich_match_events_semantics_populates_identity():
    load_df = pd.DataFrame(
        [
            {
                "event_id": "evt-1",
                "fixture_id": 17958481,
                "season": 2021,
                "time_elapsed": 62,
                "team_id": 3422,
            }
        ]
    )
    scope_df = pd.DataFrame(
        [
            {
                "fixture_id": 17958481,
                "provider": "sportmonks",
                "provider_league_id": 648,
                "competition_key": "brasileirao_a",
                "season_label": "2021",
                "provider_season_id": 18215,
            }
        ]
    )

    enriched = _enrich_match_events_semantics(load_df, scope_df=scope_df, run_id="run-789")

    row = enriched.iloc[0]
    assert row["provider"] == "sportmonks"
    assert row["provider_league_id"] == 648
    assert row["competition_key"] == "brasileirao_a"
    assert row["season_label"] == "2021"
    assert row["provider_season_id"] == 18215
    assert row["source_run_id"] == "run-789"


def test_enrich_match_events_semantics_rejects_unresolved_fixture_scope():
    load_df = pd.DataFrame(
        [
            {
                "event_id": "evt-1",
                "fixture_id": 17958481,
                "season": 2021,
            }
        ]
    )

    with pytest.raises(RuntimeError, match="Nao foi possivel resolver identidade semantica de match_events"):
        _enrich_match_events_semantics(load_df, scope_df=pd.DataFrame(), run_id="run-789")


def test_match_events_target_columns_include_semantic_identity():
    assert {
        "provider",
        "provider_league_id",
        "competition_key",
        "season_label",
        "provider_season_id",
        "source_run_id",
    } <= set(EVENTS_TARGET_COLUMNS)
