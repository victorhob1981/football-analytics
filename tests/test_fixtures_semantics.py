from __future__ import annotations

from pathlib import Path
import json
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

from common.services.warehouse_service import _enrich_fixtures_semantics
from data_quality_checks import CHECKS


def test_enrich_fixtures_semantics_populates_identity() -> None:
    load_df = pd.DataFrame(
        [
            {
                "fixture_id": 17958490,
                "source_provider": "sportmonks",
                "league_id": 648,
                "season": 2021,
            }
        ]
    )
    scope_df = pd.DataFrame(
        [
            {
                "source_provider": "sportmonks",
                "league_id": 648,
                "operational_season": 2021,
                "competition_key": "brasileirao_a",
                "competition_type": "league",
                "season_label": "2021",
                "provider_season_id": 18215,
                "season_start_date": "2021-05-30",
                "season_end_date": "2021-12-10",
            }
        ]
    )

    enriched = _enrich_fixtures_semantics(load_df, scope_df=scope_df, run_id="run-fixtures-1")

    row = enriched.iloc[0]
    assert row["competition_key"] == "brasileirao_a"
    assert row["competition_type"] == "league"
    assert row["season_label"] == "2021"
    assert row["provider_season_id"] == 18215
    assert row["season_name"] == "2021"
    assert str(row["season_start_date"]) == "2021-05-30"
    assert str(row["season_end_date"]) == "2021-12-10"
    assert row["source_run_id"] == "run-fixtures-1"


def test_enrich_fixtures_semantics_rejects_unresolved_scope() -> None:
    load_df = pd.DataFrame(
        [
            {
                "fixture_id": 17958490,
                "source_provider": "sportmonks",
                "league_id": 648,
                "season": 2021,
            }
        ]
    )

    with pytest.raises(RuntimeError, match="Nao foi possivel resolver identidade semantica de fixtures"):
        _enrich_fixtures_semantics(load_df, scope_df=pd.DataFrame(), run_id="run-fixtures-2")


def test_data_quality_checks_include_fixture_semantic_guards() -> None:
    check_names = {check["check_name"] for check in CHECKS}

    assert "raw_fixtures_null_competition_key" in check_names
    assert "raw_fixtures_null_season_label" in check_names
    assert "raw_fixtures_null_provider_season_id" in check_names
    assert "raw_fixtures_outside_catalog" in check_names


def test_raw_fixtures_suite_requires_semantic_columns() -> None:
    expectation_suite = json.loads(
        Path("quality/great_expectations/expectations/raw_fixtures_suite.json").read_text()
    )
    not_null_columns = {
        expectation["kwargs"]["column"]
        for expectation in expectation_suite["expectations"]
        if expectation["expectation_type"] == "expect_column_values_to_not_be_null"
    }

    assert {"competition_key", "season_label", "provider_season_id"} <= not_null_columns
