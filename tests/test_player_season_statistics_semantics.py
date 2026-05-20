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
    airflow_module = sys.modules.get("airflow", types.ModuleType("airflow"))
    operators_module = sys.modules.get("airflow.operators", types.ModuleType("airflow.operators"))
    python_module = sys.modules.get("airflow.operators.python", types.ModuleType("airflow.operators.python"))

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

from common.services.warehouse_service import _enrich_player_season_statistics_semantics
from data_quality_checks import CHECKS


def test_enrich_player_season_statistics_semantics_populates_identity():
    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "player_id": 100391,
                "player_nationality": "France",
                "season_id": 17160,
                "team_id": 686,
                "league_id": 301,
                "season_name": "2020/2021",
            }
        ]
    )
    scope_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "provider_league_id": 301,
                "provider_season_id": 17160,
                "competition_key": "ligue_1",
                "season_label": "2020_21",
            }
        ]
    )

    enriched = _enrich_player_season_statistics_semantics(load_df, scope_df=scope_df, run_id="run-123")

    row = enriched.iloc[0]
    assert row["provider_league_id"] == 301
    assert row["competition_key"] == "ligue_1"
    assert row["season_label"] == "2020_21"
    assert row["provider_season_id"] == 17160
    assert row["player_nationality"] == "France"
    assert row["source_run_id"] == "run-123"


def test_enrich_player_season_statistics_semantics_rejects_unresolved_scope():
    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "player_id": 100391,
                "season_id": 17160,
                "team_id": 686,
                "league_id": 301,
                "season_name": "2020/2021",
            }
        ]
    )

    with pytest.raises(RuntimeError, match="Nao foi possivel resolver identidade semantica"):
        _enrich_player_season_statistics_semantics(load_df, scope_df=pd.DataFrame(), run_id="run-123")


def test_data_quality_checks_include_pss_semantic_guards():
    check_names = {check["check_name"] for check in CHECKS}

    assert "raw_player_season_statistics_null_competition_key" in check_names
    assert "raw_player_season_statistics_null_season_label" in check_names
    assert "raw_player_season_statistics_null_provider_season_id" in check_names
    assert "raw_player_season_statistics_outside_catalog" in check_names
    assert "raw_player_season_statistics_semantically_invisible_scope" in check_names


def test_raw_checkpoint_and_suite_cover_player_season_statistics():
    checkpoint_text = Path("platform/quality/great_expectations/checkpoints/raw_checkpoint.yml").read_text()
    assert "data_asset_name: raw_player_season_statistics_asset" in checkpoint_text
    assert "expectation_suite_name: raw_player_season_statistics_suite" in checkpoint_text

    expectation_suite = json.loads(
        Path("platform/quality/great_expectations/expectations/raw_player_season_statistics_suite.json").read_text()
    )
    not_null_columns = {
        expectation["kwargs"]["column"]
        for expectation in expectation_suite["expectations"]
        if expectation["expectation_type"] == "expect_column_values_to_not_be_null"
    }

    assert {"competition_key", "season_label", "provider_season_id"} <= not_null_columns
