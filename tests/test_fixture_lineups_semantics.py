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

from common.services.warehouse_service import _enrich_lineups_semantics
from data_quality_checks import CHECKS


def test_enrich_lineups_semantics_populates_identity() -> None:
    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "fixture_id": 16744816,
                "team_id": 260,
                "lineup_id": 8880001,
            }
        ]
    )
    scope_df = pd.DataFrame(
        [
            {
                "fixture_id": 16744816,
                "provider": "sportmonks",
                "provider_league_id": 2,
                "competition_key": "champions_league",
                "season_label": "2020_21",
                "provider_season_id": 17299,
                "home_team_id": 260,
                "away_team_id": 261,
            }
        ]
    )

    enriched = _enrich_lineups_semantics(load_df, scope_df=scope_df, run_id="run-789")

    row = enriched.iloc[0]
    assert row["provider"] == "sportmonks"
    assert row["provider_league_id"] == 2
    assert row["competition_key"] == "champions_league"
    assert row["season_label"] == "2020_21"
    assert row["provider_season_id"] == 17299
    assert row["source_run_id"] == "run-789"


def test_enrich_lineups_semantics_remaps_unambiguous_fixture_team_drift() -> None:
    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "fixture_id": 19098756,
                "team_id": 6188,
                "lineup_id": 10046955912,
            },
            {
                "provider": "sportmonks",
                "fixture_id": 19098756,
                "team_id": 710,
                "lineup_id": 10046955913,
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

    enriched = _enrich_lineups_semantics(load_df, scope_df=scope_df, run_id="run-790")

    assert set(enriched["team_id"].tolist()) == {275822, 710}


def test_enrich_lineups_semantics_rejects_unresolved_fixture_team_drift() -> None:
    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "fixture_id": 19098756,
                "team_id": 6188,
                "lineup_id": 10046955912,
            },
            {
                "provider": "sportmonks",
                "fixture_id": 19098756,
                "team_id": 999999,
                "lineup_id": 10046955913,
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

    with pytest.raises(RuntimeError, match="Nao foi possivel resolver identidade semantica de lineups"):
        _enrich_lineups_semantics(load_df, scope_df=scope_df, run_id="run-791")


def test_enrich_lineups_semantics_rejects_unresolved_fixture_scope() -> None:
    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "fixture_id": 16744816,
                "team_id": 260,
                "lineup_id": 8880001,
            }
        ]
    )

    with pytest.raises(RuntimeError, match="Nao foi possivel resolver identidade semantica de lineups"):
        _enrich_lineups_semantics(load_df, scope_df=pd.DataFrame(), run_id="run-789")


def test_data_quality_checks_include_lineups_semantic_guards() -> None:
    check_names = {check["check_name"] for check in CHECKS}

    assert "raw_fixture_lineups_null_competition_key" in check_names
    assert "raw_fixture_lineups_null_season_label" in check_names
    assert "raw_fixture_lineups_null_provider_season_id" in check_names
    assert "raw_fixture_lineups_outside_catalog" in check_names
    assert "raw_fixture_lineups_team_scope_mismatch" in check_names


def test_lineups_starters_check_treats_ftp_as_final_status() -> None:
    starters_check = next(check for check in CHECKS if check["check_name"] == "raw_fixture_lineups_min_starters")

    assert "FTP" in starters_check["sql"]


def test_lineups_starters_check_has_specific_provider_caveat_exception() -> None:
    starters_check = next(check for check in CHECKS if check["check_name"] == "raw_fixture_lineups_min_starters")

    assert "provider_caveat_exceptions" in starters_check["sql"]
    assert "18809781::bigint" in starters_check["sql"]
    assert "6188::bigint" in starters_check["sql"]
    assert "results=45" in starters_check["sql"]
    assert "exc.fixture_id IS NULL" in starters_check["sql"]
    assert "< 11" in starters_check["sql"]


def test_raw_fixture_lineups_suite_requires_semantic_columns() -> None:
    expectation_suite = json.loads(
        Path("quality/great_expectations/expectations/raw_fixture_lineups_suite.json").read_text()
    )
    not_null_columns = {
        expectation["kwargs"]["column"]
        for expectation in expectation_suite["expectations"]
        if expectation["expectation_type"] == "expect_column_values_to_not_be_null"
    }

    assert {"competition_key", "season_label", "provider_season_id"} <= not_null_columns
