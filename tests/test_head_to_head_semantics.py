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

from common.services.warehouse_service import _enrich_head_to_head_semantics
from data_quality_checks import CHECKS


def test_enrich_head_to_head_semantics_populates_identity() -> None:
    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "pair_team_id": 50,
                "pair_opponent_id": 529,
                "fixture_id": 16744816,
                "league_id": 2,
                "season_id": 17299,
                "match_date": "2020-08-08T19:00:00+00:00",
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
                "fixture_date": "2020-08-08",
                "scope_pair_team_id": 50,
                "scope_pair_opponent_id": 529,
            }
        ]
    )

    enriched = _enrich_head_to_head_semantics(load_df, scope_df=scope_df, run_id="run-h2h")

    row = enriched.iloc[0]
    assert row["competition_key"] == "champions_league"
    assert row["season_label"] == "2020_21"
    assert row["provider_season_id"] == 17299
    assert row["provider_league_id"] == 2
    assert row["source_run_id"] == "run-h2h"


def test_enrich_head_to_head_semantics_rejects_out_of_scope_fixture() -> None:
    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "pair_team_id": 50,
                "pair_opponent_id": 529,
                "fixture_id": 19296333,
                "league_id": 2,
                "season_id": 23619,
                "match_date": "2025-01-29T20:00:00+00:00",
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
                "fixture_date": "2020-08-08",
                "scope_pair_team_id": 50,
                "scope_pair_opponent_id": 529,
            }
        ]
    )

    with pytest.raises(RuntimeError, match="Nao foi possivel resolver identidade semantica de head_to_head"):
        _enrich_head_to_head_semantics(load_df, scope_df=scope_df, run_id="run-h2h")


def test_enrich_head_to_head_semantics_drops_rows_without_authoritative_fixture_match() -> None:
    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "pair_team_id": 50,
                "pair_opponent_id": 529,
                "fixture_id": 16744816,
                "league_id": 2,
                "season_id": 17299,
                "match_date": "2020-08-08T19:00:00+00:00",
            },
            {
                "provider": "sportmonks",
                "pair_team_id": 50,
                "pair_opponent_id": 529,
                "fixture_id": 19296333,
                "league_id": 2,
                "season_id": 17299,
                "match_date": "2025-01-29T20:00:00+00:00",
            },
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
                "fixture_date": "2020-08-08",
                "scope_pair_team_id": 50,
                "scope_pair_opponent_id": 529,
            }
        ]
    )

    enriched = _enrich_head_to_head_semantics(load_df, scope_df=scope_df, run_id="run-h2h")

    assert len(enriched) == 1
    row = enriched.iloc[0]
    assert row["fixture_id"] == 16744816
    assert row["competition_key"] == "champions_league"
    assert row["season_label"] == "2020_21"


def test_data_quality_checks_and_ge_cover_head_to_head() -> None:
    check_names = {check["check_name"] for check in CHECKS}
    assert "raw_head_to_head_orphan_fixture" in check_names
    assert "raw_head_to_head_null_competition_key" in check_names
    assert "raw_head_to_head_null_season_label" in check_names
    assert "raw_head_to_head_null_provider_season_id" in check_names
    assert "raw_head_to_head_outside_catalog" in check_names
    assert "raw_head_to_head_fixture_scope_mismatch" in check_names

    checkpoint_text = Path("platform/quality/great_expectations/checkpoints/raw_checkpoint.yml").read_text()
    assert "data_asset_name: raw_head_to_head_fixtures_asset" in checkpoint_text
    assert "expectation_suite_name: raw_head_to_head_fixtures_suite" in checkpoint_text

    expectation_suite = json.loads(
        Path("platform/quality/great_expectations/expectations/raw_head_to_head_fixtures_suite.json").read_text()
    )
    not_null_columns = {
        expectation["kwargs"]["column"]
        for expectation in expectation_suite["expectations"]
        if expectation["expectation_type"] == "expect_column_values_to_not_be_null"
    }
    assert {"competition_key", "season_label", "provider_season_id"} <= not_null_columns
