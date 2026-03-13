from __future__ import annotations

import json
from pathlib import Path
import sys
import types

import pytest


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))


def _install_airflow_stub() -> None:
    airflow_module = sys.modules.get("airflow") or types.ModuleType("airflow")
    operators_module = sys.modules.get("airflow.operators") or types.ModuleType("airflow.operators")
    python_module = sys.modules.get("airflow.operators.python") or types.ModuleType("airflow.operators.python")

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

    python_module.PythonOperator = _DummyPythonOperator
    python_module.get_current_context = lambda: {}
    operators_module.python = python_module
    airflow_module.DAG = _DummyDAG
    airflow_module.operators = operators_module

    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.operators"] = operators_module
    sys.modules["airflow.operators.python"] = python_module


def _install_sqlalchemy_stub() -> None:
    if "sqlalchemy" in sys.modules:
        return

    sqlalchemy_module = types.ModuleType("sqlalchemy")
    sqlalchemy_module.create_engine = lambda *_args, **_kwargs: None
    sqlalchemy_module.text = lambda sql: sql
    sys.modules["sqlalchemy"] = sqlalchemy_module


_install_airflow_stub()
_install_sqlalchemy_stub()

import data_quality_checks as dq_module


class _DummyMetrics:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def set_counts(self, **kwargs):
        self.counts = kwargs


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConn:
    def execute(self, query):
        sql = str(query)
        if "FROM raw.head_to_head_fixtures h2h" in sql and "LEFT JOIN control.season_catalog sc" in sql:
            return _FakeResult([(1,)])
        return _FakeResult([])


class _FakeEngine:
    def begin(self):
        return self

    def __enter__(self):
        return _FakeConn()

    def __exit__(self, exc_type, exc, tb):
        return False


def test_data_quality_checks_include_h2h_and_pss_guardrails():
    check_names = {check["check_name"] for check in dq_module.CHECKS}

    assert "raw_head_to_head_orphan_fixture" in check_names
    assert "raw_head_to_head_outside_catalog" in check_names
    assert "raw_head_to_head_fixture_scope_mismatch" in check_names
    assert "raw_player_season_statistics_outside_catalog" in check_names
    assert "raw_player_season_statistics_lineups_coverage_gap" in check_names


def test_run_data_quality_checks_fails_when_new_h2h_check_detects_anomaly(monkeypatch):
    monkeypatch.setattr(dq_module, "create_engine", lambda *_args, **_kwargs: _FakeEngine())
    monkeypatch.setattr(dq_module, "_get_required_env", lambda _name: "postgresql://fake")
    monkeypatch.setattr(dq_module, "StepMetrics", _DummyMetrics)
    monkeypatch.setattr(dq_module, "log_event", lambda **_kwargs: None)

    with pytest.raises(ValueError, match="raw_head_to_head_outside_catalog"):
        dq_module.run_data_quality_checks()


def test_raw_checkpoint_wires_h2h_and_pss_suites():
    checkpoint_text = Path("quality/great_expectations/checkpoints/raw_checkpoint.yml").read_text(encoding="utf-8")

    assert "raw_head_to_head_fixtures_suite" in checkpoint_text
    assert "raw_player_season_statistics_suite" in checkpoint_text
    assert "SELECT * FROM raw.head_to_head_fixtures" in checkpoint_text
    assert "SELECT * FROM raw.player_season_statistics" in checkpoint_text


def test_expectation_suites_cover_semantic_identity_columns():
    h2h_suite = json.loads(
        Path("quality/great_expectations/expectations/raw_head_to_head_fixtures_suite.json").read_text(
            encoding="utf-8"
        )
    )
    pss_suite = json.loads(
        Path("quality/great_expectations/expectations/raw_player_season_statistics_suite.json").read_text(
            encoding="utf-8"
        )
    )

    h2h_expectations = {item["expectation_type"]: item["kwargs"] for item in h2h_suite["expectations"]}
    pss_expectations = {item["expectation_type"]: item["kwargs"] for item in pss_suite["expectations"]}

    assert h2h_suite["expectation_suite_name"] == "raw_head_to_head_fixtures_suite"
    assert pss_suite["expectation_suite_name"] == "raw_player_season_statistics_suite"
    assert h2h_expectations["expect_column_values_to_match_regex"]["column"] == "season_label"
    assert pss_expectations["expect_column_values_to_match_regex"]["column"] == "season_label"
    assert h2h_expectations["expect_column_values_to_be_in_set"]["value_set"] == ["sportmonks"]
    assert pss_expectations["expect_column_values_to_be_in_set"]["value_set"] == ["sportmonks"]
