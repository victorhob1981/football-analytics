from __future__ import annotations

from pathlib import Path
import sys
import types

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

from common.services.ingestion_service import (
    _calculate_next_cursor,
    _resolve_pending_fixture_ids,
    _resolve_statistics_targets,
)


class _FakeS3Client:
    def __init__(self, pages: list[dict]) -> None:
        self._pages = pages
        self._index = 0

    def list_objects_v2(self, **_kwargs):
        page = self._pages[self._index]
        self._index += 1
        return page


class _FailOnS3Call:
    def list_objects_v2(self, **_kwargs):
        raise AssertionError("Nao deveria chamar S3 quando cursor existe")


class _FakeDagRun:
    def __init__(self, conf: dict):
        self.conf = conf


def test_resolve_pending_prefers_cursor_without_full_scan():
    pending, ingested, strategy = _resolve_pending_fixture_ids(
        s3_client=_FailOnS3Call(),
        fixture_ids=[10, 20, 30],
        skip_ingested=True,
        s3_prefix="statistics/league=71/season=2024/",
        cursor=20,
    )

    assert pending == [30]
    assert ingested == set()
    assert strategy == "sync_state_cursor>20"


def test_resolve_pending_uses_full_scan_when_cursor_missing():
    pages = [
        {
            "Contents": [
                {"Key": "statistics/league=71/season=2024/fixture_id=10/run=1/data.json"},
                {"Key": "statistics/league=71/season=2024/fixture_id=30/run=1/data.json"},
            ],
            "IsTruncated": True,
            "NextContinuationToken": "next-page",
        },
        {
            "Contents": [
                {"Key": "statistics/league=71/season=2024/fixture_id=40/run=1/data.json"},
            ],
            "IsTruncated": False,
        },
    ]
    pending, ingested, strategy = _resolve_pending_fixture_ids(
        s3_client=_FakeS3Client(pages),
        fixture_ids=[10, 20, 30, 40],
        skip_ingested=True,
        s3_prefix="statistics/league=71/season=2024/",
        cursor=None,
    )

    assert pending == [20]
    assert ingested == {10, 30, 40}
    assert strategy == "full_scan_s3"


def test_calculate_next_cursor_advances_only_on_successful_prefix():
    assert _calculate_next_cursor(
        current_cursor=100,
        pending_fixture_ids=[120, 140, 160],
        attempt_success_flags=[True, True, False],
    ) == 140

    assert _calculate_next_cursor(
        current_cursor=100,
        pending_fixture_ids=[120, 140, 160],
        attempt_success_flags=[False, True, True],
    ) == 100

    assert _calculate_next_cursor(
        current_cursor=None,
        pending_fixture_ids=[120, 140],
        attempt_success_flags=[True, True],
    ) == 140


def test_resolve_statistics_targets_backfill_explicit_fixture_ids():
    context = {
        "params": {"league_id": 71, "season": 2024},
        "dag_run": _FakeDagRun({"mode": "backfill", "fixture_ids": [2003, 2001, 2003, 2002]}),
    }

    def _fail_fetch(_league_id: int, _season: int):
        raise AssertionError("Nao deveria buscar fixture_ids por temporada quando fixture_ids explicitos foram enviados")

    targets = _resolve_statistics_targets(
        context=context,
        default_league_id=71,
        default_season=2024,
        fetch_finished_fixture_ids=_fail_fetch,
    )

    assert targets["mode"] == "backfill"
    assert targets["target_source"] == "explicit_fixture_ids"
    assert targets["fixture_ids"] == [2001, 2002, 2003]
    assert targets["league_id"] == 71
    assert targets["season"] == 2024


def test_resolve_statistics_targets_backfill_season_scope():
    context = {
        "params": {"league_id": 71, "season": 2024},
        "dag_run": _FakeDagRun({"mode": "backfill", "season_id": 2023, "league_id": 39}),
    }

    called = {}

    def _fake_fetch(league_id: int, season: int):
        called["league_id"] = league_id
        called["season"] = season
        return [5001, 5002]

    targets = _resolve_statistics_targets(
        context=context,
        default_league_id=71,
        default_season=2024,
        fetch_finished_fixture_ids=_fake_fetch,
    )

    assert called == {"league_id": 39, "season": 2023}
    assert targets["mode"] == "backfill"
    assert targets["target_source"] == "season_scope"
    assert targets["fixture_ids"] == [5001, 5002]
    assert targets["league_id"] == 39
    assert targets["season"] == 2023


def test_resolve_statistics_targets_rejects_fixture_ids_without_backfill():
    context = {
        "params": {"league_id": 71, "season": 2024},
        "dag_run": _FakeDagRun({"mode": "incremental", "fixture_ids": [123]}),
    }

    with pytest.raises(ValueError, match="fixture_ids so pode ser usado com mode='backfill'"):
        _resolve_statistics_targets(
            context=context,
            default_league_id=71,
            default_season=2024,
            fetch_finished_fixture_ids=lambda _league_id, _season: [1, 2],
        )


def test_resolve_pending_backfill_prefers_sync_state_even_without_cursor():
    pending, ingested, strategy = _resolve_pending_fixture_ids(
        s3_client=_FailOnS3Call(),
        fixture_ids=[10, 20, 30],
        skip_ingested=True,
        s3_prefix="statistics/league=71/season=2024/",
        cursor=None,
        cursor_only=True,
    )

    assert pending == [10, 20, 30]
    assert ingested == set()
    assert strategy == "sync_state_only_cursor_missing"
