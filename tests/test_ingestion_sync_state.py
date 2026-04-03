from __future__ import annotations

from datetime import date
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

from common.services.ingestion_service import (
    _calculate_next_cursor,
    _fetch_finished_fixture_ids,
    _missing_fixture_ids_from_multi_enrichments,
    _resolve_pending_fixture_ids_for_complete_artifacts,
    _resolve_pending_fixture_ids,
    _resolve_statistics_targets,
    ingest_fixture_enrichments_raw,
)
from common.fixture_status import FINAL_STATUSES_SQL


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


class _FakeQueryResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _FakeEngine:
    def __init__(self, rows):
        self._rows = rows
        self.executed_sql = None
        self.executed_params = None

    def begin(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.executed_sql = sql
        self.executed_params = params
        return _FakeQueryResult(self._rows)


def test_fetch_finished_fixture_ids_uses_shared_final_statuses():
    engine = _FakeEngine([(101,), (202,)])

    fixture_ids = _fetch_finished_fixture_ids(engine, league_id=654, season=2024)

    assert fixture_ids == [101, 202]
    assert engine.executed_params == {"league_id": 654, "season": 2024}
    assert f"status_short IN ({FINAL_STATUSES_SQL})" in engine.executed_sql
    assert "AWAR" not in engine.executed_sql


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


def test_fixture_enrichments_only_events_does_not_mark_fixture_complete(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    prefixes = [
        "events/league=71/season=2024/",
        "statistics/league=71/season=2024/",
        "lineups/league=71/season=2024/",
        "fixture_player_statistics/league=71/season=2024/",
    ]
    ingested_by_prefix = {
        prefixes[0]: {10},
        prefixes[1]: set(),
        prefixes[2]: set(),
        prefixes[3]: set(),
    }
    monkeypatch.setattr(
        ingestion_service_module,
        "_list_ingested_fixture_ids",
        lambda _s3_client, *, prefix: set(ingested_by_prefix[prefix]),
    )

    pending, completed, strategy = _resolve_pending_fixture_ids_for_complete_artifacts(
        s3_client=object(),
        fixture_ids=[10],
        skip_ingested=True,
        required_s3_prefixes=prefixes,
        cursor=None,
    )

    assert pending == [10]
    assert completed == set()
    assert strategy == "required_prefixes_full_scan"


def test_fixture_enrichments_all_artifacts_mark_fixture_complete(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    prefixes = [
        "events/league=71/season=2024/",
        "statistics/league=71/season=2024/",
        "lineups/league=71/season=2024/",
        "fixture_player_statistics/league=71/season=2024/",
    ]
    ingested_by_prefix = {prefix: {10, 20} for prefix in prefixes}
    monkeypatch.setattr(
        ingestion_service_module,
        "_list_ingested_fixture_ids",
        lambda _s3_client, *, prefix: set(ingested_by_prefix[prefix]),
    )

    pending, completed, strategy = _resolve_pending_fixture_ids_for_complete_artifacts(
        s3_client=object(),
        fixture_ids=[10, 20],
        skip_ingested=True,
        required_s3_prefixes=prefixes,
        cursor=None,
    )

    assert pending == []
    assert completed == {10, 20}
    assert strategy == "required_prefixes_full_scan"


def test_fixture_enrichments_partial_artifacts_return_to_pending(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    prefixes = [
        "events/league=71/season=2024/",
        "statistics/league=71/season=2024/",
        "lineups/league=71/season=2024/",
        "fixture_player_statistics/league=71/season=2024/",
    ]
    ingested_by_prefix = {
        prefixes[0]: {10, 20, 30},
        prefixes[1]: {20, 30},
        prefixes[2]: {20, 30},
        prefixes[3]: {20, 30},
    }
    monkeypatch.setattr(
        ingestion_service_module,
        "_list_ingested_fixture_ids",
        lambda _s3_client, *, prefix: set(ingested_by_prefix[prefix]),
    )

    pending, completed, strategy = _resolve_pending_fixture_ids_for_complete_artifacts(
        s3_client=object(),
        fixture_ids=[10, 20, 30],
        skip_ingested=True,
        required_s3_prefixes=prefixes,
        cursor=30,
    )

    assert pending == [10]
    assert completed == {20, 30}
    assert strategy == "required_prefixes_full_scan+cursor=30"


def test_fixture_enrichments_replay_keeps_complete_fixtures_skippable(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    prefixes = [
        "events/league=71/season=2024/",
        "statistics/league=71/season=2024/",
        "lineups/league=71/season=2024/",
        "fixture_player_statistics/league=71/season=2024/",
    ]
    ingested_by_prefix = {prefix: {10, 20} for prefix in prefixes}
    monkeypatch.setattr(
        ingestion_service_module,
        "_list_ingested_fixture_ids",
        lambda _s3_client, *, prefix: set(ingested_by_prefix[prefix]),
    )

    pending, completed, strategy = _resolve_pending_fixture_ids_for_complete_artifacts(
        s3_client=object(),
        fixture_ids=[10, 20, 30],
        skip_ingested=True,
        required_s3_prefixes=prefixes,
        cursor=20,
    )

    assert pending == [30]
    assert completed == {10, 20}
    assert strategy == "required_prefixes_full_scan+cursor=20"


def test_missing_fixture_ids_from_multi_enrichments_detects_absent_fixtures():
    missing = _missing_fixture_ids_from_multi_enrichments(
        requested_fixture_ids=[10, 20, 30],
        enrichments_map={10: {"match_events": {}}, 30: {"match_events": {}}},
    )

    assert missing == [20]


class _FakeMetric:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def set_counts(self, **_kwargs):
        return None


class _CapturingMetric(_FakeMetric):
    def __init__(self, exits: list[dict]) -> None:
        self._exits = exits

    def __exit__(self, exc_type, exc, tb):
        self._exits.append(
            {
                "exc_type": exc_type.__name__ if exc_type else None,
                "exc_msg": str(exc) if exc else None,
            }
        )
        return False


def test_ingest_fixture_enrichments_raw_missing_fixture_does_not_advance_cursor(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    captured_sync_calls = []
    write_calls = []
    log_calls = []
    metric_exits = []

    class _FakeProvider:
        name = "sportmonks"

        def get_fixtures_multi_enrichments(self, *, fixture_ids):
            assert fixture_ids == [10, 20]
            return (
                {
                    10: {
                        "match_events": {"results": 0, "response": []},
                        "statistics": {"results": 0, "response": []},
                        "fixture_lineups": {"results": 0, "response": []},
                        "fixture_player_statistics": {"results": 0, "response": []},
                    }
                },
                {},
            )

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {"league_id": 71, "season": 2024, "provider": "sportmonks"},
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_statistics_targets",
        lambda **_kwargs: {
            "mode": "incremental",
            "league_id": 71,
            "season": 2024,
            "fixture_ids": [10, 20],
            "target_source": "season_scope",
        },
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_get_int_env",
        lambda *args, **kwargs: 2 if "CHUNK_SIZE" in args[0] else (5 if "MAX_CONSECUTIVE_FAILURES" in args[0] else 0),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_get_bool_env",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(ingestion_service_module, "_read_sync_cursor", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_pending_fixture_ids_for_complete_artifacts",
        lambda **_kwargs: ([10, 20], set(), "required_prefixes_full_scan"),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "write_raw_payload",
        lambda **kwargs: write_calls.append(kwargs),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_upsert_sync_state",
        lambda *args, **kwargs: captured_sync_calls.append(kwargs),
    )
    monkeypatch.setattr(ingestion_service_module, "StepMetrics", lambda **_kwargs: _CapturingMetric(metric_exits))
    monkeypatch.setattr(ingestion_service_module, "log_event", lambda **kwargs: log_calls.append(kwargs))

    with pytest.raises(RuntimeError, match="Ingestao raw fixture_enrichments parcial"):
        ingest_fixture_enrichments_raw()

    assert write_calls == []
    assert captured_sync_calls == [
        {
            "provider_name": "sportmonks",
            "entity_type": "fixture_enrichments",
            "scope_key": "league=71/season=2024",
            "league_id": 71,
            "season": 2024,
            "cursor": None,
            "status": "failed",
            "update_last_successful_sync": False,
        }
    ]
    assert any(
        call["dataset"] == "fixture_enrichments"
        and "missing=[20]" in call["message"]
        for call in log_calls
    )
    assert any(call["step"] == "summary" and call["status"] == "failed" for call in log_calls)
    assert not any(call["step"] == "summary" and call["status"] == "success" for call in log_calls)
    assert metric_exits == [
        {
            "exc_type": "RuntimeError",
            "exc_msg": "Ingestao raw fixture_enrichments parcial. mode=incremental | source=season_scope | pendentes=2 | tentativas=2 | sucesso=0 | falhas=2 | limite_diario=False.",
        }
    ]


def test_ingest_fixture_enrichments_raw_complete_chunk_advances_cursor(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    captured_sync_calls = []
    write_calls = []
    log_calls = []
    metric_exits = []

    class _FakeProvider:
        name = "sportmonks"

        def get_fixtures_multi_enrichments(self, *, fixture_ids):
            assert fixture_ids == [10, 20]
            payload = {
                "match_events": {"results": 0, "response": []},
                "statistics": {"results": 0, "response": []},
                "fixture_lineups": {"results": 0, "response": []},
                "fixture_player_statistics": {"results": 0, "response": []},
            }
            return ({10: payload, 20: payload}, {})

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {"league_id": 71, "season": 2024, "provider": "sportmonks"},
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_statistics_targets",
        lambda **_kwargs: {
            "mode": "incremental",
            "league_id": 71,
            "season": 2024,
            "fixture_ids": [10, 20],
            "target_source": "season_scope",
        },
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_get_int_env",
        lambda *args, **kwargs: 2 if "CHUNK_SIZE" in args[0] else (5 if "MAX_CONSECUTIVE_FAILURES" in args[0] else 0),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_get_bool_env",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(ingestion_service_module, "_read_sync_cursor", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_pending_fixture_ids_for_complete_artifacts",
        lambda **_kwargs: ([10, 20], set(), "required_prefixes_full_scan"),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "write_raw_payload",
        lambda **kwargs: write_calls.append(kwargs) or {"results": 0},
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_upsert_sync_state",
        lambda *args, **kwargs: captured_sync_calls.append(kwargs),
    )
    monkeypatch.setattr(ingestion_service_module, "StepMetrics", lambda **_kwargs: _CapturingMetric(metric_exits))
    monkeypatch.setattr(ingestion_service_module, "log_event", lambda **kwargs: log_calls.append(kwargs))

    ingest_fixture_enrichments_raw()

    assert len(write_calls) == 8
    assert captured_sync_calls == [
        {
            "provider_name": "sportmonks",
            "entity_type": "fixture_enrichments",
            "scope_key": "league=71/season=2024",
            "league_id": 71,
            "season": 2024,
            "cursor": 20,
            "status": "success",
            "update_last_successful_sync": True,
        }
    ]
    assert any(call["step"] == "summary" and call["status"] == "success" for call in log_calls)
    assert metric_exits == [{"exc_type": None, "exc_msg": None}]


def test_ingest_fixture_enrichments_raw_partial_write_mid_chunk_does_not_mask_failure(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    captured_sync_calls = []
    write_calls = []

    class _FakeProvider:
        name = "sportmonks"

        def get_fixtures_multi_enrichments(self, *, fixture_ids):
            assert fixture_ids == [10, 20]
            payload = {
                "match_events": {"results": 0, "response": []},
                "statistics": {"results": 0, "response": []},
                "fixture_lineups": {"results": 0, "response": []},
                "fixture_player_statistics": {"results": 0, "response": []},
            }
            return ({10: payload, 20: payload}, {})

    def _write_raw_payload(**kwargs):
        write_calls.append(kwargs)
        if len(write_calls) == 5:
            raise RuntimeError("falha simulada no write do bronze")
        return {"results": 0}

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {"league_id": 71, "season": 2024, "provider": "sportmonks"},
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_statistics_targets",
        lambda **_kwargs: {
            "mode": "incremental",
            "league_id": 71,
            "season": 2024,
            "fixture_ids": [10, 20],
            "target_source": "season_scope",
        },
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_get_int_env",
        lambda *args, **kwargs: 2 if "CHUNK_SIZE" in args[0] else (5 if "MAX_CONSECUTIVE_FAILURES" in args[0] else 0),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_get_bool_env",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(ingestion_service_module, "_read_sync_cursor", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_pending_fixture_ids_for_complete_artifacts",
        lambda **_kwargs: ([10, 20], set(), "required_prefixes_full_scan"),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "write_raw_payload",
        _write_raw_payload,
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_upsert_sync_state",
        lambda *args, **kwargs: captured_sync_calls.append(kwargs),
    )
    monkeypatch.setattr(ingestion_service_module, "StepMetrics", lambda **_kwargs: _FakeMetric())
    monkeypatch.setattr(ingestion_service_module, "log_event", lambda **_kwargs: None)

    with pytest.raises(RuntimeError, match="Ingestao raw fixture_enrichments parcial"):
        ingest_fixture_enrichments_raw()

    assert len(write_calls) == 5
    assert all("/fixture_id=10/" in call["key"] for call in write_calls[:4])
    assert "/fixture_id=20/" in write_calls[4]["key"]
    assert captured_sync_calls == [
        {
            "provider_name": "sportmonks",
            "entity_type": "fixture_enrichments",
            "scope_key": "league=71/season=2024",
            "league_id": 71,
            "season": 2024,
            "cursor": None,
            "status": "failed",
            "update_last_successful_sync": False,
        }
    ]


def test_ingest_fixture_enrichments_raw_replays_only_incomplete_fixture_when_cursor_missing(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    captured_sync_calls = []
    requested_chunks = []
    write_calls = []

    prefixes = [
        "events/league=71/season=2024/",
        "statistics/league=71/season=2024/",
        "lineups/league=71/season=2024/",
        "fixture_player_statistics/league=71/season=2024/",
    ]
    ingested_by_prefix = {
        prefixes[0]: {10, 20},
        prefixes[1]: {10},
        prefixes[2]: {10},
        prefixes[3]: {10},
    }

    class _FakeProvider:
        name = "sportmonks"

        def get_fixtures_multi_enrichments(self, *, fixture_ids):
            requested_chunks.append(list(fixture_ids))
            assert fixture_ids == [20]
            payload = {
                "match_events": {"results": 0, "response": []},
                "statistics": {"results": 0, "response": []},
                "fixture_lineups": {"results": 0, "response": []},
                "fixture_player_statistics": {"results": 0, "response": []},
            }
            return ({20: payload}, {})

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {"league_id": 71, "season": 2024, "provider": "sportmonks"},
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_statistics_targets",
        lambda **_kwargs: {
            "mode": "incremental",
            "league_id": 71,
            "season": 2024,
            "fixture_ids": [10, 20],
            "target_source": "season_scope",
        },
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_get_int_env",
        lambda *args, **kwargs: 2 if "CHUNK_SIZE" in args[0] else (5 if "MAX_CONSECUTIVE_FAILURES" in args[0] else 0),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_get_bool_env",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(ingestion_service_module, "_read_sync_cursor", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        ingestion_service_module,
        "_list_ingested_fixture_ids",
        lambda _s3_client, *, prefix: set(ingested_by_prefix[prefix]),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "write_raw_payload",
        lambda **kwargs: write_calls.append(kwargs) or {"results": 0},
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_upsert_sync_state",
        lambda *args, **kwargs: captured_sync_calls.append(kwargs),
    )
    monkeypatch.setattr(ingestion_service_module, "StepMetrics", lambda **_kwargs: _FakeMetric())
    monkeypatch.setattr(ingestion_service_module, "log_event", lambda **_kwargs: None)

    ingest_fixture_enrichments_raw()

    assert requested_chunks == [[20]]
    assert len(write_calls) == 4
    assert all("/fixture_id=20/" in call["key"] for call in write_calls)
    assert captured_sync_calls == [
        {
            "provider_name": "sportmonks",
            "entity_type": "fixture_enrichments",
            "scope_key": "league=71/season=2024",
            "league_id": 71,
            "season": 2024,
            "cursor": 20,
            "status": "success",
            "update_last_successful_sync": True,
        }
    ]


def test_ingest_head_to_head_raw_passes_semantic_scope_to_provider(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    captured_provider_kwargs = {}
    captured_source_params = {}

    class _FakeProvider:
        name = "sportmonks"

        def get_head_to_head(self, **kwargs):
            captured_provider_kwargs.update(kwargs)
            return ({"provider": "sportmonks", "response": []}, {})

    def _capture_ingest(**kwargs):
        kwargs["fetch_fn"](1)
        captured_source_params.update(kwargs["source_params_fn"](1))

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {"league_id": 2, "season": 2020, "provider": "sportmonks"},
    )
    monkeypatch.setattr(ingestion_service_module, "_get_int_env", lambda *args, **kwargs: 0 if "REQUESTS" in args[0] else 5)
    monkeypatch.setattr(ingestion_service_module, "_get_bool_env", lambda *args, **kwargs: True)
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_fixture_scope_identity",
        lambda *_args, **_kwargs: {
            "provider": "sportmonks",
            "provider_league_id": 2,
            "competition_key": "champions_league",
            "season_label": "2020_21",
            "provider_season_id": 17299,
        },
    )
    monkeypatch.setattr(ingestion_service_module, "_fetch_team_pairs", lambda *_args, **_kwargs: [(50, 529)])
    monkeypatch.setattr(ingestion_service_module, "_ingest_entity_by_numeric_ids", _capture_ingest)

    ingestion_service_module.ingest_head_to_head_raw()

    assert captured_provider_kwargs == {
        "team_id": 50,
        "opponent_id": 529,
        "league_id": 2,
        "season": 2020,
        "season_label": "2020_21",
        "provider_season_id": 17299,
    }
    assert captured_source_params["competition_key"] == "champions_league"
    assert captured_source_params["season_label"] == "2020_21"
    assert captured_source_params["provider_season_id"] == 17299


def test_ingest_player_season_statistics_raw_passes_semantic_scope_to_provider(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    captured_provider_kwargs = {}
    captured_source_params = {}

    class _FakeProvider:
        name = "sportmonks"

        def get_player_season_statistics(self, **kwargs):
            captured_provider_kwargs.update(kwargs)
            return ({"provider": "sportmonks", "response": []}, {})

    def _capture_ingest(**kwargs):
        kwargs["fetch_fn"](183516)
        captured_source_params.update(kwargs["source_params_fn"](183516))

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {"league_id": 2, "season": 2020, "provider": "sportmonks"},
    )
    monkeypatch.setattr(ingestion_service_module, "_get_int_env", lambda *args, **kwargs: 0 if "REQUESTS" in args[0] else 5)
    monkeypatch.setattr(ingestion_service_module, "_get_bool_env", lambda *args, **kwargs: True)
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_fixture_scope_identity",
        lambda *_args, **_kwargs: {
            "provider": "sportmonks",
            "provider_league_id": 2,
            "competition_key": "champions_league",
            "season_label": "2020_21",
            "provider_season_id": 17299,
        },
    )
    monkeypatch.setattr(ingestion_service_module, "_fetch_player_ids_for_scope", lambda *_args, **_kwargs: [183516])
    monkeypatch.setattr(ingestion_service_module, "_ingest_entity_by_numeric_ids", _capture_ingest)

    ingestion_service_module.ingest_player_season_statistics_raw()

    assert captured_provider_kwargs == {
        "player_id": 183516,
        "season": 2020,
        "league_id": 2,
        "season_label": "2020_21",
        "provider_season_id": 17299,
    }
    assert captured_source_params["competition_key"] == "champions_league"
    assert captured_source_params["season_label"] == "2020_21"
    assert captured_source_params["provider_season_id"] == 17299


def test_resolve_catalog_season_scope_returns_historical_row():
    import common.services.ingestion_service as ingestion_service_module

    engine = _FakeEngine(
        [
            {
                "competition_key": "premier_league",
                "season_label": "2020_21",
                "provider_season_id": 17420,
                "season_start_date": date(2020, 9, 12),
                "season_end_date": date(2021, 5, 23),
            }
        ]
    )

    scope = ingestion_service_module._resolve_catalog_season_scope(
        engine,
        provider_name="sportmonks",
        league_id=8,
        season=2020,
    )

    assert scope == {
        "competition_key": "premier_league",
        "season_label": "2020_21",
        "provider_season_id": 17420,
        "season_start_date": "2020-09-12",
        "season_end_date": "2021-05-23",
    }
    assert engine.executed_params == {
        "provider": "sportmonks",
        "league_id": 8,
        "season_prefix": "2020",
    }


def test_ingest_competition_structure_raw_prefers_catalog_scope(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    captured_provider_kwargs = {}
    captured_write_kwargs = {}

    class _FakeProvider:
        name = "sportmonks"

        def get_competition_structure(self, **kwargs):
            captured_provider_kwargs.update(kwargs)
            return ({"provider": "sportmonks", "response": []}, {"x-ratelimit-remaining": "100"})

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {"league_id": 8, "season": 2020, "provider": "sportmonks"},
    )
    monkeypatch.setattr(ingestion_service_module, "_get_int_env", lambda *args, **kwargs: 0)
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_catalog_season_scope",
        lambda *_args, **_kwargs: {
            "competition_key": "premier_league",
            "season_label": "2020_21",
            "provider_season_id": 17420,
            "season_start_date": "2020-09-12",
            "season_end_date": "2021-05-23",
        },
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "write_raw_payload",
        lambda **kwargs: captured_write_kwargs.update(kwargs) or {"results": 1},
    )
    monkeypatch.setattr(ingestion_service_module, "log_event", lambda **_kwargs: None)

    ingestion_service_module.ingest_competition_structure_raw()

    assert captured_provider_kwargs == {
        "league_id": 8,
        "season": 2020,
        "season_label": "2020_21",
        "provider_season_id": 17420,
        "season_start_date": "2020-09-12",
        "season_end_date": "2021-05-23",
    }
    assert captured_write_kwargs["source_params"] == {
        "league_id": 8,
        "season": 2020,
        "competition_key": "premier_league",
        "season_label": "2020_21",
        "provider_season_id": 17420,
    }


def test_ingest_standings_raw_prefers_catalog_scope(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    captured_provider_kwargs = {}
    captured_write_kwargs = {}

    class _FakeProvider:
        name = "sportmonks"

        def get_standings(self, **kwargs):
            captured_provider_kwargs.update(kwargs)
            return ({"provider": "sportmonks", "response": []}, {"x-ratelimit-remaining": "100"})

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {"league_id": 8, "season": 2020, "provider": "sportmonks"},
    )
    monkeypatch.setattr(ingestion_service_module, "_get_int_env", lambda *args, **kwargs: 0)
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_catalog_season_scope",
        lambda *_args, **_kwargs: {
            "competition_key": "premier_league",
            "season_label": "2020_21",
            "provider_season_id": 17420,
            "season_start_date": "2020-09-12",
            "season_end_date": "2021-05-23",
        },
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "write_raw_payload",
        lambda **kwargs: captured_write_kwargs.update(kwargs) or {"results": 1},
    )
    monkeypatch.setattr(ingestion_service_module, "log_event", lambda **_kwargs: None)

    ingestion_service_module.ingest_standings_raw()

    assert captured_provider_kwargs == {
        "league_id": 8,
        "season": 2020,
        "season_label": "2020_21",
        "provider_season_id": 17420,
        "season_start_date": "2020-09-12",
        "season_end_date": "2021-05-23",
    }
    assert captured_write_kwargs["source_params"] == {
        "league_id": 8,
        "season": 2020,
        "competition_key": "premier_league",
        "season_label": "2020_21",
        "provider_season_id": 17420,
    }


def test_ingest_fixture_player_statistics_raw_backfill_explicit_fixture_ids_ignores_sync_cursor(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    fetched_ids = []
    write_calls = []
    sync_calls = []

    class _FakeProvider:
        name = "sportmonks"

        def get_fixture_player_statistics(self, *, fixture_id):
            fetched_ids.append(fixture_id)
            return (
                {
                    "provider": "sportmonks",
                    "entity_type": "fixture_player_statistics",
                    "response": [],
                    "results": 0,
                },
                {"x-ratelimit-remaining": "100"},
            )

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {"league_id": 651, "season": 2024, "provider": "sportmonks"},
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_resolve_statistics_targets",
        lambda **_kwargs: {
            "mode": "backfill",
            "league_id": 651,
            "season": 2024,
            "fixture_ids": [10, 20],
            "target_source": "explicit_fixture_ids",
        },
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_get_int_env",
        lambda env_name, *args, **kwargs: 0 if "REQUESTS_PER_MINUTE" in env_name else 5,
    )
    monkeypatch.setattr(ingestion_service_module, "_get_bool_env", lambda *args, **kwargs: True)
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(ingestion_service_module, "_read_sync_cursor", lambda *args, **kwargs: 30)
    monkeypatch.setattr(
        ingestion_service_module,
        "write_raw_payload",
        lambda **kwargs: write_calls.append(kwargs) or {"results": 0},
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_upsert_sync_state",
        lambda *args, **kwargs: sync_calls.append(kwargs),
    )
    monkeypatch.setattr(ingestion_service_module, "StepMetrics", lambda **_kwargs: _FakeMetric())
    monkeypatch.setattr(ingestion_service_module, "log_event", lambda **_kwargs: None)

    ingestion_service_module.ingest_fixture_player_statistics_raw()

    assert fetched_ids == [10, 20]
    assert [call["source_params"]["fixture"] for call in write_calls] == [10, 20]
    assert sync_calls[-1]["cursor"] == 30
    assert sync_calls[-1]["status"] == "success"
