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
    _upsert_sync_state,
    ingest_fixtures_raw,
    ingest_head_to_head_raw,
    ingest_player_season_statistics_raw,
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


class _CaptureConn:
    def __init__(self, captured_calls: list[dict]) -> None:
        self._captured_calls = captured_calls

    def execute(self, query, params):
        self._captured_calls.append({"query": str(query), "params": dict(params)})
        return None


class _CaptureEngine:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def begin(self):
        return self

    def __enter__(self):
        return _CaptureConn(self.calls)

    def __exit__(self, exc_type, exc, tb):
        return False


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


def test_ingest_fixtures_raw_counts_paginated_requests_from_provider_meta(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    metric_calls = []
    log_calls = []

    class _FakeMetric:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def set_counts(self, **kwargs):
            metric_calls.append(kwargs)

    class _FakeProvider:
        name = "sportmonks"

        def __init__(self):
            self._calls = 0

        def get_fixtures(self, **_kwargs):
            self._calls += 1
            if self._calls == 1:
                return (
                    {
                        "provider_meta": {"pages_used": 3},
                        "results": 10,
                        "response": [{}] * 10,
                    },
                    {"x-ratelimit-remaining": "97"},
                )
            return (
                {
                    "provider_meta": {},
                    "results": 5,
                    "response": [{}] * 5,
                },
                {"x-ratelimit-remaining": "96"},
            )

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {
            "league_id": 71,
            "season": 2024,
            "season_label": "2024",
            "provider_season_id": 23265,
            "provider": "sportmonks",
        },
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_fixture_windows",
        lambda _context, _season: [("2024-01-01", "2024-01-31"), ("2024-02-01", "2024-02-29")],
    )
    monkeypatch.setattr(ingestion_service_module, "_get_int_env", lambda *args, **kwargs: 0)
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(
        ingestion_service_module,
        "write_raw_payload",
        lambda **kwargs: {"results": int(kwargs["payload"].get("results", 0))},
    )
    monkeypatch.setattr(ingestion_service_module, "StepMetrics", lambda **_kwargs: _FakeMetric())
    monkeypatch.setattr(
        ingestion_service_module,
        "log_event",
        lambda **kwargs: log_calls.append(kwargs),
    )

    ingest_fixtures_raw()

    assert metric_calls == [{"rows_in": 4, "rows_out": 15, "row_count": 15}]
    assert log_calls[-1]["rows_in"] == 4
    assert "requests=4" in log_calls[-1]["message"]


def test_ingest_head_to_head_raw_passes_full_scope_to_provider(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    provider_calls = []
    captured_source_params = {}
    captured_sync_calls = []

    class _FakeProvider:
        name = "sportmonks"

        def get_head_to_head(self, **kwargs):
            provider_calls.append(kwargs)
            return ({"results": 0, "response": []}, {})

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {
            "league_id": 648,
            "season": 2024,
            "season_label": "2024",
            "provider_season_id": 23265,
            "provider": "sportmonks",
        },
    )
    monkeypatch.setattr(ingestion_service_module, "_get_int_env", lambda *args, **kwargs: 0)
    monkeypatch.setattr(ingestion_service_module, "_get_bool_env", lambda *args, **kwargs: True)
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        ingestion_service_module,
        "_fetch_team_pairs",
        lambda _engine, *, league_id, season: [(10, 11)] if (league_id, season) == (648, 2024) else [],
    )

    def _fake_ingest_entity_by_numeric_ids(**kwargs):
        captured_source_params.update(kwargs["source_params_fn"](1))
        captured_sync_calls.append(
            {
                "scope_validated_on_write": kwargs.get("scope_validated_on_write"),
                "scope_validation_notes_on_write": kwargs.get("scope_validation_notes_on_write"),
            }
        )
        kwargs["fetch_fn"](1)

    monkeypatch.setattr(ingestion_service_module, "_ingest_entity_by_numeric_ids", _fake_ingest_entity_by_numeric_ids)

    ingest_head_to_head_raw()

    assert provider_calls == [
        {
            "team_id": 10,
            "opponent_id": 11,
            "league_id": 648,
            "season": 2024,
            "season_label": "2024",
            "provider_season_id": 23265,
        }
    ]
    assert captured_source_params == {
        "league_id": 648,
        "season": 2024,
        "season_label": "2024",
        "provider_season_id": 23265,
        "pair_team_id": 10,
        "pair_opponent_id": 11,
    }
    assert captured_sync_calls == [
        {
            "scope_validated_on_write": False,
            "scope_validation_notes_on_write": "pending_raw_scope_validation",
        }
    ]


def test_upsert_sync_state_persists_scope_validation_fields():
    engine = _CaptureEngine()

    _upsert_sync_state(
        engine,
        provider_name="sportmonks",
        entity_type="head_to_head",
        scope_key="league=2/season=2020/entity=head_to_head",
        league_id=2,
        season=2020,
        cursor=17299,
        status="success",
        update_last_successful_sync=False,
        scope_validated=True,
        scope_validation_notes="validated_against_raw.fixtures",
    )

    assert len(engine.calls) == 1
    params = engine.calls[0]["params"]
    assert params["scope_validated"] is True
    assert params["scope_validation_notes"] == "validated_against_raw.fixtures"
    assert params["cursor"] == "17299"


def test_upsert_sync_state_is_idempotent_for_scope_validation_fields():
    engine = _CaptureEngine()

    _upsert_sync_state(
        engine,
        provider_name="sportmonks",
        entity_type="head_to_head",
        scope_key="league=2/season=2020/entity=head_to_head",
        league_id=2,
        season=2020,
        cursor=17299,
        status="success",
        update_last_successful_sync=False,
        scope_validated=True,
        scope_validation_notes="validated_against_raw.fixtures",
    )
    _upsert_sync_state(
        engine,
        provider_name="sportmonks",
        entity_type="head_to_head",
        scope_key="league=2/season=2020/entity=head_to_head",
        league_id=2,
        season=2020,
        cursor=17299,
        status="success",
        update_last_successful_sync=False,
        scope_validated=True,
        scope_validation_notes="validated_against_raw.fixtures",
    )

    assert len(engine.calls) == 2
    assert engine.calls[0]["params"]["scope_validated"] is True
    assert engine.calls[1]["params"]["scope_validated"] is True
    assert engine.calls[0]["params"]["scope_validation_notes"] == engine.calls[1]["params"]["scope_validation_notes"]


def test_ingest_player_season_statistics_guardrail_fails_without_lineups(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    provider_calls = []
    log_calls = []

    class _FakeProvider:
        name = "sportmonks"

        def get_player_season_statistics(self, **kwargs):
            provider_calls.append(kwargs)
            return ({"results": 0, "response": []}, {})

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {
            "league_id": 301,
            "season": 2021,
            "season_label": "2021_22",
            "provider_season_id": 18441,
            "provider": "sportmonks",
        },
    )
    monkeypatch.setattr(ingestion_service_module, "_get_int_env", lambda *args, **kwargs: 0)
    monkeypatch.setattr(ingestion_service_module, "_get_bool_env", lambda *args, **kwargs: True)
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_count_lineups_for_scope", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        ingestion_service_module,
        "_fetch_player_ids_from_lineups_only",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Nao deveria buscar player_ids sem lineups")),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_fetch_player_ids_for_scope",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Fallback legado nao deve ser usado em PSS")),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_ingest_entity_by_numeric_ids",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("Nao deveria iniciar ingestao sem lineups")),
    )
    monkeypatch.setattr(ingestion_service_module, "log_event", lambda **kwargs: log_calls.append(kwargs))

    with pytest.raises(RuntimeError, match="raw.fixture_lineups vazio"):
        ingest_player_season_statistics_raw()

    assert provider_calls == []
    assert any(call["status"] == "failed" and "raw.fixture_lineups vazio" in call["message"] for call in log_calls)


def test_ingest_player_season_statistics_guardrail_fails_when_lineups_have_no_player_ids(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    provider_calls = []
    log_calls = []

    class _FakeProvider:
        name = "sportmonks"

        def get_player_season_statistics(self, **kwargs):
            provider_calls.append(kwargs)
            return ({"results": 0, "response": []}, {})

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {
            "league_id": 301,
            "season": 2021,
            "season_label": "2021_22",
            "provider_season_id": 18441,
            "provider": "sportmonks",
        },
    )
    monkeypatch.setattr(ingestion_service_module, "_get_int_env", lambda *args, **kwargs: 0)
    monkeypatch.setattr(ingestion_service_module, "_get_bool_env", lambda *args, **kwargs: True)
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_count_lineups_for_scope", lambda *_args, **_kwargs: 12)
    monkeypatch.setattr(ingestion_service_module, "_fetch_player_ids_from_lineups_only", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        ingestion_service_module,
        "_fetch_player_ids_for_scope",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Fallback legado nao deve ser usado em PSS")),
    )
    monkeypatch.setattr(
        ingestion_service_module,
        "_ingest_entity_by_numeric_ids",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("Nao deveria iniciar ingestao sem player_ids seeded")),
    )
    monkeypatch.setattr(ingestion_service_module, "log_event", lambda **kwargs: log_calls.append(kwargs))

    with pytest.raises(RuntimeError, match="nenhum player_id encontrado"):
        ingest_player_season_statistics_raw()

    assert provider_calls == []
    assert any(call["status"] == "failed" and "lineup_count=12" in call["message"] for call in log_calls)


def test_ingest_player_season_statistics_passes_full_scope_to_provider(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    provider_calls = []
    log_calls = []
    captured = {}

    class _FakeProvider:
        name = "sportmonks"

        def get_player_season_statistics(self, **kwargs):
            provider_calls.append(kwargs)
            return ({"results": 1, "response": [{"player_id": kwargs["player_id"]}]}, {})

    monkeypatch.setattr(ingestion_service_module, "get_current_context", lambda: {"params": {}, "dag_run": None})
    monkeypatch.setattr(
        ingestion_service_module,
        "resolve_runtime_params",
        lambda _context: {
            "league_id": 301,
            "season": 2020,
            "season_label": "2020_21",
            "provider_season_id": 17160,
            "provider": "sportmonks",
        },
    )
    monkeypatch.setattr(ingestion_service_module, "_get_int_env", lambda *args, **kwargs: 0)
    monkeypatch.setattr(ingestion_service_module, "_get_bool_env", lambda *args, **kwargs: True)
    monkeypatch.setattr(ingestion_service_module, "get_provider", lambda *_args, **_kwargs: _FakeProvider())
    monkeypatch.setattr(ingestion_service_module, "_s3_client", lambda: object())
    monkeypatch.setattr(ingestion_service_module, "_get_required_env", lambda _name: "postgresql://test")
    monkeypatch.setattr(ingestion_service_module, "create_engine", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(ingestion_service_module, "_count_lineups_for_scope", lambda *_args, **_kwargs: 79)
    monkeypatch.setattr(ingestion_service_module, "_fetch_player_ids_from_lineups_only", lambda *_args, **_kwargs: [100, 200])
    monkeypatch.setattr(
        ingestion_service_module,
        "_fetch_player_ids_for_scope",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("Fallback legado nao deve ser usado em PSS")),
    )
    monkeypatch.setattr(ingestion_service_module, "log_event", lambda **kwargs: log_calls.append(kwargs))

    def _fake_ingest_entity_by_numeric_ids(**kwargs):
        captured["target_ids"] = kwargs["target_ids"]
        captured["source_params"] = kwargs["source_params_fn"](100)
        kwargs["fetch_fn"](100)

    monkeypatch.setattr(ingestion_service_module, "_ingest_entity_by_numeric_ids", _fake_ingest_entity_by_numeric_ids)

    ingest_player_season_statistics_raw()

    assert provider_calls == [
        {
            "player_id": 100,
            "season": 2020,
            "league_id": 301,
            "season_label": "2020_21",
            "provider_season_id": 17160,
        }
    ]
    assert captured["target_ids"] == [100, 200]
    assert captured["source_params"] == {
        "league_id": 301,
        "season": 2020,
        "player_id": 100,
        "lineup_count": 79,
        "seeded_player_ids": 2,
        "provider_requests_planned": 2,
        "season_label": "2020_21",
        "provider_season_id": 17160,
    }
    assert any(
        call["status"] == "success"
        and "Seed PSS | source=fixture_lineups_only" in call["message"]
        and "lineup_count=79" in call["message"]
        and "seeded_player_ids=2" in call["message"]
        and "provider_requests_planned=2" in call["message"]
        for call in log_calls
    )
