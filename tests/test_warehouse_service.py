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

import common.services.warehouse_service as warehouse_service
from common.competition_identity import derive_season_label, looks_like_provider_identifier
from common.services.warehouse_service import _enrich_with_season_identity, _mark_sync_scope_validated, _validate_h2h_scope


class _FakeMappingsResult:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self):
        return self

    def all(self) -> list[dict]:
        return self._rows


class _FakeConn:
    def __init__(self, fixture_rows: list[dict]) -> None:
        self._fixture_rows = fixture_rows

    def execute(self, _query, params):
        fixture_ids = {int(value) for value in params["fixture_ids"]}
        rows = [row for row in self._fixture_rows if int(row["fixture_id"]) in fixture_ids]
        return _FakeMappingsResult(rows)


def _fixture_scope_row(
    fixture_id: int,
    *,
    provider: str = "sportmonks",
    league_id: int = 2,
    competition_key: str = "champions_league",
    season: int = 2020,
    season_label: str = "2020_21",
    provider_season_id: int = 17299,
) -> dict:
    return {
        "fixture_id": fixture_id,
        "fixture_provider": provider,
        "fixture_league_id": league_id,
        "fixture_competition_key": competition_key,
        "fixture_season": season,
        "fixture_season_label": season_label,
        "fixture_provider_season_id": provider_season_id,
    }


def _h2h_row(
    fixture_id: int | None,
    *,
    provider: str = "sportmonks",
    pair_team_id: int = 10,
    pair_opponent_id: int = 11,
    league_id: int = 2,
    competition_key: str | None = "champions_league",
    season_label: str | None = "2020_21",
    season_id: int | None = 17299,
    provider_season_id: int | None = 17299,
) -> dict:
    return {
        "provider": provider,
        "pair_team_id": pair_team_id,
        "pair_opponent_id": pair_opponent_id,
        "fixture_id": fixture_id,
        "league_id": league_id,
        "competition_key": competition_key,
        "season_label": season_label,
        "season_id": season_id,
        "provider_season_id": provider_season_id,
    }


def test_validate_h2h_scope_keeps_valid_row():
    conn = _FakeConn([_fixture_scope_row(1001)])
    load_df = pd.DataFrame([_h2h_row(1001)])

    result = _validate_h2h_scope(
        conn,
        load_df,
        "run-valid",
        league_id=2,
        season=2020,
        season_label="2020_21",
        provider_season_id=17299,
        rejection_threshold=1.0,
    )

    assert result["fixture_id"].tolist() == [1001]


def test_validate_h2h_scope_rejects_orphan_row():
    conn = _FakeConn([_fixture_scope_row(1001)])
    load_df = pd.DataFrame([_h2h_row(9999)])

    result = _validate_h2h_scope(
        conn,
        load_df,
        "run-orphan",
        league_id=2,
        season=2020,
        season_label="2020_21",
        provider_season_id=17299,
        rejection_threshold=1.0,
    )

    assert result.empty


def test_validate_h2h_scope_rejects_null_competition_key():
    conn = _FakeConn([_fixture_scope_row(1001)])
    load_df = pd.DataFrame([_h2h_row(1001, competition_key=None)])

    result = _validate_h2h_scope(
        conn,
        load_df,
        "run-null-key",
        league_id=2,
        season=2020,
        season_label="2020_21",
        provider_season_id=17299,
        rejection_threshold=1.0,
    )

    assert result.empty


def test_validate_h2h_scope_rejects_row_from_other_scope():
    conn = _FakeConn(
        [
            _fixture_scope_row(
                2002,
                league_id=8,
                competition_key="premier_league",
                season=2021,
                season_label="2021_22",
                provider_season_id=18378,
            )
        ]
    )
    load_df = pd.DataFrame(
        [
            _h2h_row(
                2002,
                league_id=8,
                competition_key="premier_league",
                season_label="2021_22",
                season_id=18378,
                provider_season_id=18378,
            )
        ]
    )

    result = _validate_h2h_scope(
        conn,
        load_df,
        "run-other-scope",
        league_id=2,
        season=2020,
        season_label="2020_21",
        provider_season_id=17299,
        rejection_threshold=1.0,
    )

    assert result.empty


def test_validate_h2h_scope_fails_when_rejection_rate_exceeds_threshold():
    conn = _FakeConn([_fixture_scope_row(1001)])
    load_df = pd.DataFrame([_h2h_row(1001), _h2h_row(9999)])

    with pytest.raises(RuntimeError, match="Taxa de rejeicao excessiva"):
        _validate_h2h_scope(
            conn,
            load_df,
            "run-threshold",
            league_id=2,
            season=2020,
            season_label="2020_21",
            provider_season_id=17299,
        )


def test_competition_identity_rejects_provider_like_numeric_label():
    assert looks_like_provider_identifier("24218") is True
    assert derive_season_label(season=24218) is None
    assert derive_season_label(season=2024) == "2024"


def test_enrich_with_season_identity_does_not_invent_provider_season_id_without_catalog(monkeypatch):
    monkeypatch.setattr(warehouse_service, "_season_identity_df", lambda _conn: pd.DataFrame())
    monkeypatch.setattr(warehouse_service, "_competition_mapping_df", lambda _conn: pd.DataFrame())

    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "season_id": 24218,
                "provider_season_id": pd.NA,
                "season_label": pd.NA,
                "season_name": None,
                "starting_at": None,
                "ending_at": None,
            }
        ]
    )

    result = _enrich_with_season_identity(None, load_df, "run-no-catalog")

    assert pd.isna(result.loc[0, "provider_season_id"])
    assert pd.isna(result.loc[0, "season_label"])


def test_enrich_with_season_identity_nulls_provider_like_season_label(monkeypatch):
    monkeypatch.setattr(warehouse_service, "_season_identity_df", lambda _conn: pd.DataFrame())
    monkeypatch.setattr(warehouse_service, "_competition_mapping_df", lambda _conn: pd.DataFrame())

    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "season_id": 24218,
                "provider_season_id": 24218,
                "season_label": "24218",
            }
        ]
    )

    result = _enrich_with_season_identity(None, load_df, "run-bad-label")

    assert pd.isna(result.loc[0, "season_label"])
    assert result.loc[0, "provider_season_id"] == 24218


def test_enrich_with_season_identity_keeps_valid_catalog_match(monkeypatch):
    monkeypatch.setattr(
        warehouse_service,
        "_season_identity_df",
        lambda _conn: pd.DataFrame(
            [
                {
                    "provider": "sportmonks",
                    "season_id": 17299,
                    "provider_season_id": 17299,
                    "provider_league_id": 2,
                    "competition_key": "champions_league",
                    "season_label": "2020_21",
                    "season_name": "UEFA Champions League 2020/2021",
                    "starting_at": "2020-08-08",
                    "ending_at": "2021-05-29",
                }
            ]
        ),
    )
    monkeypatch.setattr(warehouse_service, "_competition_mapping_df", lambda _conn: pd.DataFrame())

    load_df = pd.DataFrame(
        [
            {
                "provider": "sportmonks",
                "season_id": 17299,
                "provider_season_id": pd.NA,
                "provider_league_id": pd.NA,
                "competition_key": pd.NA,
                "season_label": pd.NA,
                "season_name": None,
                "starting_at": None,
                "ending_at": None,
            }
        ]
    )

    result = _enrich_with_season_identity(None, load_df, "run-valid-catalog")

    assert result.loc[0, "provider_season_id"] == 17299
    assert result.loc[0, "provider_league_id"] == 2
    assert result.loc[0, "competition_key"] == "champions_league"
    assert result.loc[0, "season_label"] == "2020_21"


def test_mark_sync_scope_validated_calls_sync_upsert_with_semantic_validation(monkeypatch):
    import common.services.ingestion_service as ingestion_service_module

    captured = {}

    def _fake_upsert(engine, **kwargs):
        captured["engine"] = engine
        captured["kwargs"] = kwargs

    monkeypatch.setattr(ingestion_service_module, "_upsert_sync_state", _fake_upsert)

    fake_engine = object()
    _mark_sync_scope_validated(
        fake_engine,
        provider_name="sportmonks",
        entity_type="head_to_head",
        scope_key="league=2/season=2020/entity=head_to_head",
        league_id=2,
        season=2020,
        scope_validation_notes="validated_against_raw.fixtures",
    )

    assert captured["engine"] is fake_engine
    assert captured["kwargs"]["scope_validated"] is True
    assert captured["kwargs"]["scope_validation_notes"] == "validated_against_raw.fixtures"
    assert captured["kwargs"]["entity_type"] == "head_to_head"
