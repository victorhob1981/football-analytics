from __future__ import annotations

from pathlib import Path
import sys


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from common.runtime import DEFAULT_FIXTURE_WINDOWS_BY_SEASON, resolve_fixture_windows


def test_resolve_fixture_windows_uses_catalog_dates_for_split_year(monkeypatch):
    from common import runtime as runtime_module

    monkeypatch.setattr(
        runtime_module,
        "_resolve_catalog_fixture_scope",
        lambda **_kwargs: {
            "season_label": "2023_24",
            "season_start_date": "2023-08-11",
            "season_end_date": "2024-05-19",
        },
    )

    windows = resolve_fixture_windows(
        {"params": {}, "dag_run": None},
        2023,
        provider_name="sportmonks",
        league_id=8,
    )

    assert windows == [
        ("2023-08-11", "2023-11-08"),
        ("2023-11-09", "2024-02-06"),
        ("2024-02-07", "2024-05-06"),
        ("2024-05-07", "2024-05-19"),
    ]


def test_resolve_fixture_windows_preserves_annual_defaults(monkeypatch):
    from common import runtime as runtime_module

    monkeypatch.setattr(
        runtime_module,
        "_resolve_catalog_fixture_scope",
        lambda **_kwargs: {
            "season_label": "2024",
            "season_start_date": "2024-04-13",
            "season_end_date": "2024-12-08",
        },
    )

    windows = resolve_fixture_windows(
        {"params": {}, "dag_run": None},
        2024,
        provider_name="sportmonks",
        league_id=71,
    )

    assert windows == DEFAULT_FIXTURE_WINDOWS_BY_SEASON[2024]


def test_resolve_fixture_windows_falls_back_when_catalog_is_missing(monkeypatch):
    from common import runtime as runtime_module

    monkeypatch.setattr(runtime_module, "_resolve_catalog_fixture_scope", lambda **_kwargs: None)

    windows = resolve_fixture_windows(
        {"params": {}, "dag_run": None},
        2021,
        provider_name="sportmonks",
        league_id=8,
    )

    assert windows == [("2021-01-01", "2021-12-31")]
