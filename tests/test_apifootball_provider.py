from __future__ import annotations

from pathlib import Path
import sys

import pytest

DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from common.providers.api_football import APIFootballProvider


class _SpyHttpClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def request_json(self, *, endpoint: str, params: dict | None = None):
        self.calls.append({"endpoint": endpoint, "params": params})
        if endpoint == "/fixtures":
            return {"errors": {}, "response": [{"fixture": {"id": 1}}], "results": 1}, {"x-ratelimit-remaining": "98"}
        if endpoint == "/standings":
            return {"errors": {}, "response": [{"league": {"id": 71}}], "results": 1}, {"x-ratelimit-remaining": "97"}
        if endpoint == "/fixtures/statistics":
            return {"errors": {}, "response": [{"team": {"id": 10}}], "results": 1}, {"x-ratelimit-remaining": "96"}
        if endpoint == "/fixtures/events":
            return {"errors": {}, "response": [{"time": {"elapsed": 10}}], "results": 1}, {"x-ratelimit-remaining": "95"}
        raise AssertionError(f"Endpoint inesperado: {endpoint}")


def test_apifootball_provider_uses_http_client():
    spy = _SpyHttpClient()
    provider = APIFootballProvider(api_key="test-key", base_url="https://api.test")
    provider._client = spy

    fixtures_payload, fixtures_headers = provider.get_fixtures(
        league_id=71, season=2024, date_from="2024-01-01", date_to="2024-01-31"
    )
    standings_payload, standings_headers = provider.get_standings(league_id=71, season=2024)
    stats_payload, stats_headers = provider.get_fixture_statistics(fixture_id=1234)
    events_payload, events_headers = provider.get_fixture_events(fixture_id=1234)

    assert len(spy.calls) == 4
    assert spy.calls[0]["endpoint"] == "/fixtures"
    assert spy.calls[1]["endpoint"] == "/standings"
    assert spy.calls[2]["endpoint"] == "/fixtures/statistics"
    assert spy.calls[3]["endpoint"] == "/fixtures/events"
    assert spy.calls[0]["params"] == {"league": 71, "season": 2024, "from": "2024-01-01", "to": "2024-01-31"}
    assert spy.calls[1]["params"] == {"league": 71, "season": 2024}
    assert spy.calls[2]["params"] == {"fixture": 1234}
    assert spy.calls[3]["params"] == {"fixture": 1234}

    assert fixtures_payload["provider"] == "api_football"
    assert fixtures_payload["entity_type"] == "fixtures"
    assert standings_payload["entity_type"] == "standings"
    assert stats_payload["entity_type"] == "statistics"
    assert events_payload["entity_type"] == "match_events"
    assert fixtures_headers["x-ratelimit-remaining"] == "98"
    assert standings_headers["x-ratelimit-remaining"] == "97"
    assert stats_headers["x-ratelimit-remaining"] == "96"
    assert events_headers["x-ratelimit-remaining"] == "95"


def test_apifootball_provider_raises_on_api_errors():
    class _ErrorClient:
        def request_json(self, *, endpoint: str, params: dict | None = None):
            return {"errors": {"detail": "failure"}, "response": []}, {}

    provider = APIFootballProvider(api_key="test-key", base_url="https://api.test")
    provider._client = _ErrorClient()
    with pytest.raises(RuntimeError, match="errors"):
        provider.get_fixtures(league_id=71, season=2024, date_from="2024-01-01", date_to="2024-01-31")


def test_no_requests_get_in_dags():
    dag_dir = Path("infra/airflow/dags")
    offenders: list[str] = []

    for py_file in dag_dir.rglob("*.py"):
        content = py_file.read_text(encoding="utf-8", errors="ignore")
        if "requests.get" in content:
            offenders.append(str(py_file))

    assert not offenders, (
        "Encontrado requests.get nos DAGs (equivalente a grep -R \"requests.get\" infra/airflow/dags): "
        f"{offenders}"
    )
