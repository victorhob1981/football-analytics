from __future__ import annotations

from pathlib import Path
import sys

DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from common.http_client import ProviderHttpClient


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict, headers: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text

    def json(self) -> dict:
        return self._payload


def test_http_client_retries_on_429(monkeypatch):
    responses = [
        _FakeResponse(429, {"error": "rate_limit"}, headers={"Retry-After": "0"}),
        _FakeResponse(200, {"ok": True}),
    ]
    client = ProviderHttpClient(
        provider="test-provider",
        base_url="https://example.com",
        max_retries=2,
        backoff_seconds=0.0,
    )

    def fake_request(method, url, params=None, headers=None, timeout=None):
        return responses.pop(0)

    sleeps: list[float] = []
    monkeypatch.setattr(client._session, "request", fake_request)
    monkeypatch.setattr("common.http_client.time.sleep", lambda value: sleeps.append(value))

    payload, _headers = client.request_json(endpoint="/fixtures")
    assert payload == {"ok": True}
    assert len(sleeps) == 1


def test_http_client_retries_on_500(monkeypatch):
    responses = [
        _FakeResponse(500, {"error": "temporary"}, text="temporary error"),
        _FakeResponse(200, {"ok": True}),
    ]
    client = ProviderHttpClient(
        provider="test-provider",
        base_url="https://example.com",
        max_retries=2,
        backoff_seconds=0.0,
    )

    def fake_request(method, url, params=None, headers=None, timeout=None):
        return responses.pop(0)

    sleeps: list[float] = []
    monkeypatch.setattr(client._session, "request", fake_request)
    monkeypatch.setattr("common.http_client.time.sleep", lambda value: sleeps.append(value))

    payload, _headers = client.request_json(endpoint="/standings")
    assert payload == {"ok": True}
    assert len(sleeps) == 1
