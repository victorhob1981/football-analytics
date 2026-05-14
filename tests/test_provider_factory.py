from __future__ import annotations

from pathlib import Path
import sys

import pytest

DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from common.providers.api_football import APIFootballProvider
from common.providers.registry import get_provider
from common.providers.sportmonks import SportMonksProvider


def test_provider_factory_defaults_to_sportmonks(monkeypatch):
    monkeypatch.delenv("ACTIVE_PROVIDER", raising=False)
    monkeypatch.setenv("API_KEY_SPORTMONKS", "test-key")

    provider = get_provider()

    assert isinstance(provider, SportMonksProvider)


def test_provider_factory_returns_apifootball(monkeypatch):
    monkeypatch.setenv("ACTIVE_PROVIDER", "apifootball")
    monkeypatch.setenv("APIFOOTBALL_API_KEY", "test-key")

    provider = get_provider()

    assert isinstance(provider, APIFootballProvider)


def test_provider_factory_rejects_invalid_provider(monkeypatch):
    monkeypatch.setenv("ACTIVE_PROVIDER", "invalid-provider")

    with pytest.raises(RuntimeError, match="Provider nao suportado"):
        get_provider()
