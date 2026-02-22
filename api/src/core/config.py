from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _to_bool(raw_value: str | None, *, default: bool) -> bool:
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parse_csv(raw_value: str | None) -> list[str]:
    if raw_value is None:
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _to_int(raw_value: str | None, *, default: int) -> int:
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def _build_default_pg_dsn() -> str:
    user = os.getenv("POSTGRES_USER", "football")
    password = os.getenv("POSTGRES_PASSWORD", "football")
    database = os.getenv("POSTGRES_DB", "football_dw")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@dataclass(frozen=True)
class Settings:
    app_name: str
    environment: str
    log_level: str
    pg_dsn: str
    cors_allow_origins: tuple[str, ...]
    cors_allow_credentials: bool
    rate_limit_requests_per_minute: int


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    pg_dsn = os.getenv("FOOTBALL_PG_DSN") or os.getenv("DATABASE_URL") or _build_default_pg_dsn()
    cors_allow_origins = tuple(
        _parse_csv(os.getenv("BFF_CORS_ALLOW_ORIGINS"))
        or [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
        ]
    )
    rate_limit_requests_per_minute = _to_int(os.getenv("BFF_RATE_LIMIT_REQUESTS_PER_MINUTE"), default=120)

    return Settings(
        app_name=os.getenv("BFF_APP_NAME", "football-analytics-bff"),
        environment=os.getenv("ENVIRONMENT", "local"),
        log_level=os.getenv("BFF_LOG_LEVEL", "INFO").upper(),
        pg_dsn=pg_dsn,
        cors_allow_origins=cors_allow_origins,
        cors_allow_credentials=_to_bool(os.getenv("BFF_CORS_ALLOW_CREDENTIALS"), default=False),
        rate_limit_requests_per_minute=rate_limit_requests_per_minute,
    )
