from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    pg_dsn = os.getenv("FOOTBALL_PG_DSN") or os.getenv("DATABASE_URL") or _build_default_pg_dsn()

    return Settings(
        app_name=os.getenv("BFF_APP_NAME", "football-analytics-bff"),
        environment=os.getenv("ENVIRONMENT", "local"),
        log_level=os.getenv("BFF_LOG_LEVEL", "INFO").upper(),
        pg_dsn=pg_dsn,
    )
