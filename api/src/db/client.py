from __future__ import annotations

import atexit
from contextlib import contextmanager
from decimal import Decimal
from datetime import date, datetime
from typing import Any, Iterator, Sequence

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ..core.config import get_settings


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    return value


class DatabaseClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._pool = ConnectionPool(
            conninfo=settings.pg_dsn,
            min_size=settings.pg_pool_min_size,
            max_size=settings.pg_pool_max_size,
            timeout=settings.pg_pool_timeout_s,
            kwargs={"row_factory": dict_row},
            open=True,
        )
        atexit.register(self.close)

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        with self._pool.connection() as conn:
            yield conn

    def close(self) -> None:
        self._pool.close()

    def fetch_all(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self._connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or [])
                rows = cursor.fetchall()
        return [_json_safe(dict(row)) for row in rows]

    def fetch_one(self, query: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        with self._connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or [])
                row = cursor.fetchone()
        if row is None:
            return None
        return _json_safe(dict(row))

    def fetch_val(self, query: str, params: Sequence[Any] | None = None) -> Any:
        with self._connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params or [])
                row = cursor.fetchone()
        if row is None:
            return None
        value = next(iter(row.values()))
        return _json_safe(value)


db_client = DatabaseClient()
