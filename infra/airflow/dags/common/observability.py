import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any


DEFAULT_RETRIES = 2
DEFAULT_RETRY_DELAY = timedelta(minutes=2)
DEFAULT_EXECUTION_TIMEOUT = timedelta(minutes=30)

DEFAULT_DAG_ARGS = {
    "retries": DEFAULT_RETRIES,
    "retry_delay": DEFAULT_RETRY_DELAY,
    "execution_timeout": DEFAULT_EXECUTION_TIMEOUT,
}


def _ts_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ctx_value(context: dict[str, Any] | None, key: str, env_key: str) -> str | None:
    if context and key in context and context[key] is not None:
        return str(context[key])
    value = os.getenv(env_key)
    return str(value) if value else None


def build_context_fields(context: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "dag_id": _ctx_value(context, "dag_id", "AIRFLOW_CTX_DAG_ID"),
        "task_id": _ctx_value(context, "task_id", "AIRFLOW_CTX_TASK_ID"),
        "run_id": _ctx_value(context, "run_id", "AIRFLOW_CTX_DAG_RUN_ID"),
    }


def _json_logger() -> logging.Logger:
    return logging.getLogger("football_observability")


def log_event(
    *,
    level: str = "info",
    service: str,
    module: str,
    step: str,
    status: str,
    context: dict[str, Any] | None = None,
    dataset: str | None = None,
    table: str | None = None,
    row_count: int | None = None,
    rows_in: int | None = None,
    rows_out: int | None = None,
    duration_ms: int | None = None,
    error_type: str | None = None,
    error_msg: str | None = None,
    message: str | None = None,
):
    payload: dict[str, Any] = {
        "ts": _ts_utc(),
        "level": level.upper(),
        "service": service,
        "module": module,
        "step": step,
        "status": status,
        "dataset": dataset,
        "table": table,
        "row_count": row_count,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "duration_ms": duration_ms,
        "error_type": error_type,
        "error_msg": error_msg,
    }
    payload.update(build_context_fields(context))

    logger = _json_logger()
    if message:
        logger.info(message)
    logger.info(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


class StepMetrics:
    def __init__(
        self,
        *,
        service: str,
        module: str,
        step: str,
        context: dict[str, Any] | None = None,
        dataset: str | None = None,
        table: str | None = None,
    ):
        self.service = service
        self.module = module
        self.step = step
        self.context = context
        self.dataset = dataset
        self.table = table
        self.rows_in: int | None = None
        self.rows_out: int | None = None
        self.row_count: int | None = None
        self.started_at = 0.0

    def set_counts(
        self,
        *,
        rows_in: int | None = None,
        rows_out: int | None = None,
        row_count: int | None = None,
    ):
        self.rows_in = rows_in
        self.rows_out = rows_out
        self.row_count = row_count

    def __enter__(self):
        self.started_at = time.perf_counter()
        log_event(
            service=self.service,
            module=self.module,
            step=self.step,
            status="started",
            context=self.context,
            dataset=self.dataset,
            table=self.table,
            message=f"[{self.module}] START step={self.step}",
        )
        return self

    def __exit__(self, exc_type, exc, _tb):
        duration_ms = int((time.perf_counter() - self.started_at) * 1000)
        if exc_type is None:
            log_event(
                service=self.service,
                module=self.module,
                step=self.step,
                status="success",
                context=self.context,
                dataset=self.dataset,
                table=self.table,
                row_count=self.row_count,
                rows_in=self.rows_in,
                rows_out=self.rows_out,
                duration_ms=duration_ms,
                message=f"[{self.module}] END step={self.step} duration_ms={duration_ms}",
            )
            return False

        log_event(
            level="error",
            service=self.service,
            module=self.module,
            step=self.step,
            status="failed",
            context=self.context,
            dataset=self.dataset,
            table=self.table,
            row_count=self.row_count,
            rows_in=self.rows_in,
            rows_out=self.rows_out,
            duration_ms=duration_ms,
            error_type=exc_type.__name__,
            error_msg=str(exc),
            message=f"[{self.module}] FAIL step={self.step} duration_ms={duration_ms} error={exc_type.__name__}",
        )
        return False
