import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import great_expectations as gx


DEFAULT_SEQUENCE = ["raw_checkpoint", "gold_marts_checkpoint"]


def _ts_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ctx() -> dict:
    return {
        "dag_id": os.getenv("AIRFLOW_CTX_DAG_ID"),
        "task_id": os.getenv("AIRFLOW_CTX_TASK_ID"),
        "run_id": os.getenv("AIRFLOW_CTX_DAG_RUN_ID"),
    }


def _log_json(level: str, step: str, status: str, message: str | None = None, **kwargs):
    payload = {
        "ts": _ts_utc(),
        "level": level.upper(),
        "service": "airflow",
        "module": "great_expectations_runner",
        **_ctx(),
        "step": step,
        "dataset": kwargs.pop("dataset", "quality"),
        "table": kwargs.pop("table", None),
        "row_count": kwargs.pop("row_count", None),
        "duration_ms": kwargs.pop("duration_ms", None),
        "status": status,
        "error_type": kwargs.pop("error_type", None),
        "error_msg": kwargs.pop("error_msg", None),
    }
    payload.update(kwargs)
    if message:
        print(message)
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


def run_checkpoint(checkpoint_name: str, context_root_dir: Path) -> bool:
    started = time.perf_counter()
    _log_json("info", checkpoint_name, "started", f"[GE] START checkpoint={checkpoint_name}")
    context = gx.get_context(context_root_dir=str(context_root_dir))
    result = context.run_checkpoint(checkpoint_name=checkpoint_name)

    success = bool(result.get("success", False))
    stats = result.get("statistics", {})
    evaluated = int(stats.get("evaluated_expectations", 0))
    successful = int(stats.get("successful_expectations", 0))
    failed = evaluated - successful
    duration_ms = int((time.perf_counter() - started) * 1000)

    _log_json(
        "info" if success else "error",
        checkpoint_name,
        "success" if success else "failed",
        (
            f"[GE] checkpoint={checkpoint_name} | success={success} | "
            f"evaluated={evaluated} | successful={successful} | failed={failed}"
        ),
        duration_ms=duration_ms,
        row_count=evaluated,
        evaluated=evaluated,
        successful=successful,
        failed=failed,
    )
    return success


def main():
    parser = argparse.ArgumentParser(description="Run Great Expectations checkpoints.")
    parser.add_argument(
        "--checkpoint",
        action="append",
        dest="checkpoints",
        help="Checkpoint name. Can be provided multiple times.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    checkpoints = args.checkpoints or DEFAULT_SEQUENCE

    failed = []
    for checkpoint in checkpoints:
        try:
            if not run_checkpoint(checkpoint, root):
                failed.append(checkpoint)
        except Exception as exc:
            failed.append(checkpoint)
            _log_json(
                "error",
                checkpoint,
                "failed",
                f"[GE] checkpoint={checkpoint} erro: {exc}",
                error_type=type(exc).__name__,
                error_msg=str(exc),
            )

    if failed:
        raise RuntimeError(f"Great Expectations falhou nos checkpoints: {failed}")

    _log_json("info", "summary", "success", f"[GE] Todos checkpoints passaram: {checkpoints}")


if __name__ == "__main__":
    main()
