from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from sqlalchemy import create_engine, text

from common.observability import DEFAULT_DAG_ARGS, StepMetrics, log_event


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria ausente: {name}")
    return value


CHECKS = [
    {
        "check_name": "raw_events_orphan",
        "description": "raw.match_events possui eventos com fixture_id inexistente em raw.fixtures.",
        "sql": """
            SELECT e.*
            FROM raw.match_events e
            LEFT JOIN raw.fixtures f
              ON e.fixture_id = f.fixture_id
            WHERE f.fixture_id IS NULL
        """,
    },
    {
        "check_name": "mart_score_mismatch",
        "description": "mart.league_summary possui total_matches > 0 com total_goals = 0.",
        "sql": """
            SELECT *
            FROM mart.league_summary
            WHERE total_matches > 0
              AND total_goals = 0
        """,
    },
]


def run_data_quality_checks():
    context = get_current_context()
    engine = create_engine(_get_required_env("FOOTBALL_PG_DSN"))
    failed = []

    with StepMetrics(
        service="airflow",
        module="data_quality_checks",
        step="run_data_quality_checks",
        context=context,
        dataset="quality.sql_assertions",
    ) as metric:
        with engine.begin() as conn:
            for check in CHECKS:
                rows = conn.execute(text(check["sql"])).fetchall()
                bad_count = len(rows)

                log_event(
                    level="info" if bad_count == 0 else "error",
                    service="airflow",
                    module="data_quality_checks",
                    step=check["check_name"],
                    status="success" if bad_count == 0 else "failed",
                    context=context,
                    dataset="quality.sql_assertions",
                    row_count=bad_count,
                    message=(
                        f"[DQ {'PASS' if bad_count == 0 else 'FAIL'}] "
                        f"check={check['check_name']} | bad_rows={bad_count} | description={check['description']}"
                    ),
                )

                if bad_count > 0:
                    failed.append((check["check_name"], bad_count, check["description"]))

        metric.set_counts(rows_in=len(CHECKS), rows_out=len(CHECKS) - len(failed), row_count=len(CHECKS))

    if failed:
        summary = "; ".join([f"{name}(bad_rows={count}): {desc}" for name, count, desc in failed])
        raise ValueError(f"Data quality checks falharam: {summary}")


with DAG(
    dag_id="data_quality_checks",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_DAG_ARGS,
    tags=["quality", "validation", "warehouse"],
) as dag:
    PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks,
        execution_timeout=timedelta(minutes=10),
    )
