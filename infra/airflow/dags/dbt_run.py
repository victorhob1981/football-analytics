from datetime import datetime, timedelta
import os
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

from common.observability import DEFAULT_DAG_ARGS, StepMetrics, log_event


DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"


def _run_cmd(step: str, cmd: list[str], dataset: str):
    context = get_current_context()
    env = os.environ.copy()

    with StepMetrics(
        service="airflow",
        module="dbt_run",
        step=step,
        context=context,
        dataset=dataset,
        table=dataset,
    ) as metric:
        result = subprocess.run(cmd, env=env, text=True, capture_output=True)
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            raise RuntimeError(f"Comando falhou ({step}) rc={result.returncode}")

        metric.set_counts(row_count=0)


def run_dbt_deps():
    _run_cmd(
        step="dbt_deps",
        cmd=["dbt", "deps", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR],
        dataset="dbt.packages",
    )


def run_dbt_core():
    _run_cmd(
        step="dbt_run_core",
        cmd=["dbt", "run", "--select", "marts.core", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR],
        dataset="marts.core",
    )


def run_dbt_analytics():
    _run_cmd(
        step="dbt_run_analytics",
        cmd=["dbt", "run", "--select", "marts.analytics", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR],
        dataset="marts.analytics",
    )


def run_dbt_test():
    _run_cmd(
        step="dbt_test",
        cmd=["dbt", "test", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR],
        dataset="dbt.tests",
    )


def analyze_db():
    context = get_current_context()
    with StepMetrics(
        service="airflow",
        module="dbt_run",
        step="analyze_db",
        context=context,
        dataset="postgres",
        table="all",
    ):
        from sqlalchemy import create_engine, text

        engine = create_engine(os.environ["FOOTBALL_PG_DSN"])
        conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("ANALYZE;"))
        conn.close()

    log_event(
        service="airflow",
        module="dbt_run",
        step="summary",
        status="success",
        context=context,
        dataset="dbt",
        message="dbt pipeline concluido (deps -> core -> analytics -> test -> analyze)",
    )


with DAG(
    dag_id="dbt_run",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_DAG_ARGS,
    tags=["dbt", "gold", "analytics"],
) as dag:
    dbt_deps = PythonOperator(
        task_id="dbt_deps",
        python_callable=run_dbt_deps,
        execution_timeout=timedelta(minutes=10),
    )

    dbt_run_core = PythonOperator(
        task_id="dbt_run_core",
        python_callable=run_dbt_core,
        execution_timeout=timedelta(minutes=30),
    )

    dbt_run_analytics = PythonOperator(
        task_id="dbt_run_analytics",
        python_callable=run_dbt_analytics,
        execution_timeout=timedelta(minutes=30),
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_test,
        execution_timeout=timedelta(minutes=20),
    )

    analyze_db_task = PythonOperator(
        task_id="analyze_db",
        python_callable=analyze_db,
        execution_timeout=timedelta(minutes=10),
    )

    dbt_deps >> dbt_run_core >> dbt_run_analytics >> dbt_test >> analyze_db_task
