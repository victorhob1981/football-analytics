from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DEFAULT_LEAGUE_ID = 71
DEFAULT_SEASON = 2024


def _safe_int(value, default_value: int, field_name: str) -> int:
    if value is None:
        return default_value
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Parametro invalido para {field_name}: {value}") from exc


def resolve_params() -> dict:
    context = get_current_context()
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    resolved = {
        "league_id": _safe_int(conf.get("league_id", params.get("league_id", DEFAULT_LEAGUE_ID)), DEFAULT_LEAGUE_ID, "league_id"),
        "season": _safe_int(conf.get("season", params.get("season", DEFAULT_SEASON)), DEFAULT_SEASON, "season"),
    }
    print(f"[pipeline_brasileirao] Params resolvidos: {resolved}")
    return resolved


def log_stage(stage: str, action: str):
    context = get_current_context()
    run_id = context.get("run_id")
    print(f"[pipeline_brasileirao] {action} | stage={stage} | run_id={run_id}")


with DAG(
    dag_id="pipeline_brasileirao",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    params={"league_id": DEFAULT_LEAGUE_ID, "season": DEFAULT_SEASON},
    render_template_as_native_obj=True,
    tags=["pipeline", "orchestrator", "brasileirao"],
) as dag:
    resolve_runtime_params = PythonOperator(
        task_id="resolve_runtime_params",
        python_callable=resolve_params,
    )

    start_ingest = PythonOperator(
        task_id="start_ingest",
        python_callable=log_stage,
        op_kwargs={"stage": "ingest_brasileirao_2024_backfill", "action": "START"},
    )
    run_ingest = TriggerDagRunOperator(
        task_id="run_ingest_brasileirao_2024_backfill",
        trigger_dag_id="ingest_brasileirao_2024_backfill",
        conf="{{ ti.xcom_pull(task_ids='resolve_runtime_params') }}",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    end_ingest = PythonOperator(
        task_id="end_ingest",
        python_callable=log_stage,
        op_kwargs={"stage": "ingest_brasileirao_2024_backfill", "action": "END"},
    )

    start_bronze_silver = PythonOperator(
        task_id="start_bronze_to_silver",
        python_callable=log_stage,
        op_kwargs={"stage": "bronze_to_silver_fixtures_backfill", "action": "START"},
    )
    run_bronze_silver = TriggerDagRunOperator(
        task_id="run_bronze_to_silver_fixtures_backfill",
        trigger_dag_id="bronze_to_silver_fixtures_backfill",
        conf="{{ ti.xcom_pull(task_ids='resolve_runtime_params') }}",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    end_bronze_silver = PythonOperator(
        task_id="end_bronze_to_silver",
        python_callable=log_stage,
        op_kwargs={"stage": "bronze_to_silver_fixtures_backfill", "action": "END"},
    )

    start_silver_postgres = PythonOperator(
        task_id="start_silver_to_postgres",
        python_callable=log_stage,
        op_kwargs={"stage": "silver_to_postgres_fixtures", "action": "START"},
    )
    run_silver_postgres = TriggerDagRunOperator(
        task_id="run_silver_to_postgres_fixtures",
        trigger_dag_id="silver_to_postgres_fixtures",
        conf="{{ ti.xcom_pull(task_ids='resolve_runtime_params') }}",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    end_silver_postgres = PythonOperator(
        task_id="end_silver_to_postgres",
        python_callable=log_stage,
        op_kwargs={"stage": "silver_to_postgres_fixtures", "action": "END"},
    )

    start_mart = PythonOperator(
        task_id="start_mart",
        python_callable=log_stage,
        op_kwargs={"stage": "mart_build_brasileirao_2024", "action": "START"},
    )
    run_mart = TriggerDagRunOperator(
        task_id="run_mart_build_brasileirao_2024",
        trigger_dag_id="mart_build_brasileirao_2024",
        conf="{{ ti.xcom_pull(task_ids='resolve_runtime_params') }}",
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    end_mart = PythonOperator(
        task_id="end_mart",
        python_callable=log_stage,
        op_kwargs={"stage": "mart_build_brasileirao_2024", "action": "END"},
    )

    (
        resolve_runtime_params
        >> start_ingest
        >> run_ingest
        >> end_ingest
        >> start_bronze_silver
        >> run_bronze_silver
        >> end_bronze_silver
        >> start_silver_postgres
        >> run_silver_postgres
        >> end_silver_postgres
        >> start_mart
        >> run_mart
        >> end_mart
    )
