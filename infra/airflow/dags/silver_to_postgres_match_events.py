from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.observability import DEFAULT_DAG_ARGS
from common.providers import get_default_league_id, get_default_provider
from common.services.warehouse_service import load_match_events_silver_to_raw

DEFAULT_PROVIDER = get_default_provider()
DEFAULT_LEAGUE_ID = get_default_league_id(DEFAULT_PROVIDER)


with DAG(
    dag_id="silver_to_postgres_match_events",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    params={"league_id": DEFAULT_LEAGUE_ID, "season": 2024, "provider": DEFAULT_PROVIDER},
    render_template_as_native_obj=True,
    default_args=DEFAULT_DAG_ARGS,
    tags=["warehouse", "load", "events"],
) as dag:
    PythonOperator(
        task_id="load_match_events_silver_to_postgres",
        python_callable=load_match_events_silver_to_raw,
        execution_timeout=timedelta(minutes=25),
    )
