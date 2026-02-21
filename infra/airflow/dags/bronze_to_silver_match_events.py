from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.observability import DEFAULT_DAG_ARGS
from common.providers import get_default_league_id, get_default_provider
from common.services.mapping_service import map_match_events_raw_to_silver

DEFAULT_PROVIDER = get_default_provider()
DEFAULT_LEAGUE_ID = get_default_league_id(DEFAULT_PROVIDER)


with DAG(
    dag_id="bronze_to_silver_match_events",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    params={"league_id": DEFAULT_LEAGUE_ID, "season": 2024, "provider": DEFAULT_PROVIDER},
    render_template_as_native_obj=True,
    default_args=DEFAULT_DAG_ARGS,
    tags=["silver", "events"],
) as dag:
    PythonOperator(
        task_id="bronze_to_silver_match_events_latest_per_fixture",
        python_callable=map_match_events_raw_to_silver,
        execution_timeout=timedelta(minutes=20),
    )
