from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from common.observability import DEFAULT_DAG_ARGS


GE_RUNNER = "/opt/airflow/quality/great_expectations/run_checkpoints.py"


with DAG(
    dag_id="great_expectations_checks",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_DAG_ARGS,
    tags=["quality", "great_expectations", "validation"],
) as dag:
    ge_raw = BashOperator(
        task_id="ge_raw",
        bash_command=f"python {GE_RUNNER} --checkpoint raw_checkpoint",
        execution_timeout=timedelta(minutes=15),
    )

    ge_gold_marts = BashOperator(
        task_id="ge_gold_marts",
        bash_command=f"python {GE_RUNNER} --checkpoint gold_marts_checkpoint",
        execution_timeout=timedelta(minutes=15),
    )

    ge_raw >> ge_gold_marts
