import base64
import shutil
import subprocess

import pytest

pytestmark = pytest.mark.integration


def _run_python_in_airflow(script: str):
    if shutil.which("docker") is None:
        pytest.skip("docker nao disponivel no ambiente de teste")

    precheck = subprocess.run(
        ["docker", "compose", "ps", "--status", "running"],
        capture_output=True,
        text=True,
    )
    if precheck.returncode != 0:
        pytest.skip("docker compose indisponivel para testes de DAG import")
    if "airflow-webserver" not in precheck.stdout and "football_airflow_webserver" not in precheck.stdout:
        pytest.skip("container airflow-webserver nao esta em execucao para testes de DAG import")

    encoded = base64.b64encode(script.encode("utf-8")).decode("ascii")
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "airflow-webserver",
        "python",
        "-c",
        f"import base64; exec(base64.b64decode('{encoded}').decode('utf-8'))",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, (
        f"Erro ao validar DAGs no container Airflow.\n"
        f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def test_dag_parsing_import_and_key_tasks():
    script = """
from airflow.models import DagBag

dag_bag = DagBag(dag_folder='/opt/airflow/dags', include_examples=False)
assert not dag_bag.import_errors, f"Import errors: {dag_bag.import_errors}"

required_dags = [
    'pipeline_brasileirao',
    'dbt_run',
    'great_expectations_checks',
    'data_quality_checks',
]
missing_dags = [dag_id for dag_id in required_dags if dag_id not in dag_bag.dags]
assert not missing_dags, f"DAGs ausentes: {missing_dags}"

pipeline = dag_bag.get_dag('pipeline_brasileirao')
pipeline_task_ids = sorted(pipeline.task_ids)

def _pipeline_has_dbt_signal(task):
    task_id = task.task_id.lower()
    if "dbt" in task_id:
        return True
    trigger_dag_id = getattr(task, "trigger_dag_id", None)
    if trigger_dag_id and "dbt" in str(trigger_dag_id).lower():
        return True
    return False

def _pipeline_has_ge_signal(task):
    task_id = task.task_id.lower()
    if "great" in task_id or task_id.startswith("ge") or "_ge_" in task_id:
        return True
    trigger_dag_id = getattr(task, "trigger_dag_id", None)
    if trigger_dag_id and ("great" in str(trigger_dag_id).lower() or "ge" in str(trigger_dag_id).lower()):
        return True
    return False

def _pipeline_has_quality_signal(task):
    return "quality" in task.task_id.lower()

pipeline_tasks = list(pipeline.tasks)
assert any(_pipeline_has_dbt_signal(task) for task in pipeline_tasks), (
    "Nao encontrei sinal de etapa dbt em pipeline_brasileirao. "
    f"Task IDs disponiveis: {pipeline_task_ids}"
)
assert any(_pipeline_has_ge_signal(task) for task in pipeline_tasks), (
    "Nao encontrei sinal de etapa Great Expectations em pipeline_brasileirao. "
    f"Task IDs disponiveis: {pipeline_task_ids}"
)
assert any(_pipeline_has_quality_signal(task) for task in pipeline_tasks), (
    "Nao encontrei sinal de etapa quality em pipeline_brasileirao. "
    f"Task IDs disponiveis: {pipeline_task_ids}"
)

dbt_dag = dag_bag.get_dag('dbt_run')
dbt_task_ids = sorted(dbt_dag.task_ids)
assert len(dbt_task_ids) >= 3, (
    "DAG dbt_run tem tasks insuficientes para o fluxo esperado. "
    f"Task IDs disponiveis: {dbt_task_ids}"
)
assert any("dbt" in task_id.lower() for task_id in dbt_task_ids), (
    "DAG dbt_run nao possui task com sinal 'dbt' no task_id. "
    f"Task IDs disponiveis: {dbt_task_ids}"
)

ge_dag = dag_bag.get_dag('great_expectations_checks')
ge_task_ids = sorted(ge_dag.task_ids)
assert len(ge_task_ids) >= 2, (
    "DAG great_expectations_checks tem tasks insuficientes para o fluxo esperado. "
    f"Task IDs disponiveis: {ge_task_ids}"
)
assert any(("ge" in task_id.lower() or "great" in task_id.lower()) for task_id in ge_task_ids), (
    "DAG great_expectations_checks nao possui task com sinal 'ge/great' no task_id. "
    f"Task IDs disponiveis: {ge_task_ids}"
)
"""
    _run_python_in_airflow(script)
