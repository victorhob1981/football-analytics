import subprocess
import shutil

import pytest


def test_dbt_compile_in_airflow_container():
    if shutil.which("docker") is None:
        pytest.skip("docker nao disponivel no ambiente de teste")

    precheck = subprocess.run(
        ["docker", "compose", "ps", "--status", "running"],
        capture_output=True,
        text=True,
    )
    if precheck.returncode != 0:
        pytest.skip("docker compose indisponivel para dbt compile em container")
    if "airflow-webserver" not in precheck.stdout and "football_airflow_webserver" not in precheck.stdout:
        pytest.skip("container airflow-webserver nao esta em execucao para dbt compile em container")

    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "airflow-webserver",
        "bash",
        "-lc",
        "dbt compile --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, (
        "dbt compile falhou.\n"
        f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )
