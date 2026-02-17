import subprocess
import shutil


def test_dbt_compile_in_airflow_container():
    assert shutil.which("docker") is not None, (
        "docker nao disponivel no ambiente de teste. "
        "Instale e inicie o Docker Desktop para validar dbt compile no container Airflow."
    )

    precheck = subprocess.run(
        ["docker", "compose", "ps", "--status", "running"],
        capture_output=True,
        text=True,
    )
    assert precheck.returncode == 0, (
        "docker compose indisponivel para dbt compile em container.\n"
        f"STDOUT:\n{precheck.stdout}\nSTDERR:\n{precheck.stderr}"
    )
    assert (
        "airflow-webserver" in precheck.stdout or "football_airflow_webserver" in precheck.stdout
    ), (
        "container airflow-webserver nao esta em execucao para dbt compile em container. "
        "Suba a stack antes de rodar os testes: `docker compose up -d`."
    )

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
