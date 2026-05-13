from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import boto3
import json
from io import BytesIO


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria ausente: {name}")
    return value


def write_to_minio():
    s3 = boto3.client(
        "s3",
        endpoint_url=_get_required_env("MINIO_ENDPOINT_URL"),
        aws_access_key_id=_get_required_env("MINIO_ACCESS_KEY"),
        aws_secret_access_key=_get_required_env("MINIO_SECRET_KEY"),
    )

    data = {
        "message": "Hello from Airflow to MinIO",
        "timestamp": str(datetime.utcnow())
    }

    file_bytes = BytesIO(json.dumps(data).encode("utf-8"))

    s3.upload_fileobj(
        file_bytes,
        "football-bronze",
        "test/hello.json"
    )

    print("Arquivo enviado para o MinIO com sucesso.")


with DAG(
    dag_id="hello_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="write_file",
        python_callable=write_to_minio,
    )

    task
