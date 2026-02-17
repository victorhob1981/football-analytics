from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
from io import BytesIO

import requests
import boto3


LEAGUE_ID = 71  # Brasileirão Série A
SEASON = 2024

# 3 janelas do campeonato 2024
WINDOWS = [
    ("2024-04-13", "2024-06-30"),
    ("2024-07-01", "2024-09-30"),
    ("2024-10-01", "2024-12-08"),
]

BRONZE_BUCKET = "football-bronze"


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria ausente: {name}")
    return value


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=_get_required_env("MINIO_ENDPOINT_URL"),
        aws_access_key_id=_get_required_env("MINIO_ACCESS_KEY"),
        aws_secret_access_key=_get_required_env("MINIO_SECRET_KEY"),
    )


def _api_get(session: requests.Session, base_url: str, api_key: str, params: dict):
    url = f"{base_url}/fixtures"
    headers = {"x-apisports-key": api_key}

    response = session.get(url, headers=headers, params=params, timeout=30)

    if response.status_code != 200:
        raise Exception(f"Erro API: {response.status_code} - {response.text}")

    data = response.json()

    errors = data.get("errors")
    if errors:
        raise Exception(f"API errors: {errors}")

    return data, response.headers


def ingest_brasileirao_2024_backfill():
    api_key = _get_required_env("APIFOOTBALL_API_KEY")
    base_url = os.getenv("APIFOOTBALL_BASE_URL", "https://v3.football.api-sports.io")

    if not api_key:
        raise Exception("APIFOOTBALL_API_KEY não definida no ambiente do container Airflow")

    s3 = _s3_client()
    session = requests.Session()

    run_utc = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
    requests_used = 0

    for date_from, date_to in WINDOWS:
        params = {
            "league": LEAGUE_ID,
            "season": SEASON,
            "from": date_from,
            "to": date_to,
        }

        data, headers = _api_get(session, base_url, api_key, params)
        requests_used += 1

        results = data.get("results", 0)

        rate_headers = {
            k: v for k, v in headers.items()
            if "rate" in k.lower() or "limit" in k.lower()
        }

        print(f"[{date_from}..{date_to}] results={results} | rate_headers={rate_headers}")

        if results == 0:
            print(f"Nenhum fixture encontrado na janela {date_from}..{date_to}.")
            continue

        key = (
            f"fixtures/league={LEAGUE_ID}/season={SEASON}"
            f"/from={date_from}/to={date_to}"
            f"/run={run_utc}/data.json"
        )

        buffer = BytesIO(json.dumps(data).encode("utf-8"))
        s3.upload_fileobj(buffer, BRONZE_BUCKET, key)

        print(f"Salvo no bronze: s3://{BRONZE_BUCKET}/{key}")

    print(f"Backfill concluído. Requests usados: {requests_used}")


with DAG(
    dag_id="ingest_brasileirao_2024_backfill",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "backfill"],
) as dag:

    PythonOperator(
        task_id="ingest_fixtures_2024_in_windows",
        python_callable=ingest_brasileirao_2024_backfill,
    )
