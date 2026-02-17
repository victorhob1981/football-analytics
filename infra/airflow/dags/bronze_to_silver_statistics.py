from datetime import datetime, timedelta
import json
import os
import re
from io import BytesIO

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

from common.observability import DEFAULT_DAG_ARGS, StepMetrics, log_event


BRONZE_BUCKET = "football-bronze"
SILVER_BUCKET = "football-silver"
LEAGUE_ID = 71
SEASON = 2024


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria ausente: {name}")
    return value


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=_get_required_env("MINIO_ENDPOINT_URL"),
        aws_access_key_id=_get_required_env("MINIO_ACCESS_KEY"),
        aws_secret_access_key=_get_required_env("MINIO_SECRET_KEY"),
    )


def _list_all_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys = []
    token = None
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if token:
            params["ContinuationToken"] = token
        resp = s3.list_objects_v2(**params)
        keys.extend([obj["Key"] for obj in resp.get("Contents", [])])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def _extract_fixture_and_run(key: str) -> tuple[int, str] | None:
    match = re.search(r"/fixture_id=(\d+)/run=([^/]+)/data\.json$", key)
    if not match:
        return None
    return int(match.group(1)), match.group(2)


def _to_metric_column(stat_type: str | None) -> str | None:
    if not stat_type:
        return None
    normalized = stat_type.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or None


def _normalize_stat_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.endswith("%"):
            number = stripped[:-1].strip()
            if re.fullmatch(r"-?\d+", number):
                return int(number)
            return None
        if re.fullmatch(r"-?\d+", stripped):
            return int(stripped)
        return stripped
    return value


def bronze_to_silver_statistics_latest_per_fixture():
    context = get_current_context()
    s3 = _s3()
    prefix = f"statistics/league={LEAGUE_ID}/season={SEASON}/"
    keys = _list_all_keys(s3, BRONZE_BUCKET, prefix)
    if not keys:
        raise RuntimeError(f"Nenhum arquivo encontrado no bronze com prefixo: {prefix}")

    data_keys = [key for key in keys if key.endswith("/data.json")]
    if not data_keys:
        raise RuntimeError("Nenhum data.json encontrado para statistics no bronze.")

    latest_by_fixture = {}
    for key in data_keys:
        parsed = _extract_fixture_and_run(key)
        if not parsed:
            continue
        fixture_id, run_id = parsed
        current = latest_by_fixture.get(fixture_id)
        if current is None or run_id > current[0]:
            latest_by_fixture[fixture_id] = (run_id, key)

    if not latest_by_fixture:
        raise RuntimeError("Nao foi possivel extrair fixture_id/run dos arquivos statistics.")

    selected_items = sorted(
        [(fixture_id, run_id, key) for fixture_id, (run_id, key) in latest_by_fixture.items()],
        key=lambda item: item[0],
    )

    rows = []
    with StepMetrics(
        service="airflow",
        module="bronze_to_silver_statistics",
        step="bronze_to_silver_statistics_latest_per_fixture",
        context=context,
        dataset="statistics",
        table="football-silver",
    ) as metric:
        for fixture_id, _run_id, key in selected_items:
            obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
            payload = json.loads(obj["Body"].read().decode("utf-8"))
            if payload.get("errors"):
                continue

            response_rows = payload.get("response", []) or []
            if not isinstance(response_rows, list):
                continue

            for team_stats in response_rows:
                team = (team_stats or {}).get("team") or {}
                stats = (team_stats or {}).get("statistics") or []

                row = {
                    "fixture_id": fixture_id,
                    "team_id": team.get("id"),
                    "team_name": team.get("name"),
                }

                for stat in stats:
                    metric_name = _to_metric_column((stat or {}).get("type"))
                    if metric_name:
                        row[metric_name] = _normalize_stat_value((stat or {}).get("value"))
                rows.append(row)

        if not rows:
            raise RuntimeError("Nenhuma linha de statistics foi gerada apos processamento do bronze.")

        df = pd.DataFrame(rows)
        df["fixture_id"] = pd.to_numeric(df["fixture_id"], errors="coerce").astype("Int64")
        df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").astype("Int64")
        df["team_name"] = df["team_name"].astype("string")
        df = df.dropna(subset=["fixture_id", "team_id"]).drop_duplicates(subset=["fixture_id", "team_id"], keep="last")

        run_utc = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
        out_key = f"statistics/league={LEAGUE_ID}/season={SEASON}/run={run_utc}/statistics.parquet"

        buf = BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)
        s3.upload_fileobj(buf, SILVER_BUCKET, out_key)

        metric.set_counts(rows_in=len(rows), rows_out=len(df), row_count=len(df))

    log_event(
        service="airflow",
        module="bronze_to_silver_statistics",
        step="summary",
        status="success",
        context=context,
        dataset="statistics",
        row_count=len(df),
        rows_in=len(rows),
        rows_out=len(df),
        message=f"Bronze->Silver statistics concluido | rows={len(df)}",
    )


with DAG(
    dag_id="bronze_to_silver_statistics",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_DAG_ARGS,
    tags=["silver", "statistics"],
) as dag:
    PythonOperator(
        task_id="bronze_to_silver_statistics_latest_per_fixture",
        python_callable=bronze_to_silver_statistics_latest_per_fixture,
        execution_timeout=timedelta(minutes=20),
    )
