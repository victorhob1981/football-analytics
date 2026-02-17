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


def _extract_run_id(key: str) -> str | None:
    match = re.search(r"/run=([^/]+)/", key)
    return match.group(1) if match else None


def bronze_to_silver_latest_run():
    context = get_current_context()
    s3 = _s3()

    prefix = f"fixtures/league={LEAGUE_ID}/season={SEASON}/"
    resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)

    contents = resp.get("Contents", [])
    if not contents:
        raise RuntimeError(f"Nenhum arquivo encontrado no bronze com prefix {prefix}")

    data_keys = [obj["Key"] for obj in contents if obj["Key"].endswith("/data.json")]
    if not data_keys:
        raise RuntimeError("Nenhum data.json encontrado (esperado .../run=.../data.json)")

    run_ids = [_extract_run_id(key) for key in data_keys]
    run_ids = [run_id for run_id in run_ids if run_id]
    if not run_ids:
        raise RuntimeError("Nao consegui extrair run=... das chaves.")

    latest_run = sorted(set(run_ids))[-1]
    latest_keys = [key for key in data_keys if f"/run={latest_run}/" in key]

    all_rows = []
    with StepMetrics(
        service="airflow",
        module="bronze_to_silver_fixtures_backfill",
        step="bronze_to_silver_latest_run",
        context=context,
        dataset="fixtures",
        table="football-silver",
    ) as metric:
        for key in latest_keys:
            obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
            payload = json.loads(obj["Body"].read().decode("utf-8"))

            errors = payload.get("errors")
            if errors:
                raise RuntimeError(f"Erros no payload do bronze ({key}): {errors}")

            fixtures = payload.get("response", [])
            for item in fixtures:
                fixture = item.get("fixture", {}) or {}
                league = item.get("league", {}) or {}
                teams = item.get("teams", {}) or {}
                goals = item.get("goals", {}) or {}

                venue = fixture.get("venue") or {}
                status = fixture.get("status") or {}

                all_rows.append(
                    {
                        "fixture_id": fixture.get("id"),
                        "date_utc": fixture.get("date"),
                        "timestamp": fixture.get("timestamp"),
                        "timezone": fixture.get("timezone"),
                        "referee": fixture.get("referee"),
                        "venue_id": venue.get("id"),
                        "venue_name": venue.get("name"),
                        "venue_city": venue.get("city"),
                        "status_short": status.get("short"),
                        "status_long": status.get("long"),
                        "league_id": league.get("id"),
                        "league_name": league.get("name"),
                        "season": league.get("season"),
                        "round": league.get("round"),
                        "home_team_id": (teams.get("home") or {}).get("id"),
                        "home_team_name": (teams.get("home") or {}).get("name"),
                        "away_team_id": (teams.get("away") or {}).get("id"),
                        "away_team_name": (teams.get("away") or {}).get("name"),
                        "home_goals": goals.get("home"),
                        "away_goals": goals.get("away"),
                    }
                )

        if not all_rows:
            raise RuntimeError("Nenhuma linha gerada a partir do bronze (all_rows vazio).")

        df = pd.DataFrame(all_rows)
        df = df.dropna(subset=["fixture_id"]).drop_duplicates(subset=["fixture_id"]).copy()
        df["date"] = df["date_utc"].astype(str).str[:10]
        df["year"] = df["date"].str[:4]
        df["month"] = df["date"].str[5:7]

        months = sorted(df[["year", "month"]].dropna().drop_duplicates().itertuples(index=False, name=None))
        written_rows = 0

        for year, month in months:
            part = df[(df["year"] == year) & (df["month"] == month)].copy()
            out_key = (
                f"fixtures/league={LEAGUE_ID}/season={SEASON}"
                f"/year={year}/month={month}/run={latest_run}/fixtures.parquet"
            )

            buf = BytesIO()
            part.to_parquet(buf, index=False)
            buf.seek(0)
            s3.upload_fileobj(buf, SILVER_BUCKET, out_key)
            written_rows += len(part)

        metric.set_counts(rows_in=len(all_rows), rows_out=written_rows, row_count=written_rows)

    log_event(
        service="airflow",
        module="bronze_to_silver_fixtures_backfill",
        step="summary",
        status="success",
        context=context,
        dataset="fixtures",
        rows_in=len(all_rows),
        rows_out=len(df),
        row_count=len(df),
        message=f"Concluido | latest_run={latest_run} | fixtures_unicos={len(df)}",
    )


with DAG(
    dag_id="bronze_to_silver_fixtures_backfill",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_DAG_ARGS,
    tags=["silver", "backfill"],
) as dag:
    PythonOperator(
        task_id="bronze_to_silver_latest_run",
        python_callable=bronze_to_silver_latest_run,
        execution_timeout=timedelta(minutes=20),
    )
