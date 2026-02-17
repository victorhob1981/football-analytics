from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import boto3
import json
import pandas as pd
from io import BytesIO
import re


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
    # key contém .../run=YYYY-MM-DDTHHMMSSZ/...
    m = re.search(r"/run=([^/]+)/", key)
    return m.group(1) if m else None


def bronze_to_silver_latest_run():
    s3 = _s3()

    prefix = f"fixtures/league={LEAGUE_ID}/season={SEASON}/"
    resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)

    contents = resp.get("Contents", [])
    if not contents:
        raise Exception(f"Nenhum arquivo encontrado no bronze com prefix {prefix}")

    # Filtra só os data.json do backfill
    data_keys = [o["Key"] for o in contents if o["Key"].endswith("/data.json")]
    if not data_keys:
        raise Exception("Nenhum data.json encontrado (esperado .../run=.../data.json)")

    # Descobre o run mais recente baseado no run=... no path (não só LastModified)
    run_ids = []
    for k in data_keys:
        rid = _extract_run_id(k)
        if rid:
            run_ids.append(rid)

    if not run_ids:
        raise Exception("Não consegui extrair run=... das chaves. Verifique o padrão do path no bronze.")

    latest_run = sorted(set(run_ids))[-1]
    latest_keys = [k for k in data_keys if f"/run={latest_run}/" in k]

    print(f"Usando latest_run={latest_run}")
    print(f"Arquivos no run: {len(latest_keys)}")
    for k in latest_keys:
        print(f" - {k}")

    all_rows = []

    for key in latest_keys:
        obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
        payload = json.loads(obj["Body"].read().decode("utf-8"))

        errors = payload.get("errors")
        if errors:
            raise Exception(f"Erros no payload do bronze ({key}): {errors}")

        fixtures = payload.get("response", [])
        print(f"{key}: fixtures={len(fixtures)}")

        for item in fixtures:
            fixture = item.get("fixture", {}) or {}
            league = item.get("league", {}) or {}
            teams = item.get("teams", {}) or {}
            goals = item.get("goals", {}) or {}

            venue = fixture.get("venue") or {}
            status = fixture.get("status") or {}

            all_rows.append({
                "fixture_id": fixture.get("id"),
                "date_utc": fixture.get("date"),          # ISO string
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
            })

    if not all_rows:
        raise Exception("Nenhuma linha gerada a partir do bronze (all_rows vazio).")

    df = pd.DataFrame(all_rows)

    # Normalização mínima
    df = df.dropna(subset=["fixture_id"]).drop_duplicates(subset=["fixture_id"]).copy()

    # Colunas de partição (YYYY-MM)
    df["date"] = df["date_utc"].astype(str).str[:10]
    df["year"] = df["date"].str[:4]
    df["month"] = df["date"].str[5:7]

    # Grava 1 parquet consolidado por mês (melhor para lake)
    months = sorted(df[["year", "month"]].dropna().drop_duplicates().itertuples(index=False, name=None))
    print(f"Partições (year, month): {months}")

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

        print(f"Silver escrito: s3://{SILVER_BUCKET}/{out_key} | rows={len(part)}")

    print(f"Concluído. Total fixtures únicos: {len(df)}")


with DAG(
    dag_id="bronze_to_silver_fixtures_backfill",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["silver", "backfill"],
) as dag:

    PythonOperator(
        task_id="bronze_to_silver_latest_run",
        python_callable=bronze_to_silver_latest_run,
    )
