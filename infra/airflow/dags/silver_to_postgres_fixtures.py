from datetime import datetime, timedelta
import os
import re
from io import BytesIO

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from sqlalchemy import create_engine, text

from common.observability import DEFAULT_DAG_ARGS, StepMetrics, log_event


SILVER_BUCKET = "football-silver"
LEAGUE_ID = 71
SEASON = 2024

TARGET_COLUMNS = [
    "fixture_id",
    "date_utc",
    "timestamp",
    "timezone",
    "referee",
    "venue_id",
    "venue_name",
    "venue_city",
    "status_short",
    "status_long",
    "league_id",
    "league_name",
    "season",
    "round",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "home_goals",
    "away_goals",
    "year",
    "month",
    "ingested_run",
]

REQUIRED_INPUT_COLUMNS = [c for c in TARGET_COLUMNS if c != "ingested_run"]


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


def _latest_run(keys: list[str]) -> str:
    runs = []
    for key in keys:
        match = re.search(r"/run=([^/]+)/", key)
        if match:
            runs.append(match.group(1))
    if not runs:
        raise RuntimeError("Nao encontrei run=... nas chaves do silver.")
    return sorted(set(runs))[-1]


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


def _assert_input_schema(df: pd.DataFrame, source_key: str):
    missing = sorted(set(REQUIRED_INPUT_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(
            f"Schema invalido no parquet {source_key}. Colunas ausentes: {missing}. "
            f"Esperadas (minimas): {REQUIRED_INPUT_COLUMNS}"
        )


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["fixture_id"] = pd.to_numeric(out["fixture_id"], errors="coerce").astype("Int64")

    int_cols = [
        "timestamp",
        "venue_id",
        "league_id",
        "season",
        "home_team_id",
        "away_team_id",
        "home_goals",
        "away_goals",
    ]
    for col in int_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")

    out["date_utc"] = pd.to_datetime(out["date_utc"], errors="coerce", utc=True)
    out["year"] = out["year"].astype("string")
    out["month"] = out["month"].astype("string")

    return out


def _assert_target_columns(conn):
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = 'fixtures'
        """
    )
    found = {row[0] for row in conn.execute(sql)}
    missing = sorted(set(TARGET_COLUMNS) - found)
    if missing:
        raise ValueError(f"Tabela raw.fixtures sem colunas esperadas: {missing}. Aplique migracoes via dbmate.")


def load_silver_to_postgres():
    context = get_current_context()
    s3 = _s3()
    engine = create_engine(_get_required_env("FOOTBALL_PG_DSN"))

    prefix = f"fixtures/league={LEAGUE_ID}/season={SEASON}/"
    keys = _list_all_keys(s3, SILVER_BUCKET, prefix)
    if not keys:
        raise RuntimeError(f"Nenhum parquet encontrado no silver com prefix {prefix}")

    parquet_keys = [key for key in keys if key.endswith("fixtures.parquet")]
    if not parquet_keys:
        raise RuntimeError("Nenhum fixtures.parquet encontrado no silver.")

    run_id = _latest_run(parquet_keys)
    run_keys = sorted([key for key in parquet_keys if f"/run={run_id}/" in key])
    if not run_keys:
        raise RuntimeError(f"Nao encontrei parquets para run={run_id}")

    read_rows = 0
    frames = []

    with StepMetrics(
        service="airflow",
        module="silver_to_postgres_fixtures",
        step="load_silver_to_postgres",
        context=context,
        dataset="raw.fixtures",
        table="raw.fixtures",
    ) as metric:
        for key in run_keys:
            obj = s3.get_object(Bucket=SILVER_BUCKET, Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()))
            _assert_input_schema(df, key)
            read_rows += len(df)
            frames.append(df)

        load_df = pd.concat(frames, ignore_index=True)
        load_df = _normalize_dataframe(load_df)

        invalid_mask = load_df["fixture_id"].isna()
        invalid_rows = int(invalid_mask.sum())
        if invalid_rows:
            load_df = load_df[~invalid_mask].copy()

        before_dedup = len(load_df)
        load_df = load_df.drop_duplicates(subset=["fixture_id"], keep="last").copy()
        duplicated_rows = before_dedup - len(load_df)

        load_df["ingested_run"] = run_id
        load_df = load_df[TARGET_COLUMNS]

        compare_columns = [col for col in TARGET_COLUMNS if col != "fixture_id"]
        distinct_predicate = " OR ".join([f"t.{col} IS DISTINCT FROM s.{col}" for col in compare_columns])

        insert_cols = ", ".join(TARGET_COLUMNS)
        select_cols = ", ".join([f"s.{col}" for col in TARGET_COLUMNS])
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in TARGET_COLUMNS if col != "fixture_id"])
        conflict_where = " OR ".join(
            [f"raw.fixtures.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in TARGET_COLUMNS if col != "fixture_id"]
        )

        with engine.begin() as conn:
            _assert_target_columns(conn)
            conn.execute(text("CREATE TEMP TABLE staging_fixtures (LIKE raw.fixtures INCLUDING DEFAULTS) ON COMMIT DROP"))

            load_df.to_sql("staging_fixtures", con=conn, if_exists="append", index=False, method="multi")

            inserted = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM staging_fixtures s
                    LEFT JOIN raw.fixtures t ON t.fixture_id = s.fixture_id
                    WHERE t.fixture_id IS NULL
                    """
                )
            ).scalar_one()

            updated = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM staging_fixtures s
                    JOIN raw.fixtures t ON t.fixture_id = s.fixture_id
                    WHERE {distinct_predicate}
                    """
                )
            ).scalar_one()

            conn.execute(
                text(
                    f"""
                    INSERT INTO raw.fixtures ({insert_cols})
                    SELECT {select_cols}
                    FROM staging_fixtures s
                    ON CONFLICT (fixture_id) DO UPDATE
                    SET {update_set}
                    WHERE {conflict_where}
                    """
                )
            )

            ignored = len(load_df) - inserted - updated

        metric.set_counts(rows_in=read_rows, rows_out=len(load_df), row_count=len(load_df))

    log_event(
        service="airflow",
        module="silver_to_postgres_fixtures",
        step="summary",
        status="success",
        context=context,
        dataset="raw.fixtures",
        rows_in=read_rows,
        rows_out=len(load_df),
        row_count=len(load_df),
        message=(
            f"Load concluido | run={run_id} | inseridas={inserted} | atualizadas={updated} | "
            f"ignoradas={ignored} | invalidas={invalid_rows} | duplicadas={duplicated_rows}"
        ),
    )


with DAG(
    dag_id="silver_to_postgres_fixtures",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_DAG_ARGS,
    tags=["warehouse", "load"],
) as dag:
    PythonOperator(
        task_id="load_silver_to_postgres",
        python_callable=load_silver_to_postgres,
        execution_timeout=timedelta(minutes=25),
    )
