from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import re
from io import BytesIO

import boto3
import pandas as pd
from sqlalchemy import create_engine, text


SILVER_BUCKET = "football-silver"
LEAGUE_ID = 71
SEASON = 2024

TARGET_COLUMNS = [
    "fixture_id",
    "team_id",
    "team_name",
    "shots_on_goal",
    "shots_off_goal",
    "total_shots",
    "blocked_shots",
    "shots_inside_box",
    "shots_outside_box",
    "fouls",
    "corner_kicks",
    "offsides",
    "ball_possession",
    "yellow_cards",
    "red_cards",
    "goalkeeper_saves",
    "total_passes",
    "passes_accurate",
    "passes_pct",
    "ingested_run",
]

REQUIRED_INPUT_COLUMNS = ["fixture_id", "team_id", "team_name"]
INT_COLUMNS = [
    "fixture_id",
    "team_id",
    "shots_on_goal",
    "shots_off_goal",
    "total_shots",
    "blocked_shots",
    "shots_inside_box",
    "shots_outside_box",
    "fouls",
    "corner_kicks",
    "offsides",
    "ball_possession",
    "yellow_cards",
    "red_cards",
    "goalkeeper_saves",
    "total_passes",
    "passes_accurate",
]


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


def _latest_run(keys: list[str]) -> str:
    runs = []
    for key in keys:
        match = re.search(r"/run=([^/]+)/", key)
        if match:
            runs.append(match.group(1))
    if not runs:
        raise RuntimeError("Nao encontrei run=... nas chaves do silver/statistics.")
    return sorted(set(runs))[-1]


def _assert_target_columns(conn):
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'raw' AND table_name = 'match_statistics'
        """
    )
    found = {row[0] for row in conn.execute(sql)}
    missing = sorted(set(TARGET_COLUMNS) - found)
    if missing:
        raise ValueError(
            f"Tabela raw.match_statistics sem colunas esperadas: {missing}. "
            "Aplique warehouse/ddl/002_raw_statistics.sql."
        )


def _assert_required_input_columns(df: pd.DataFrame, source_key: str):
    missing = sorted(set(REQUIRED_INPUT_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(
            f"Schema invalido no parquet {source_key}. Colunas ausentes: {missing}. "
            f"Esperadas (minimas): {REQUIRED_INPUT_COLUMNS}"
        )


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for col in TARGET_COLUMNS:
        if col not in out.columns and col != "ingested_run":
            out[col] = pd.NA

    for col in INT_COLUMNS:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")

    out["team_name"] = out["team_name"].astype("string")
    out["passes_pct"] = pd.to_numeric(out["passes_pct"], errors="coerce")
    return out


def load_statistics_silver_to_postgres():
    s3 = _s3()
    engine = create_engine(_get_required_env("FOOTBALL_PG_DSN"))

    prefix = f"statistics/league={LEAGUE_ID}/season={SEASON}/"
    keys = _list_all_keys(s3, SILVER_BUCKET, prefix)
    if not keys:
        raise RuntimeError(f"Nenhum parquet encontrado no silver com prefixo {prefix}")

    parquet_keys = [key for key in keys if key.endswith("statistics.parquet")]
    if not parquet_keys:
        raise RuntimeError("Nenhum statistics.parquet encontrado no silver.")

    run_id = _latest_run(parquet_keys)
    run_keys = sorted([key for key in parquet_keys if f"/run={run_id}/" in key])
    if not run_keys:
        raise RuntimeError(f"Nao encontrei statistics.parquet para run={run_id}")

    print(f"Carregando statistics run={run_id} | arquivos={len(run_keys)}")

    read_rows = 0
    frames = []

    for key in run_keys:
        obj = s3.get_object(Bucket=SILVER_BUCKET, Key=key)
        df = pd.read_parquet(BytesIO(obj["Body"].read()))
        _assert_required_input_columns(df, key)
        read_rows += len(df)
        frames.append(df)
        print(f"Lido: {key} | rows={len(df)}")

    load_df = pd.concat(frames, ignore_index=True)
    load_df = _normalize_dataframe(load_df)

    invalid_mask = load_df["fixture_id"].isna() | load_df["team_id"].isna()
    invalid_rows = int(invalid_mask.sum())
    if invalid_rows:
        load_df = load_df[~invalid_mask].copy()

    before_dedup = len(load_df)
    load_df = load_df.drop_duplicates(subset=["fixture_id", "team_id"], keep="last").copy()
    duplicated_rows = before_dedup - len(load_df)

    load_df["ingested_run"] = run_id
    load_df = load_df[TARGET_COLUMNS]

    compare_columns = [col for col in TARGET_COLUMNS if col not in ("fixture_id", "team_id")]
    distinct_predicate = " OR ".join([f"t.{col} IS DISTINCT FROM s.{col}" for col in compare_columns])

    insert_cols = ", ".join(TARGET_COLUMNS)
    select_cols = ", ".join([f"s.{col}" for col in TARGET_COLUMNS])
    update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in compare_columns] + ["updated_at = now()"])
    conflict_where = " OR ".join([f"raw.match_statistics.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in compare_columns])

    with engine.begin() as conn:
        _assert_target_columns(conn)

        conn.execute(
            text(
                "CREATE TEMP TABLE staging_statistics (LIKE raw.match_statistics INCLUDING DEFAULTS) ON COMMIT DROP"
            )
        )

        load_df.to_sql(
            "staging_statistics",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
        )

        inserted = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM staging_statistics s
                LEFT JOIN raw.match_statistics t
                  ON t.fixture_id = s.fixture_id
                 AND t.team_id = s.team_id
                WHERE t.fixture_id IS NULL
                """
            )
        ).scalar_one()

        updated = conn.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM staging_statistics s
                JOIN raw.match_statistics t
                  ON t.fixture_id = s.fixture_id
                 AND t.team_id = s.team_id
                WHERE {distinct_predicate}
                """
            )
        ).scalar_one()

        conn.execute(
            text(
                f"""
                INSERT INTO raw.match_statistics ({insert_cols})
                SELECT {select_cols}
                FROM staging_statistics s
                ON CONFLICT (fixture_id, team_id) DO UPDATE
                SET {update_set}
                WHERE {conflict_where}
                """
            )
        )

        ignored = len(load_df) - inserted - updated

    print(
        "Load statistics concluido | "
        f"run={run_id} | lidas={read_rows} | validas={len(load_df)} | "
        f"inseridas={inserted} | atualizadas={updated} | ignoradas={ignored} | "
        f"invalidas_sem_chave={invalid_rows} | duplicadas_no_lote={duplicated_rows}"
    )


with DAG(
    dag_id="silver_to_postgres_statistics",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["warehouse", "load", "statistics"],
) as dag:
    PythonOperator(
        task_id="load_statistics_silver_to_postgres",
        python_callable=load_statistics_silver_to_postgres,
    )
