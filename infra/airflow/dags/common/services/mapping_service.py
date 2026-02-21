from __future__ import annotations

from datetime import datetime
import json
import os
import re
from io import BytesIO
from typing import Any, Callable

import boto3
import pandas as pd
from airflow.operators.python import get_current_context

from common.mappers import (
    build_competition_structure_dataframes,
    build_fixture_lineups_dataframe,
    build_fixture_player_statistics_dataframe,
    build_fixtures_dataframe,
    build_head_to_head_fixtures_dataframe,
    build_match_events_dataframe,
    build_player_season_statistics_dataframe,
    build_player_transfers_dataframe,
    build_standings_snapshots_dataframe,
    build_statistics_dataframe,
    build_team_coaches_dataframe,
    build_team_sidelined_dataframe,
)
from common.observability import StepMetrics, log_event
from common.runtime import resolve_runtime_params


BRONZE_BUCKET = "football-bronze"
SILVER_BUCKET = "football-silver"
RUN_PATTERN = re.compile(r"/run=([^/]+)/")
FIXTURE_RUN_PATTERN = re.compile(r"/fixture_id=(\d+)/run=([^/]+)/")
PLAYER_RUN_PATTERN = re.compile(r"/player_id=(\d+)/run=([^/]+)/")
TEAM_RUN_PATTERN = re.compile(r"/team_id=(\d+)/run=([^/]+)/")
PAIR_RUN_PATTERN = re.compile(r"/pair_index=(\d+)/run=([^/]+)/")


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


def _list_all_keys(s3_client, *, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        params: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            params["ContinuationToken"] = token
        resp = s3_client.list_objects_v2(**params)
        keys.extend([obj["Key"] for obj in resp.get("Contents", [])])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def _latest_run(keys: list[str]) -> str:
    runs = []
    for key in keys:
        match = RUN_PATTERN.search(key)
        if match:
            runs.append(match.group(1))
    if not runs:
        raise RuntimeError("Nao encontrei run=... nas chaves do bronze.")
    return sorted(set(runs))[-1]


def _load_json_payloads(s3_client, *, bucket: str, keys: list[str]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for key in keys:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        payloads.append(json.loads(obj["Body"].read().decode("utf-8")))
    return payloads


def _latest_key_by_fixture(data_keys: list[str]) -> list[str]:
    latest_by_fixture: dict[int, tuple[str, str]] = {}
    for key in data_keys:
        match = FIXTURE_RUN_PATTERN.search(key)
        if not match:
            continue
        fixture_id = int(match.group(1))
        run_id = match.group(2)
        current = latest_by_fixture.get(fixture_id)
        if current is None or run_id > current[0]:
            latest_by_fixture[fixture_id] = (run_id, key)
    return [key for _, key in sorted((v for v in latest_by_fixture.values()), key=lambda item: item[1])]


def _latest_key_by_scope(
    data_keys: list[str],
    *,
    pattern: re.Pattern[str],
    scope_name: str,
) -> list[str]:
    latest_by_scope: dict[int, tuple[str, str]] = {}
    for key in data_keys:
        match = pattern.search(key)
        if not match:
            continue
        scope_id = int(match.group(1))
        run_id = match.group(2)
        current = latest_by_scope.get(scope_id)
        if current is None or run_id > current[0]:
            latest_by_scope[scope_id] = (run_id, key)

    if not latest_by_scope:
        raise RuntimeError(f"Nao encontrei chaves por {scope_name} com run=... nas chaves do bronze.")

    return [key for _, key in sorted((v for v in latest_by_scope.values()), key=lambda item: item[1])]


def _write_df_to_silver(s3_client, *, df: pd.DataFrame, key: str) -> int:
    def _json_safe(value: Any) -> Any:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            return {str(k): _json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_json_safe(v) for v in value]
        if hasattr(value, "item"):
            try:
                return _json_safe(value.item())
            except Exception:
                pass
        if hasattr(value, "tolist"):
            try:
                return _json_safe(value.tolist())
            except Exception:
                pass
        return str(value)

    def _to_json_text(value: Any) -> str | None:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, str):
            return value
        return json.dumps(_json_safe(value), ensure_ascii=False)

    normalized_df = df.copy()
    for column in normalized_df.columns:
        series = normalized_df[column]
        if not pd.api.types.is_object_dtype(series.dtype):
            continue
        has_complex_value = series.map(
            lambda value: (
                value is not None
                and not (isinstance(value, float) and pd.isna(value))
                and (
                    isinstance(value, (dict, list, tuple, set))
                    or hasattr(value, "tolist")
                    or hasattr(value, "item")
                )
            )
        ).any()
        if has_complex_value:
            normalized_df[column] = series.map(_to_json_text)

    buf = BytesIO()
    normalized_df.to_parquet(buf, index=False)
    buf.seek(0)
    s3_client.upload_fileobj(buf, SILVER_BUCKET, key)
    return len(normalized_df)


def _data_json_keys(s3_client, *, prefix: str) -> list[str]:
    keys = _list_all_keys(s3_client, bucket=BRONZE_BUCKET, prefix=prefix)
    data_keys = [key for key in keys if key.endswith("/data.json")]
    if not data_keys:
        raise RuntimeError(f"Nenhum data.json encontrado no bronze com prefixo {prefix}")
    return data_keys


def _map_scoped_entity_raw_to_silver(
    *,
    context: dict[str, Any],
    league_id: int,
    season: int,
    dataset: str,
    prefix: str,
    scope_pattern: re.Pattern[str],
    scope_name: str,
    builder: Callable[[list[dict[str, Any]]], pd.DataFrame],
    output_key_builder: Callable[[str], str],
) -> None:
    s3_client = _s3_client()
    data_keys = _data_json_keys(s3_client, prefix=prefix)
    selected_keys = _latest_key_by_scope(data_keys, pattern=scope_pattern, scope_name=scope_name)
    payloads = _load_json_payloads(s3_client, bucket=BRONZE_BUCKET, keys=selected_keys)

    with StepMetrics(
        service="airflow",
        module="mapping_service",
        step=f"map_{dataset}_raw_to_silver",
        context=context,
        dataset=dataset,
        table="football-silver",
    ) as metric:
        df = builder(payloads)
        run_utc = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
        out_key = output_key_builder(run_utc)
        rows_written = _write_df_to_silver(s3_client, df=df, key=out_key)
        metric.set_counts(rows_in=len(payloads), rows_out=rows_written, row_count=rows_written)

    log_event(
        service="airflow",
        module="mapping_service",
        step="summary",
        status="success",
        context=context,
        dataset=dataset,
        rows_in=len(payloads),
        rows_out=len(df),
        row_count=len(df),
        message=(
            f"Raw->Silver {dataset} concluido | league_id={league_id} | season={season} "
            f"| payloads={len(payloads)} | rows={len(df)}"
        ),
    )


def map_fixtures_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]

    s3_client = _s3_client()
    prefix = f"fixtures/league={league_id}/season={season}/"
    data_keys = _data_json_keys(s3_client, prefix=prefix)

    latest_run = _latest_run(data_keys)
    selected_keys = [key for key in data_keys if f"/run={latest_run}/" in key]
    payloads = _load_json_payloads(s3_client, bucket=BRONZE_BUCKET, keys=selected_keys)

    with StepMetrics(
        service="airflow",
        module="mapping_service",
        step="map_fixtures_raw_to_silver",
        context=context,
        dataset="fixtures",
        table="football-silver",
    ) as metric:
        df = build_fixtures_dataframe(payloads)
        months = sorted(df[["year", "month"]].dropna().drop_duplicates().itertuples(index=False, name=None))
        written_rows = 0
        for year, month in months:
            part = df[(df["year"] == year) & (df["month"] == month)].copy()
            out_key = (
                f"fixtures/league={league_id}/season={season}"
                f"/year={year}/month={month}/run={latest_run}/fixtures.parquet"
            )
            written_rows += _write_df_to_silver(s3_client, df=part, key=out_key)
        metric.set_counts(rows_in=len(payloads), rows_out=written_rows, row_count=written_rows)

    log_event(
        service="airflow",
        module="mapping_service",
        step="summary",
        status="success",
        context=context,
        dataset="fixtures",
        rows_in=len(payloads),
        rows_out=len(df),
        row_count=len(df),
        message=f"Raw->Silver fixtures concluido | league_id={league_id} | season={season} | rows={len(df)}",
    )


def map_statistics_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]

    s3_client = _s3_client()
    prefix = f"statistics/league={league_id}/season={season}/"
    data_keys = _data_json_keys(s3_client, prefix=prefix)

    selected_keys = _latest_key_by_fixture(data_keys)
    payloads = _load_json_payloads(s3_client, bucket=BRONZE_BUCKET, keys=selected_keys)

    with StepMetrics(
        service="airflow",
        module="mapping_service",
        step="map_statistics_raw_to_silver",
        context=context,
        dataset="statistics",
        table="football-silver",
    ) as metric:
        df = build_statistics_dataframe(payloads)
        run_utc = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
        out_key = f"statistics/league={league_id}/season={season}/run={run_utc}/statistics.parquet"
        rows_written = _write_df_to_silver(s3_client, df=df, key=out_key)
        metric.set_counts(rows_in=len(payloads), rows_out=rows_written, row_count=rows_written)

    log_event(
        service="airflow",
        module="mapping_service",
        step="summary",
        status="success",
        context=context,
        dataset="statistics",
        rows_in=len(payloads),
        rows_out=len(df),
        row_count=len(df),
        message=f"Raw->Silver statistics concluido | league_id={league_id} | season={season} | rows={len(df)}",
    )


def map_match_events_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]

    s3_client = _s3_client()
    prefix = f"events/league={league_id}/season={season}/"
    data_keys = _data_json_keys(s3_client, prefix=prefix)

    selected_keys = _latest_key_by_fixture(data_keys)
    payloads = _load_json_payloads(s3_client, bucket=BRONZE_BUCKET, keys=selected_keys)

    with StepMetrics(
        service="airflow",
        module="mapping_service",
        step="map_match_events_raw_to_silver",
        context=context,
        dataset="match_events",
        table="football-silver",
    ) as metric:
        df = build_match_events_dataframe(payloads)
        run_utc = datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
        out_key = f"events/season={season}/league_id={league_id}/run={run_utc}/match_events.parquet"
        rows_written = _write_df_to_silver(s3_client, df=df, key=out_key)
        metric.set_counts(rows_in=len(payloads), rows_out=rows_written, row_count=rows_written)

    log_event(
        service="airflow",
        module="mapping_service",
        step="summary",
        status="success",
        context=context,
        dataset="match_events",
        rows_in=len(payloads),
        rows_out=len(df),
        row_count=len(df),
        message=f"Raw->Silver match_events concluido | league_id={league_id} | season={season} | rows={len(df)}",
    )


def map_competition_structure_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]

    s3_client = _s3_client()
    prefix = f"competition_structure/league={league_id}/season={season}/"
    data_keys = _data_json_keys(s3_client, prefix=prefix)
    latest_run = _latest_run(data_keys)
    selected_keys = [key for key in data_keys if f"/run={latest_run}/" in key]
    payloads = _load_json_payloads(s3_client, bucket=BRONZE_BUCKET, keys=selected_keys)

    with StepMetrics(
        service="airflow",
        module="mapping_service",
        step="map_competition_structure_raw_to_silver",
        context=context,
        dataset="competition_structure",
        table="football-silver",
    ) as metric:
        dataframes = build_competition_structure_dataframes(payloads)
        rows_written = 0
        for entity_name, df in dataframes.items():
            out_key = (
                f"competition_structure/league={league_id}/season={season}"
                f"/run={latest_run}/{entity_name}.parquet"
            )
            rows_written += _write_df_to_silver(s3_client, df=df, key=out_key)
        metric.set_counts(rows_in=len(payloads), rows_out=rows_written, row_count=rows_written)

    total_rows = sum(len(df) for df in dataframes.values())
    log_event(
        service="airflow",
        module="mapping_service",
        step="summary",
        status="success",
        context=context,
        dataset="competition_structure",
        rows_in=len(payloads),
        rows_out=total_rows,
        row_count=total_rows,
        message=(
            f"Raw->Silver competition_structure concluido | league_id={league_id} | season={season} "
            f"| payloads={len(payloads)} | rows={total_rows}"
        ),
    )


def map_standings_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]

    s3_client = _s3_client()
    prefix = f"standings/league={league_id}/season={season}/"
    data_keys = _data_json_keys(s3_client, prefix=prefix)
    latest_run = _latest_run(data_keys)
    selected_keys = [key for key in data_keys if f"/run={latest_run}/" in key]
    payloads = _load_json_payloads(s3_client, bucket=BRONZE_BUCKET, keys=selected_keys)

    with StepMetrics(
        service="airflow",
        module="mapping_service",
        step="map_standings_raw_to_silver",
        context=context,
        dataset="standings",
        table="football-silver",
    ) as metric:
        df = build_standings_snapshots_dataframe(payloads)
        out_key = f"standings/league={league_id}/season={season}/run={latest_run}/standings_snapshots.parquet"
        rows_written = _write_df_to_silver(s3_client, df=df, key=out_key)
        metric.set_counts(rows_in=len(payloads), rows_out=rows_written, row_count=rows_written)

    log_event(
        service="airflow",
        module="mapping_service",
        step="summary",
        status="success",
        context=context,
        dataset="standings",
        rows_in=len(payloads),
        rows_out=len(df),
        row_count=len(df),
        message=f"Raw->Silver standings concluido | league_id={league_id} | season={season} | rows={len(df)}",
    )


def map_fixture_lineups_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _map_scoped_entity_raw_to_silver(
        context=context,
        league_id=league_id,
        season=season,
        dataset="fixture_lineups",
        prefix=f"lineups/league={league_id}/season={season}/",
        scope_pattern=FIXTURE_RUN_PATTERN,
        scope_name="fixture_id",
        builder=build_fixture_lineups_dataframe,
        output_key_builder=lambda run_utc: f"lineups/league={league_id}/season={season}/run={run_utc}/fixture_lineups.parquet",
    )


def map_fixture_player_statistics_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _map_scoped_entity_raw_to_silver(
        context=context,
        league_id=league_id,
        season=season,
        dataset="fixture_player_statistics",
        prefix=f"fixture_player_statistics/league={league_id}/season={season}/",
        scope_pattern=FIXTURE_RUN_PATTERN,
        scope_name="fixture_id",
        builder=build_fixture_player_statistics_dataframe,
        output_key_builder=lambda run_utc: (
            f"fixture_player_statistics/league={league_id}/season={season}/run={run_utc}/fixture_player_statistics.parquet"
        ),
    )


def map_player_season_statistics_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _map_scoped_entity_raw_to_silver(
        context=context,
        league_id=league_id,
        season=season,
        dataset="player_season_statistics",
        prefix=f"player_season_statistics/league={league_id}/season={season}/",
        scope_pattern=PLAYER_RUN_PATTERN,
        scope_name="player_id",
        builder=build_player_season_statistics_dataframe,
        output_key_builder=lambda run_utc: (
            f"player_season_statistics/league={league_id}/season={season}/run={run_utc}/player_season_statistics.parquet"
        ),
    )


def map_player_transfers_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _map_scoped_entity_raw_to_silver(
        context=context,
        league_id=league_id,
        season=season,
        dataset="player_transfers",
        prefix=f"player_transfers/league={league_id}/season={season}/",
        scope_pattern=PLAYER_RUN_PATTERN,
        scope_name="player_id",
        builder=build_player_transfers_dataframe,
        output_key_builder=lambda run_utc: (
            f"player_transfers/league={league_id}/season={season}/run={run_utc}/player_transfers.parquet"
        ),
    )


def map_team_sidelined_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _map_scoped_entity_raw_to_silver(
        context=context,
        league_id=league_id,
        season=season,
        dataset="team_sidelined",
        prefix=f"team_sidelined/league={league_id}/season={season}/",
        scope_pattern=TEAM_RUN_PATTERN,
        scope_name="team_id",
        builder=build_team_sidelined_dataframe,
        output_key_builder=lambda run_utc: f"team_sidelined/league={league_id}/season={season}/run={run_utc}/team_sidelined.parquet",
    )


def map_team_coaches_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _map_scoped_entity_raw_to_silver(
        context=context,
        league_id=league_id,
        season=season,
        dataset="team_coaches",
        prefix=f"team_coaches/league={league_id}/season={season}/",
        scope_pattern=TEAM_RUN_PATTERN,
        scope_name="team_id",
        builder=build_team_coaches_dataframe,
        output_key_builder=lambda run_utc: f"team_coaches/league={league_id}/season={season}/run={run_utc}/team_coaches.parquet",
    )


def map_head_to_head_raw_to_silver():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _map_scoped_entity_raw_to_silver(
        context=context,
        league_id=league_id,
        season=season,
        dataset="head_to_head",
        prefix=f"head_to_head/league={league_id}/season={season}/",
        scope_pattern=PAIR_RUN_PATTERN,
        scope_name="pair_index",
        builder=build_head_to_head_fixtures_dataframe,
        output_key_builder=lambda run_utc: (
            f"head_to_head/league={league_id}/season={season}/run={run_utc}/head_to_head_fixtures.parquet"
        ),
    )
