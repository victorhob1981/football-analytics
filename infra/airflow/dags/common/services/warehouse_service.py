from __future__ import annotations

import json
import os
import re
from io import BytesIO
from typing import Callable

import boto3
import pandas as pd
from airflow.operators.python import get_current_context
from sqlalchemy import create_engine, text

from common.observability import StepMetrics, log_event
from common.runtime import resolve_runtime_params


SILVER_BUCKET = "football-silver"

FIXTURES_TARGET_COLUMNS = [
    "fixture_id",
    "source_provider",
    "date_utc",
    "timestamp",
    "timezone",
    "referee",
    "referee_id",
    "venue_id",
    "venue_name",
    "venue_city",
    "status_short",
    "status_long",
    "league_id",
    "league_name",
    "season",
    "round",
    "stage_id",
    "round_id",
    "attendance",
    "weather_description",
    "weather_temperature_c",
    "weather_wind_kph",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "home_goals",
    "away_goals",
    "home_goals_ht",
    "away_goals_ht",
    "home_goals_ft",
    "away_goals_ft",
    "competition_key",
    "competition_type",
    "season_label",
    "provider_season_id",
    "season_name",
    "season_start_date",
    "season_end_date",
    "source_run_id",
    "year",
    "month",
    "ingested_run",
]
FIXTURES_REQUIRED_COLUMNS = [
    "fixture_id",
    "source_provider",
    "date_utc",
    "timestamp",
    "timezone",
    "referee",
    "referee_id",
    "venue_id",
    "venue_name",
    "venue_city",
    "status_short",
    "status_long",
    "league_id",
    "league_name",
    "season",
    "round",
    "stage_id",
    "round_id",
    "attendance",
    "weather_description",
    "weather_temperature_c",
    "weather_wind_kph",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "home_goals",
    "away_goals",
    "home_goals_ht",
    "away_goals_ht",
    "home_goals_ft",
    "away_goals_ft",
    "year",
    "month",
]

STATISTICS_TARGET_COLUMNS = [
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
    "provider",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "source_run_id",
    "ingested_run",
]
STATISTICS_REQUIRED_COLUMNS = ["fixture_id", "team_id", "team_name"]
STATISTICS_INT_COLUMNS = [
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
    "provider_league_id",
    "provider_season_id",
]
STATISTICS_TEXT_COLUMNS = ["team_name", "provider", "competition_key", "season_label", "source_run_id"]

EVENTS_TARGET_COLUMNS = [
    "event_id",
    "season",
    "fixture_id",
    "time_elapsed",
    "time_extra",
    "is_time_elapsed_anomalous",
    "team_id",
    "team_name",
    "player_id",
    "player_name",
    "assist_id",
    "assist_name",
    "type",
    "detail",
    "comments",
    "provider",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "source_run_id",
    "ingested_run",
]
EVENTS_REQUIRED_COLUMNS = [
    "event_id",
    "fixture_id",
    "time_elapsed",
    "time_extra",
    "team_id",
    "team_name",
    "player_id",
    "player_name",
    "assist_id",
    "assist_name",
    "type",
    "detail",
    "comments",
]
EVENTS_INT_COLUMNS = [
    "season",
    "fixture_id",
    "time_elapsed",
    "time_extra",
    "team_id",
    "player_id",
    "assist_id",
    "provider_league_id",
    "provider_season_id",
]
EVENTS_BOOL_COLUMNS = ["is_time_elapsed_anomalous"]
EVENTS_TEXT_COLUMNS = [
    "event_id",
    "team_name",
    "player_name",
    "assist_name",
    "type",
    "detail",
    "comments",
    "provider",
    "competition_key",
    "season_label",
    "source_run_id",
]

COMPETITION_LEAGUES_TARGET_COLUMNS = [
    "provider",
    "league_id",
    "provider_league_id",
    "competition_key",
    "league_name",
    "country_id",
    "payload",
    "source_run_id",
    "ingested_run",
]
COMPETITION_LEAGUES_REQUIRED_COLUMNS = ["provider", "league_id"]
COMPETITION_LEAGUES_CONFLICT_KEYS = ["provider", "league_id"]
COMPETITION_LEAGUES_INT_COLUMNS = ["league_id", "provider_league_id", "country_id"]
COMPETITION_LEAGUES_JSON_COLUMNS = ["payload"]
COMPETITION_LEAGUES_TEXT_COLUMNS = ["provider", "competition_key", "league_name", "source_run_id"]

COMPETITION_SEASONS_TARGET_COLUMNS = [
    "provider",
    "season_id",
    "league_id",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "season_year",
    "season_name",
    "starting_at",
    "ending_at",
    "payload",
    "source_run_id",
    "ingested_run",
]
COMPETITION_SEASONS_REQUIRED_COLUMNS = ["provider", "season_id"]
COMPETITION_SEASONS_CONFLICT_KEYS = ["provider", "season_id"]
COMPETITION_SEASONS_INT_COLUMNS = [
    "season_id",
    "league_id",
    "provider_league_id",
    "provider_season_id",
    "season_year",
]
COMPETITION_SEASONS_DATE_COLUMNS = ["starting_at", "ending_at"]
COMPETITION_SEASONS_JSON_COLUMNS = ["payload"]
COMPETITION_SEASONS_TEXT_COLUMNS = [
    "provider",
    "competition_key",
    "season_label",
    "season_name",
    "source_run_id",
]

COMPETITION_STAGES_TARGET_COLUMNS = [
    "provider",
    "stage_id",
    "season_id",
    "league_id",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "stage_name",
    "sort_order",
    "finished",
    "is_current",
    "starting_at",
    "ending_at",
    "payload",
    "source_run_id",
    "ingested_run",
]
COMPETITION_STAGES_REQUIRED_COLUMNS = ["provider", "stage_id"]
COMPETITION_STAGES_CONFLICT_KEYS = ["provider", "stage_id"]
COMPETITION_STAGES_INT_COLUMNS = [
    "stage_id",
    "season_id",
    "league_id",
    "provider_league_id",
    "provider_season_id",
    "sort_order",
]
COMPETITION_STAGES_BOOL_COLUMNS = ["finished", "is_current"]
COMPETITION_STAGES_DATE_COLUMNS = ["starting_at", "ending_at"]
COMPETITION_STAGES_JSON_COLUMNS = ["payload"]
COMPETITION_STAGES_TEXT_COLUMNS = [
    "provider",
    "competition_key",
    "season_label",
    "stage_name",
    "source_run_id",
]

COMPETITION_ROUNDS_TARGET_COLUMNS = [
    "provider",
    "round_id",
    "stage_id",
    "season_id",
    "league_id",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "round_name",
    "finished",
    "is_current",
    "starting_at",
    "ending_at",
    "games_in_week",
    "payload",
    "source_run_id",
    "ingested_run",
]
COMPETITION_ROUNDS_REQUIRED_COLUMNS = ["provider", "round_id"]
COMPETITION_ROUNDS_CONFLICT_KEYS = ["provider", "round_id"]
COMPETITION_ROUNDS_INT_COLUMNS = [
    "round_id",
    "stage_id",
    "season_id",
    "league_id",
    "provider_league_id",
    "provider_season_id",
    "games_in_week",
]
COMPETITION_ROUNDS_BOOL_COLUMNS = ["finished", "is_current"]
COMPETITION_ROUNDS_DATE_COLUMNS = ["starting_at", "ending_at"]
COMPETITION_ROUNDS_JSON_COLUMNS = ["payload"]
COMPETITION_ROUNDS_TEXT_COLUMNS = [
    "provider",
    "competition_key",
    "season_label",
    "round_name",
    "source_run_id",
]

STANDINGS_SNAPSHOTS_TARGET_COLUMNS = [
    "provider",
    "league_id",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "season_id",
    "stage_id",
    "round_id",
    "team_id",
    "position",
    "points",
    "games_played",
    "won",
    "draw",
    "lost",
    "goals_for",
    "goals_against",
    "goal_diff",
    "payload",
    "source_run_id",
    "ingested_run",
]
STANDINGS_SNAPSHOTS_REQUIRED_COLUMNS = ["provider", "league_id", "season_id", "stage_id", "round_id", "team_id"]
STANDINGS_SNAPSHOTS_CONFLICT_KEYS = ["provider", "season_id", "stage_id", "round_id", "team_id"]
STANDINGS_SNAPSHOTS_INT_COLUMNS = [
    "league_id",
    "provider_league_id",
    "provider_season_id",
    "season_id",
    "stage_id",
    "round_id",
    "team_id",
    "position",
    "points",
    "games_played",
    "won",
    "draw",
    "lost",
    "goals_for",
    "goals_against",
    "goal_diff",
]
STANDINGS_SNAPSHOTS_JSON_COLUMNS = ["payload"]
STANDINGS_SNAPSHOTS_TEXT_COLUMNS = ["provider", "competition_key", "season_label", "source_run_id"]

FIXTURE_LINEUPS_TARGET_COLUMNS = [
    "provider",
    "fixture_id",
    "team_id",
    "player_id",
    "lineup_id",
    "position_id",
    "position_name",
    "lineup_type_id",
    "formation_field",
    "formation_position",
    "jersey_number",
    "details",
    "payload",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "source_run_id",
    "ingested_run",
]
FIXTURE_LINEUPS_REQUIRED_COLUMNS = ["provider", "fixture_id", "team_id", "lineup_id"]
FIXTURE_LINEUPS_CONFLICT_KEYS = ["provider", "fixture_id", "team_id", "lineup_id"]
FIXTURE_LINEUPS_INT_COLUMNS = [
    "fixture_id",
    "team_id",
    "player_id",
    "lineup_id",
    "position_id",
    "lineup_type_id",
    "formation_position",
    "jersey_number",
    "provider_league_id",
    "provider_season_id",
]
FIXTURE_LINEUPS_JSON_COLUMNS = ["details", "payload"]
FIXTURE_LINEUPS_TEXT_COLUMNS = [
    "provider",
    "position_name",
    "formation_field",
    "competition_key",
    "season_label",
    "source_run_id",
]

FIXTURE_PLAYER_STATISTICS_TARGET_COLUMNS = [
    "provider",
    "fixture_id",
    "team_id",
    "player_id",
    "statistics",
    "payload",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "source_run_id",
    "ingested_run",
]
FIXTURE_PLAYER_STATISTICS_REQUIRED_COLUMNS = ["provider", "fixture_id", "team_id", "player_id"]
FIXTURE_PLAYER_STATISTICS_CONFLICT_KEYS = ["provider", "fixture_id", "team_id", "player_id"]
FIXTURE_PLAYER_STATISTICS_INT_COLUMNS = [
    "fixture_id",
    "team_id",
    "player_id",
    "provider_league_id",
    "provider_season_id",
]
FIXTURE_PLAYER_STATISTICS_JSON_COLUMNS = ["statistics", "payload"]
FIXTURE_PLAYER_STATISTICS_TEXT_COLUMNS = [
    "provider",
    "competition_key",
    "season_label",
    "source_run_id",
]

PLAYER_SEASON_STATISTICS_TARGET_COLUMNS = [
    "provider",
    "player_id",
    "player_nationality",
    "season_id",
    "team_id",
    "league_id",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "season_name",
    "position_name",
    "statistics",
    "payload",
    "source_run_id",
    "ingested_run",
]
PLAYER_SEASON_STATISTICS_REQUIRED_COLUMNS = ["provider", "player_id", "season_id", "team_id"]
PLAYER_SEASON_STATISTICS_CONFLICT_KEYS = ["provider", "player_id", "season_id", "team_id"]
PLAYER_SEASON_STATISTICS_INT_COLUMNS = [
    "player_id",
    "season_id",
    "team_id",
    "league_id",
    "provider_league_id",
    "provider_season_id",
]
PLAYER_SEASON_STATISTICS_JSON_COLUMNS = ["statistics", "payload"]
PLAYER_SEASON_STATISTICS_TEXT_COLUMNS = [
    "provider",
    "player_nationality",
    "competition_key",
    "season_label",
    "season_name",
    "position_name",
    "source_run_id",
]

PLAYER_TRANSFERS_TARGET_COLUMNS = [
    "provider",
    "transfer_id",
    "player_id",
    "from_team_id",
    "to_team_id",
    "transfer_date",
    "completed",
    "career_ended",
    "type_id",
    "position_id",
    "amount",
    "payload",
    "ingested_run",
]
PLAYER_TRANSFERS_REQUIRED_COLUMNS = ["provider", "transfer_id", "player_id"]
PLAYER_TRANSFERS_CONFLICT_KEYS = ["provider", "transfer_id"]
PLAYER_TRANSFERS_INT_COLUMNS = ["transfer_id", "player_id", "from_team_id", "to_team_id", "type_id", "position_id"]
PLAYER_TRANSFERS_BOOL_COLUMNS = ["completed", "career_ended"]
PLAYER_TRANSFERS_DATE_COLUMNS = ["transfer_date"]
PLAYER_TRANSFERS_JSON_COLUMNS = ["payload"]
PLAYER_TRANSFERS_TEXT_COLUMNS = ["provider", "amount"]

TEAM_SIDELINED_TARGET_COLUMNS = [
    "provider",
    "sidelined_id",
    "team_id",
    "player_id",
    "season_id",
    "category",
    "type_id",
    "start_date",
    "end_date",
    "games_missed",
    "completed",
    "payload",
    "ingested_run",
]
TEAM_SIDELINED_REQUIRED_COLUMNS = ["provider", "sidelined_id", "team_id", "player_id"]
TEAM_SIDELINED_CONFLICT_KEYS = ["provider", "sidelined_id"]
TEAM_SIDELINED_INT_COLUMNS = ["sidelined_id", "team_id", "player_id", "season_id", "type_id", "games_missed"]
TEAM_SIDELINED_BOOL_COLUMNS = ["completed"]
TEAM_SIDELINED_DATE_COLUMNS = ["start_date", "end_date"]
TEAM_SIDELINED_JSON_COLUMNS = ["payload"]
TEAM_SIDELINED_TEXT_COLUMNS = ["provider", "category"]

TEAM_COACHES_TARGET_COLUMNS = [
    "provider",
    "coach_tenure_id",
    "team_id",
    "coach_id",
    "position_id",
    "active",
    "temporary",
    "start_date",
    "end_date",
    "payload",
    "ingested_run",
]
TEAM_COACHES_REQUIRED_COLUMNS = ["provider", "coach_tenure_id", "team_id", "coach_id"]
TEAM_COACHES_CONFLICT_KEYS = ["provider", "coach_tenure_id"]
TEAM_COACHES_INT_COLUMNS = ["coach_tenure_id", "team_id", "coach_id", "position_id"]
TEAM_COACHES_BOOL_COLUMNS = ["active", "temporary"]
TEAM_COACHES_DATE_COLUMNS = ["start_date", "end_date"]
TEAM_COACHES_JSON_COLUMNS = ["payload"]
TEAM_COACHES_TEXT_COLUMNS = ["provider"]

COACHES_TARGET_COLUMNS = [
    "provider",
    "coach_id",
    "coach_name",
    "image_path",
    "payload",
    "ingested_run",
]
COACHES_REQUIRED_COLUMNS = ["provider", "coach_id"]
COACHES_CONFLICT_KEYS = ["provider", "coach_id"]
COACHES_INT_COLUMNS = ["coach_id"]
COACHES_JSON_COLUMNS = ["payload"]
COACHES_TEXT_COLUMNS = ["provider", "coach_name", "image_path"]

HEAD_TO_HEAD_FIXTURES_TARGET_COLUMNS = [
    "provider",
    "pair_team_id",
    "pair_opponent_id",
    "fixture_id",
    "league_id",
    "season_id",
    "match_date",
    "home_team_id",
    "away_team_id",
    "home_goals",
    "away_goals",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "payload",
    "source_run_id",
    "ingested_run",
]
HEAD_TO_HEAD_FIXTURES_REQUIRED_COLUMNS = ["provider", "pair_team_id", "pair_opponent_id", "fixture_id"]
HEAD_TO_HEAD_FIXTURES_CONFLICT_KEYS = ["provider", "pair_team_id", "pair_opponent_id", "fixture_id"]
HEAD_TO_HEAD_FIXTURES_INT_COLUMNS = [
    "pair_team_id",
    "pair_opponent_id",
    "fixture_id",
    "league_id",
    "season_id",
    "home_team_id",
    "away_team_id",
    "home_goals",
    "away_goals",
    "provider_league_id",
    "provider_season_id",
]
HEAD_TO_HEAD_FIXTURES_DATETIME_COLUMNS = ["match_date"]
HEAD_TO_HEAD_FIXTURES_JSON_COLUMNS = ["payload"]
HEAD_TO_HEAD_FIXTURES_TEXT_COLUMNS = [
    "provider",
    "competition_key",
    "season_label",
    "source_run_id",
]


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
    keys = []
    token = None
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
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
        match = re.search(r"/run=([^/]+)/", key)
        if match:
            runs.append(match.group(1))
    if not runs:
        raise RuntimeError("Nao encontrei run=... nas chaves do silver.")
    return sorted(set(runs))[-1]


def _assert_columns(df: pd.DataFrame, expected: list[str], source_key: str):
    missing = sorted(set(expected) - set(df.columns))
    if missing:
        raise ValueError(f"Schema invalido no parquet {source_key}. Colunas ausentes: {missing}.")


def _assert_target_columns(conn, *, schema: str, table: str, expected: list[str]):
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
        """
    )
    found = {row[0] for row in conn.execute(sql, {"schema": schema, "table": table})}
    missing = sorted(set(expected) - found)
    if missing:
        raise ValueError(f"Tabela {schema}.{table} sem colunas esperadas: {missing}.")


def _to_json_text(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str):
        return value

    def _json_safe(raw):
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            return None
        if isinstance(raw, (str, int, float, bool)):
            return raw
        if isinstance(raw, dict):
            return {str(key): _json_safe(item) for key, item in raw.items()}
        if isinstance(raw, (list, tuple, set)):
            return [_json_safe(item) for item in raw]
        if hasattr(raw, "item"):
            try:
                return _json_safe(raw.item())
            except Exception:
                pass
        if hasattr(raw, "tolist"):
            try:
                return _json_safe(raw.tolist())
            except Exception:
                pass
        return str(raw)

    return json.dumps(_json_safe(value), ensure_ascii=False)


def _coerce_bool(series: pd.Series) -> pd.Series:
    mapping = {
        "true": True,
        "1": True,
        "t": True,
        "yes": True,
        "y": True,
        "false": False,
        "0": False,
        "f": False,
        "no": False,
        "n": False,
    }
    normalized = series.map(lambda value: str(value).strip().lower() if value is not None and not pd.isna(value) else value)
    coerced = normalized.map(lambda value: mapping.get(value) if isinstance(value, str) else value)
    return coerced.astype("boolean")


def _read_latest_parquet_run(
    s3_client,
    *,
    prefix: str,
    suffix: str,
) -> tuple[str, list[str]]:
    keys = _list_all_keys(s3_client, bucket=SILVER_BUCKET, prefix=prefix)
    parquet_keys = [key for key in keys if key.endswith(suffix)]
    if not parquet_keys:
        raise RuntimeError(f"Nenhum arquivo {suffix} encontrado com prefixo {prefix}")

    run_id = _latest_run(parquet_keys)
    run_keys = sorted([key for key in parquet_keys if f"/run={run_id}/" in key])
    if not run_keys:
        raise RuntimeError(f"Nenhuma chave de run encontrada para prefixo {prefix} run={run_id}")
    return run_id, run_keys


def _read_fixture_semantic_scope(conn) -> pd.DataFrame:
    query = text(
        """
        SELECT
            cpm.provider AS source_provider,
            cpm.provider_league_id AS league_id,
            CAST(LEFT(sc.season_label, 4) AS INTEGER) AS operational_season,
            cpm.competition_key,
            c.competition_type,
            sc.season_label,
            sc.provider_season_id,
            sc.season_start_date,
            sc.season_end_date
        FROM control.competition_provider_map cpm
        JOIN control.competitions c
          ON c.competition_key = cpm.competition_key
        JOIN control.season_catalog sc
          ON sc.competition_key = cpm.competition_key
         AND sc.provider = cpm.provider
        WHERE cpm.provider IS NOT NULL
          AND cpm.provider_league_id IS NOT NULL
          AND sc.provider_season_id IS NOT NULL
        """
    )
    return pd.read_sql_query(query, conn)


def _read_catalog_competition_scope(conn) -> pd.DataFrame:
    query = text(
        """
        SELECT DISTINCT
            provider,
            provider_league_id,
            competition_key
        FROM control.competition_provider_map
        WHERE provider IS NOT NULL
          AND provider_league_id IS NOT NULL
          AND competition_key IS NOT NULL
        """
    )
    return pd.read_sql_query(query, conn)


def _read_catalog_competition_season_scope(conn) -> pd.DataFrame:
    query = text(
        """
        SELECT DISTINCT
            cpm.provider,
            cpm.provider_league_id,
            cpm.competition_key,
            sc.season_label,
            sc.provider_season_id
        FROM control.competition_provider_map cpm
        JOIN control.season_catalog sc
          ON sc.provider = cpm.provider
         AND sc.competition_key = cpm.competition_key
        WHERE cpm.provider IS NOT NULL
          AND cpm.provider_league_id IS NOT NULL
          AND cpm.competition_key IS NOT NULL
          AND sc.provider_season_id IS NOT NULL
        """
    )
    return pd.read_sql_query(query, conn)


def _season_name_from_label(label: str | None) -> str | None:
    if label is None or (isinstance(label, float) and pd.isna(label)):
        return None
    text_label = str(label).strip()
    if not text_label:
        return None
    return text_label.replace("_", "/")


def _enrich_fixtures_semantics(
    load_df: pd.DataFrame,
    *,
    scope_df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    enriched = load_df.copy()

    if scope_df.empty:
        sample = (
            enriched.loc[:, ["source_provider", "league_id", "season"]]
            .drop_duplicates()
            .head(5)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de fixtures "
            f"via control.competition_provider_map + control.season_catalog. Escopos de amostra: {sample}"
        )

    join_keys = ["source_provider", "league_id", "operational_season"]
    duplicated = scope_df.duplicated(subset=join_keys, keep=False)
    if bool(duplicated.any()):
        ambiguous = scope_df.loc[
            duplicated,
            join_keys + ["competition_key", "season_label", "provider_season_id"],
        ]
        raise RuntimeError(
            "Mapeamento semantico ambiguo para fixtures. "
            f"Escopos ambiguos: {ambiguous.drop_duplicates().head(10).to_dict('records')}"
        )

    merge_scope = scope_df.rename(
        columns={
            "operational_season": "_scope_operational_season",
            "competition_key": "_scope_competition_key",
            "competition_type": "_scope_competition_type",
            "season_label": "_scope_season_label",
            "provider_season_id": "_scope_provider_season_id",
            "season_start_date": "_scope_season_start_date",
            "season_end_date": "_scope_season_end_date",
        }
    )
    enriched = enriched.merge(
        merge_scope,
        how="left",
        left_on=["source_provider", "league_id", "season"],
        right_on=["source_provider", "league_id", "_scope_operational_season"],
    )
    enriched["competition_key"] = enriched["_scope_competition_key"]
    enriched["competition_type"] = enriched["_scope_competition_type"]
    enriched["season_label"] = enriched["_scope_season_label"]
    enriched["provider_season_id"] = enriched["_scope_provider_season_id"]
    enriched["season_start_date"] = pd.to_datetime(enriched["_scope_season_start_date"], errors="coerce").dt.date
    enriched["season_end_date"] = pd.to_datetime(enriched["_scope_season_end_date"], errors="coerce").dt.date
    enriched["season_name"] = enriched["season_label"].map(_season_name_from_label).astype("string")
    enriched["source_run_id"] = run_id
    enriched = enriched.drop(
        columns=[
            "_scope_operational_season",
            "_scope_competition_key",
            "_scope_competition_type",
            "_scope_season_label",
            "_scope_provider_season_id",
            "_scope_season_start_date",
            "_scope_season_end_date",
        ]
    )

    unresolved_mask = (
        enriched["competition_key"].isna()
        | enriched["competition_type"].isna()
        | enriched["season_label"].isna()
        | enriched["provider_season_id"].isna()
    )
    if bool(unresolved_mask.any()):
        sample = (
            enriched.loc[unresolved_mask, ["fixture_id", "source_provider", "league_id", "season"]]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de fixtures "
            f"para todas as linhas. Fixtures sem match confiavel: {sample}"
        )
    return enriched


def _enrich_competition_leagues_semantics(
    load_df: pd.DataFrame,
    *,
    scope_df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    enriched = load_df.copy()
    enriched["provider_league_id"] = enriched["league_id"]

    if scope_df.empty:
        sample = (
            enriched.loc[:, ["provider", "league_id"]]
            .drop_duplicates()
            .head(5)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de competition_leagues "
            f"via control.competition_provider_map. Escopos de amostra: {sample}"
        )

    duplicated = scope_df.duplicated(subset=["provider", "provider_league_id"], keep=False)
    if bool(duplicated.any()):
        ambiguous = scope_df.loc[duplicated, ["provider", "provider_league_id", "competition_key"]]
        raise RuntimeError(
            "Mapeamento semantico ambiguo para competition_leagues. "
            f"Escopos ambiguos: {ambiguous.drop_duplicates().head(10).to_dict('records')}"
        )

    merge_scope = scope_df.rename(
        columns={
            "provider_league_id": "_scope_provider_league_id",
            "competition_key": "_scope_competition_key",
        }
    )
    enriched = enriched.merge(
        merge_scope,
        how="left",
        left_on=["provider", "provider_league_id"],
        right_on=["provider", "_scope_provider_league_id"],
    )
    enriched["competition_key"] = enriched["_scope_competition_key"]
    enriched["source_run_id"] = run_id
    enriched = enriched.drop(columns=["_scope_provider_league_id", "_scope_competition_key"])

    unresolved_mask = enriched["provider_league_id"].isna() | enriched["competition_key"].isna()
    if bool(unresolved_mask.any()):
        sample = (
            enriched.loc[unresolved_mask, ["provider", "league_id"]]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de competition_leagues "
            f"para todas as linhas. Escopos sem match confiavel: {sample}"
        )
    return enriched


def _enrich_catalog_season_semantics(
    load_df: pd.DataFrame,
    *,
    scope_df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    enriched = load_df.copy()
    enriched["provider_league_id"] = enriched["league_id"]
    enriched["provider_season_id"] = enriched["season_id"]

    if scope_df.empty:
        sample = (
            enriched.loc[:, ["provider", "provider_league_id", "season_id"]]
            .drop_duplicates()
            .head(5)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica por season "
            f"via control.competition_provider_map + control.season_catalog. Escopos de amostra: {sample}"
        )

    join_keys = ["provider", "provider_league_id", "provider_season_id"]
    duplicated = scope_df.duplicated(subset=join_keys, keep=False)
    if bool(duplicated.any()):
        ambiguous = scope_df.loc[duplicated, join_keys + ["competition_key", "season_label"]]
        raise RuntimeError(
            "Mapeamento semantico ambiguo para escopos por season. "
            f"Escopos ambiguos: {ambiguous.drop_duplicates().head(10).to_dict('records')}"
        )

    merge_scope = scope_df.rename(
        columns={
            "provider_league_id": "_scope_provider_league_id",
            "provider_season_id": "_scope_provider_season_id",
            "competition_key": "_scope_competition_key",
            "season_label": "_scope_season_label",
        }
    )
    enriched = enriched.merge(
        merge_scope,
        how="left",
        left_on=["provider", "provider_league_id", "provider_season_id"],
        right_on=["provider", "_scope_provider_league_id", "_scope_provider_season_id"],
    )
    enriched["competition_key"] = enriched["_scope_competition_key"]
    enriched["season_label"] = enriched["_scope_season_label"]
    enriched["source_run_id"] = run_id
    enriched = enriched.drop(
        columns=[
            "_scope_provider_league_id",
            "_scope_provider_season_id",
            "_scope_competition_key",
            "_scope_season_label",
        ]
    )

    unresolved_mask = (
        enriched["provider_league_id"].isna()
        | enriched["competition_key"].isna()
        | enriched["season_label"].isna()
        | enriched["provider_season_id"].isna()
    )
    if bool(unresolved_mask.any()):
        sample = (
            enriched.loc[unresolved_mask, ["provider", "league_id", "season_id"]]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica por season "
            f"para todas as linhas. Escopos sem match confiavel: {sample}"
        )
    return enriched


def _read_player_season_statistics_semantic_scope(conn) -> pd.DataFrame:
    query = text(
        """
        SELECT DISTINCT
            f.source_provider AS provider,
            f.league_id AS provider_league_id,
            f.provider_season_id,
            f.competition_key,
            f.season_label
        FROM raw.fixtures f
        JOIN control.season_catalog sc
          ON sc.provider = f.source_provider
         AND sc.competition_key = f.competition_key
         AND sc.season_label = f.season_label
         AND sc.provider_season_id = f.provider_season_id
        WHERE f.source_provider IS NOT NULL
          AND f.league_id IS NOT NULL
          AND f.provider_season_id IS NOT NULL
          AND f.competition_key IS NOT NULL
          AND f.season_label IS NOT NULL
        """
    )
    return pd.read_sql_query(query, conn)


def _read_statistics_semantic_scope(conn) -> pd.DataFrame:
    query = text(
        """
        SELECT DISTINCT
            f.fixture_id,
            f.source_provider AS provider,
            f.league_id AS provider_league_id,
            f.competition_key,
            f.season_label,
            f.provider_season_id
        FROM raw.fixtures f
        JOIN control.season_catalog sc
          ON sc.provider = f.source_provider
         AND sc.competition_key = f.competition_key
         AND sc.season_label = f.season_label
         AND sc.provider_season_id = f.provider_season_id
        WHERE f.fixture_id IS NOT NULL
          AND f.source_provider IS NOT NULL
          AND f.league_id IS NOT NULL
          AND f.competition_key IS NOT NULL
          AND f.season_label IS NOT NULL
          AND f.provider_season_id IS NOT NULL
        """
    )
    return pd.read_sql_query(query, conn)


def _read_match_events_semantic_scope(conn) -> pd.DataFrame:
    query = text(
        """
        SELECT DISTINCT
            f.fixture_id,
            f.source_provider AS provider,
            f.league_id AS provider_league_id,
            f.competition_key,
            f.season_label,
            f.provider_season_id
        FROM raw.fixtures f
        JOIN control.season_catalog sc
          ON sc.provider = f.source_provider
         AND sc.competition_key = f.competition_key
         AND sc.season_label = f.season_label
         AND sc.provider_season_id = f.provider_season_id
        WHERE f.fixture_id IS NOT NULL
          AND f.source_provider IS NOT NULL
          AND f.league_id IS NOT NULL
          AND f.competition_key IS NOT NULL
          AND f.season_label IS NOT NULL
          AND f.provider_season_id IS NOT NULL
        """
    )
    return pd.read_sql_query(query, conn)


def _enrich_match_events_semantics(
    load_df: pd.DataFrame,
    *,
    scope_df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    enriched = load_df.copy()

    if scope_df.empty:
        sample = enriched.loc[:, ["fixture_id", "event_id"]].drop_duplicates().head(5).to_dict("records")
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de match_events "
            f"via raw.fixtures + control.season_catalog. Fixtures de amostra: {sample}"
        )

    duplicated = scope_df.duplicated(subset=["fixture_id"], keep=False)
    if bool(duplicated.any()):
        ambiguous = scope_df.loc[
            duplicated,
            ["fixture_id", "provider", "provider_league_id", "competition_key", "season_label", "provider_season_id"],
        ]
        raise RuntimeError(
            "Mapeamento semantico ambiguo para match_events. "
            f"Fixtures ambiguos: {ambiguous.drop_duplicates().head(10).to_dict('records')}"
        )

    merge_scope = scope_df.rename(
        columns={
            "provider": "_scope_provider",
            "provider_league_id": "_scope_provider_league_id",
            "competition_key": "_scope_competition_key",
            "season_label": "_scope_season_label",
            "provider_season_id": "_scope_provider_season_id",
        }
    )
    enriched = enriched.merge(merge_scope, how="left", on="fixture_id")
    enriched["provider"] = enriched["_scope_provider"]
    enriched["provider_league_id"] = enriched["_scope_provider_league_id"]
    enriched["competition_key"] = enriched["_scope_competition_key"]
    enriched["season_label"] = enriched["_scope_season_label"]
    enriched["provider_season_id"] = enriched["_scope_provider_season_id"]
    enriched["source_run_id"] = run_id
    enriched = enriched.drop(
        columns=[
            "_scope_provider",
            "_scope_provider_league_id",
            "_scope_competition_key",
            "_scope_season_label",
            "_scope_provider_season_id",
        ]
    )

    unresolved_mask = (
        enriched["provider"].isna()
        | enriched["provider_league_id"].isna()
        | enriched["competition_key"].isna()
        | enriched["season_label"].isna()
        | enriched["provider_season_id"].isna()
    )
    if bool(unresolved_mask.any()):
        sample = enriched.loc[unresolved_mask, ["fixture_id", "event_id"]].drop_duplicates().head(10).to_dict("records")
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de match_events "
            f"para todas as linhas. Fixtures sem match confiavel: {sample}"
        )
    return enriched


def _read_lineups_semantic_scope(conn) -> pd.DataFrame:
    query = text(
        """
        SELECT DISTINCT
            f.fixture_id,
            f.source_provider AS provider,
            f.league_id AS provider_league_id,
            f.competition_key,
            f.season_label,
            f.provider_season_id,
            f.home_team_id,
            f.away_team_id
        FROM raw.fixtures f
        JOIN control.season_catalog sc
          ON sc.provider = f.source_provider
         AND sc.competition_key = f.competition_key
         AND sc.season_label = f.season_label
         AND sc.provider_season_id = f.provider_season_id
        WHERE f.fixture_id IS NOT NULL
          AND f.source_provider IS NOT NULL
          AND f.league_id IS NOT NULL
          AND f.competition_key IS NOT NULL
          AND f.season_label IS NOT NULL
          AND f.provider_season_id IS NOT NULL
          AND f.home_team_id IS NOT NULL
          AND f.away_team_id IS NOT NULL
        """
    )
    return pd.read_sql_query(query, conn)


def _enrich_statistics_semantics(
    load_df: pd.DataFrame,
    *,
    scope_df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    enriched = load_df.copy()

    if scope_df.empty:
        sample = enriched.loc[:, ["fixture_id"]].drop_duplicates().head(5).to_dict("records")
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de statistics "
            f"via raw.fixtures + control.season_catalog. Fixtures de amostra: {sample}"
        )

    duplicated = scope_df.duplicated(subset=["fixture_id"], keep=False)
    if bool(duplicated.any()):
        ambiguous = scope_df.loc[
            duplicated,
            ["fixture_id", "provider", "provider_league_id", "competition_key", "season_label", "provider_season_id"],
        ]
        raise RuntimeError(
            "Mapeamento semantico ambiguo para statistics. "
            f"Fixtures ambiguos: {ambiguous.drop_duplicates().head(10).to_dict('records')}"
        )

    merge_scope = scope_df.rename(
        columns={
            "provider": "_scope_provider",
            "provider_league_id": "_scope_provider_league_id",
            "competition_key": "_scope_competition_key",
            "season_label": "_scope_season_label",
            "provider_season_id": "_scope_provider_season_id",
        }
    )
    enriched = enriched.merge(merge_scope, how="left", on="fixture_id")
    enriched["provider"] = enriched["_scope_provider"]
    enriched["provider_league_id"] = enriched["_scope_provider_league_id"]
    enriched["competition_key"] = enriched["_scope_competition_key"]
    enriched["season_label"] = enriched["_scope_season_label"]
    enriched["provider_season_id"] = enriched["_scope_provider_season_id"]
    enriched["source_run_id"] = run_id
    enriched = enriched.drop(
        columns=[
            "_scope_provider",
            "_scope_provider_league_id",
            "_scope_competition_key",
            "_scope_season_label",
            "_scope_provider_season_id",
        ]
    )

    unresolved_mask = (
        enriched["provider"].isna()
        | enriched["provider_league_id"].isna()
        | enriched["competition_key"].isna()
        | enriched["season_label"].isna()
        | enriched["provider_season_id"].isna()
    )
    if bool(unresolved_mask.any()):
        sample = enriched.loc[unresolved_mask, ["fixture_id", "team_id"]].drop_duplicates().head(10).to_dict("records")
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de statistics "
            f"para todas as linhas. Fixtures sem match confiavel: {sample}"
        )
    return enriched


def _enrich_player_season_statistics_semantics(
    load_df: pd.DataFrame,
    *,
    scope_df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    enriched = load_df.copy()
    enriched["provider_league_id"] = enriched["league_id"]

    join_keys = ["provider", "provider_league_id", "provider_season_id"]
    if scope_df.empty:
        sample = (
            enriched.loc[:, ["provider", "provider_league_id", "season_id", "season_name"]]
            .drop_duplicates()
            .head(5)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de player_season_statistics "
            f"via raw.fixtures + control.season_catalog. Escopos de amostra: {sample}"
        )

    duplicated = scope_df.duplicated(subset=join_keys, keep=False)
    if bool(duplicated.any()):
        ambiguous = scope_df.loc[duplicated, join_keys + ["competition_key", "season_label"]]
        raise RuntimeError(
            "Mapeamento semantico ambiguo para player_season_statistics. "
            f"Escopos ambiguos: {ambiguous.drop_duplicates().to_dict('records')}"
        )

    merge_scope = scope_df.rename(
        columns={
            "provider_league_id": "_scope_provider_league_id",
            "provider_season_id": "_scope_provider_season_id",
            "competition_key": "_scope_competition_key",
            "season_label": "_scope_season_label",
        }
    )
    enriched = enriched.merge(
        merge_scope,
        how="left",
        left_on=["provider", "provider_league_id", "season_id"],
        right_on=["provider", "_scope_provider_league_id", "_scope_provider_season_id"],
    )
    enriched["competition_key"] = enriched["_scope_competition_key"]
    enriched["season_label"] = enriched["_scope_season_label"]
    enriched["provider_season_id"] = enriched["_scope_provider_season_id"]
    enriched["source_run_id"] = run_id
    enriched = enriched.drop(
        columns=[
            "_scope_provider_league_id",
            "_scope_provider_season_id",
            "_scope_competition_key",
            "_scope_season_label",
        ]
    )

    unresolved_mask = (
        enriched["competition_key"].isna()
        | enriched["season_label"].isna()
        | enriched["provider_season_id"].isna()
    )
    if bool(unresolved_mask.any()):
        sample = (
            enriched.loc[unresolved_mask, ["provider", "provider_league_id", "season_id", "season_name"]]
            .drop_duplicates()
            .head(5)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de player_season_statistics "
            f"para todas as linhas. Escopos sem match confiavel: {sample}"
        )
    return enriched


def _prepare_player_season_statistics_load_df(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    scope_df = _read_player_season_statistics_semantic_scope(conn)
    return _enrich_player_season_statistics_semantics(load_df, scope_df=scope_df, run_id=run_id)


def _prepare_competition_leagues_load_df(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    scope_df = _read_catalog_competition_scope(conn)
    return _enrich_competition_leagues_semantics(load_df, scope_df=scope_df, run_id=run_id)


def _prepare_catalog_season_scoped_load_df(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    scope_df = _read_catalog_competition_season_scope(conn)
    return _enrich_catalog_season_semantics(load_df, scope_df=scope_df, run_id=run_id)


def _prepare_statistics_load_df(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    scope_df = _read_statistics_semantic_scope(conn)
    return _enrich_statistics_semantics(load_df, scope_df=scope_df, run_id=run_id)


def _prepare_match_events_load_df(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    scope_df = _read_match_events_semantic_scope(conn)
    return _enrich_match_events_semantics(load_df, scope_df=scope_df, run_id=run_id)


def _enrich_lineups_semantics(
    load_df: pd.DataFrame,
    *,
    scope_df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    enriched = load_df.copy()

    if scope_df.empty:
        sample = enriched.loc[:, ["fixture_id", "team_id", "lineup_id"]].drop_duplicates().head(5).to_dict("records")
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de lineups "
            f"via raw.fixtures + control.season_catalog. Fixtures de amostra: {sample}"
        )

    duplicated = scope_df.duplicated(subset=["fixture_id"], keep=False)
    if bool(duplicated.any()):
        ambiguous = scope_df.loc[
            duplicated,
            ["fixture_id", "provider", "provider_league_id", "competition_key", "season_label", "provider_season_id"],
        ]
        raise RuntimeError(
            "Mapeamento semantico ambiguo para lineups. "
            f"Fixtures ambiguos: {ambiguous.drop_duplicates().head(10).to_dict('records')}"
        )

    merge_scope = scope_df.rename(
        columns={
            "provider": "_scope_provider",
            "provider_league_id": "_scope_provider_league_id",
            "competition_key": "_scope_competition_key",
            "season_label": "_scope_season_label",
            "provider_season_id": "_scope_provider_season_id",
            "home_team_id": "_scope_home_team_id",
            "away_team_id": "_scope_away_team_id",
        }
    )
    enriched = enriched.merge(merge_scope, how="left", on="fixture_id")
    enriched["provider"] = enriched["_scope_provider"]
    enriched["provider_league_id"] = enriched["_scope_provider_league_id"]
    enriched["competition_key"] = enriched["_scope_competition_key"]
    enriched["season_label"] = enriched["_scope_season_label"]
    enriched["provider_season_id"] = enriched["_scope_provider_season_id"]
    enriched["source_run_id"] = run_id
    enriched = enriched.drop(
        columns=[
            "_scope_provider",
            "_scope_provider_league_id",
            "_scope_competition_key",
            "_scope_season_label",
            "_scope_provider_season_id",
        ]
    )

    fixture_team_scope = (
        enriched.loc[:, ["fixture_id", "_scope_home_team_id", "_scope_away_team_id"]]
        .drop_duplicates()
        .copy()
    )
    team_remaps: list[dict[str, int]] = []
    for fixture in fixture_team_scope.to_dict("records"):
        expected_team_ids = {
            int(team_id)
            for team_id in [fixture["_scope_home_team_id"], fixture["_scope_away_team_id"]]
            if pd.notna(team_id)
        }
        observed_team_ids = {
            int(team_id) for team_id in enriched.loc[enriched["fixture_id"] == fixture["fixture_id"], "team_id"].dropna().unique()
        }
        unmatched_observed = observed_team_ids - expected_team_ids
        unmatched_expected = expected_team_ids - observed_team_ids
        if not unmatched_observed:
            continue
        if (
            len(expected_team_ids) == 2
            and len(observed_team_ids) == 2
            and len(unmatched_observed) == 1
            and len(unmatched_expected) == 1
        ):
            team_remaps.append(
                {
                    "fixture_id": int(fixture["fixture_id"]),
                    "_provider_team_id": int(next(iter(unmatched_observed))),
                    "_resolved_team_id": int(next(iter(unmatched_expected))),
                }
            )

    if team_remaps:
        remap_df = pd.DataFrame(team_remaps)
        enriched = enriched.merge(
            remap_df,
            how="left",
            left_on=["fixture_id", "team_id"],
            right_on=["fixture_id", "_provider_team_id"],
        )
        enriched["team_id"] = enriched["_resolved_team_id"].fillna(enriched["team_id"])
        enriched = enriched.drop(columns=["_provider_team_id", "_resolved_team_id"])

    unresolved_team_mask = (
        enriched["team_id"].isna()
        | (
            (enriched["team_id"] != enriched["_scope_home_team_id"])
            & (enriched["team_id"] != enriched["_scope_away_team_id"])
        )
    )
    if bool(unresolved_team_mask.any()):
        sample = (
            enriched.loc[
                unresolved_team_mask,
                ["fixture_id", "team_id", "_scope_home_team_id", "_scope_away_team_id", "lineup_id"],
            ]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de lineups "
            f"para todos os times no fixture autoritativo. Amostra: {sample}"
        )

    enriched = enriched.drop(columns=["_scope_home_team_id", "_scope_away_team_id"])

    unresolved_mask = (
        enriched["provider"].isna()
        | enriched["provider_league_id"].isna()
        | enriched["competition_key"].isna()
        | enriched["season_label"].isna()
        | enriched["provider_season_id"].isna()
    )
    if bool(unresolved_mask.any()):
        sample = (
            enriched.loc[unresolved_mask, ["fixture_id", "team_id", "lineup_id"]]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de lineups "
            f"para todas as linhas. Fixtures sem match confiavel: {sample}"
        )
    return enriched


def _prepare_lineups_load_df(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    scope_df = _read_lineups_semantic_scope(conn)
    return _enrich_lineups_semantics(load_df, scope_df=scope_df, run_id=run_id)


def _enrich_fixture_player_statistics_semantics(
    load_df: pd.DataFrame,
    *,
    scope_df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    enriched = load_df.copy()

    if scope_df.empty:
        sample = enriched.loc[:, ["fixture_id", "team_id", "player_id"]].drop_duplicates().head(5).to_dict("records")
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de fixture_player_statistics "
            f"via raw.fixtures + control.season_catalog. Fixtures de amostra: {sample}"
        )

    duplicated = scope_df.duplicated(subset=["fixture_id"], keep=False)
    if bool(duplicated.any()):
        ambiguous = scope_df.loc[
            duplicated,
            ["fixture_id", "provider", "provider_league_id", "competition_key", "season_label", "provider_season_id"],
        ]
        raise RuntimeError(
            "Mapeamento semantico ambiguo para fixture_player_statistics. "
            f"Fixtures ambiguos: {ambiguous.drop_duplicates().head(10).to_dict('records')}"
        )

    merge_scope = scope_df.rename(
        columns={
            "provider": "_scope_provider",
            "provider_league_id": "_scope_provider_league_id",
            "competition_key": "_scope_competition_key",
            "season_label": "_scope_season_label",
            "provider_season_id": "_scope_provider_season_id",
            "home_team_id": "_scope_home_team_id",
            "away_team_id": "_scope_away_team_id",
        }
    )
    enriched = enriched.merge(
        merge_scope,
        how="left",
        on="fixture_id",
    )
    enriched["provider"] = enriched["provider"].fillna(enriched["_scope_provider"])
    enriched["provider_league_id"] = enriched["_scope_provider_league_id"]
    enriched["competition_key"] = enriched["_scope_competition_key"]
    enriched["season_label"] = enriched["_scope_season_label"]
    enriched["provider_season_id"] = enriched["_scope_provider_season_id"]
    enriched["source_run_id"] = run_id
    enriched = enriched.drop(
        columns=[
            "_scope_provider",
            "_scope_provider_league_id",
            "_scope_competition_key",
            "_scope_season_label",
            "_scope_provider_season_id",
        ]
    )

    unresolved_scope_mask = (
        enriched["_scope_home_team_id"].isna()
        | enriched["_scope_away_team_id"].isna()
        | enriched["provider"].isna()
    )
    if bool(unresolved_scope_mask.any()):
        sample = (
            enriched.loc[unresolved_scope_mask, ["fixture_id", "team_id", "player_id"]]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de fixture_player_statistics "
            f"para todas as linhas. Fixtures sem match confiavel: {sample}"
        )

    fixture_team_scope = (
        enriched.loc[:, ["fixture_id", "_scope_home_team_id", "_scope_away_team_id"]]
        .drop_duplicates()
        .copy()
    )
    team_remaps: list[dict[str, int]] = []
    for fixture in fixture_team_scope.to_dict("records"):
        expected_team_ids = {
            int(team_id)
            for team_id in [fixture["_scope_home_team_id"], fixture["_scope_away_team_id"]]
            if pd.notna(team_id)
        }
        observed_team_ids = {
            int(team_id) for team_id in enriched.loc[enriched["fixture_id"] == fixture["fixture_id"], "team_id"].dropna().unique()
        }
        unmatched_observed = observed_team_ids - expected_team_ids
        unmatched_expected = expected_team_ids - observed_team_ids
        if not unmatched_observed:
            continue
        if (
            len(expected_team_ids) == 2
            and len(observed_team_ids) == 2
            and len(unmatched_observed) == 1
            and len(unmatched_expected) == 1
        ):
            team_remaps.append(
                {
                    "fixture_id": int(fixture["fixture_id"]),
                    "_provider_team_id": int(next(iter(unmatched_observed))),
                    "_resolved_team_id": int(next(iter(unmatched_expected))),
                }
            )

    if team_remaps:
        remap_df = pd.DataFrame(team_remaps)
        enriched = enriched.merge(
            remap_df,
            how="left",
            left_on=["fixture_id", "team_id"],
            right_on=["fixture_id", "_provider_team_id"],
        )
        enriched["team_id"] = enriched["_resolved_team_id"].fillna(enriched["team_id"])
        enriched = enriched.drop(columns=["_provider_team_id", "_resolved_team_id"])

    unresolved_team_mask = (
        enriched["team_id"].isna()
        | (
            (enriched["team_id"] != enriched["_scope_home_team_id"])
            & (enriched["team_id"] != enriched["_scope_away_team_id"])
        )
    )
    if bool(unresolved_team_mask.any()):
        sample = (
            enriched.loc[
                unresolved_team_mask,
                ["fixture_id", "team_id", "player_id", "_scope_home_team_id", "_scope_away_team_id"],
            ]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de fixture_player_statistics "
            f"para todos os times no fixture autoritativo. Amostra: {sample}"
        )

    enriched = enriched.drop(columns=["_scope_home_team_id", "_scope_away_team_id"])
    unresolved_mask = (
        enriched["provider"].isna()
        | enriched["provider_league_id"].isna()
        | enriched["competition_key"].isna()
        | enriched["season_label"].isna()
        | enriched["provider_season_id"].isna()
    )
    if bool(unresolved_mask.any()):
        sample = (
            enriched.loc[unresolved_mask, ["fixture_id", "team_id", "player_id"]]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de fixture_player_statistics "
            f"para todas as linhas. Fixtures sem match confiavel: {sample}"
        )

    return enriched


def _prepare_fixture_player_statistics_load_df(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    scope_df = _read_lineups_semantic_scope(conn)
    return _enrich_fixture_player_statistics_semantics(load_df, scope_df=scope_df, run_id=run_id)


def _read_head_to_head_scope_fixtures(conn, *, league_id: int, season: int) -> pd.DataFrame:
    query = text(
        """
        SELECT DISTINCT
            f.fixture_id,
            f.source_provider AS provider,
            f.league_id AS provider_league_id,
            f.competition_key,
            f.season_label,
            f.provider_season_id,
            f.date_utc::date AS fixture_date,
            LEAST(f.home_team_id, f.away_team_id) AS scope_pair_team_id,
            GREATEST(f.home_team_id, f.away_team_id) AS scope_pair_opponent_id
        FROM raw.fixtures f
        JOIN control.season_catalog sc
          ON sc.provider = f.source_provider
         AND sc.competition_key = f.competition_key
         AND sc.season_label = f.season_label
         AND sc.provider_season_id = f.provider_season_id
        WHERE f.league_id = :league_id
          AND f.season = :season
          AND f.fixture_id IS NOT NULL
          AND f.source_provider IS NOT NULL
          AND f.competition_key IS NOT NULL
          AND f.season_label IS NOT NULL
          AND f.provider_season_id IS NOT NULL
          AND f.home_team_id IS NOT NULL
          AND f.away_team_id IS NOT NULL
        """
    )
    return pd.read_sql_query(query, conn, params={"league_id": league_id, "season": season})


def _enrich_head_to_head_semantics(
    load_df: pd.DataFrame,
    *,
    scope_df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    enriched = load_df.copy()

    if scope_df.empty:
        sample = (
            enriched.loc[:, ["pair_team_id", "pair_opponent_id", "fixture_id"]]
            .drop_duplicates()
            .head(5)
            .to_dict("records")
        )
        raise RuntimeError(
            "Nao foi possivel resolver identidade semantica de head_to_head "
            f"via raw.fixtures + control.season_catalog. Linhas de amostra: {sample}"
        )

    duplicated = scope_df.duplicated(subset=["fixture_id"], keep=False)
    if bool(duplicated.any()):
        ambiguous = scope_df.loc[
            duplicated,
            [
                "fixture_id",
                "provider",
                "provider_league_id",
                "competition_key",
                "season_label",
                "provider_season_id",
            ],
        ]
        raise RuntimeError(
            "Mapeamento semantico ambiguo para head_to_head. "
            f"Fixtures ambiguos: {ambiguous.drop_duplicates().head(10).to_dict('records')}"
        )

    merge_scope = scope_df.rename(
        columns={
            "provider": "_scope_provider",
            "provider_league_id": "_scope_provider_league_id",
            "competition_key": "_scope_competition_key",
            "season_label": "_scope_season_label",
            "provider_season_id": "_scope_provider_season_id",
            "fixture_date": "_scope_fixture_date",
            "scope_pair_team_id": "_scope_pair_team_id",
            "scope_pair_opponent_id": "_scope_pair_opponent_id",
        }
    )
    enriched = enriched.merge(merge_scope, how="left", on="fixture_id")
    enriched["provider"] = enriched["_scope_provider"]
    enriched["provider_league_id"] = enriched["_scope_provider_league_id"]
    enriched["competition_key"] = enriched["_scope_competition_key"]
    enriched["season_label"] = enriched["_scope_season_label"]
    enriched["provider_season_id"] = enriched["_scope_provider_season_id"]
    enriched["source_run_id"] = run_id

    match_date_series = pd.to_datetime(enriched["match_date"], errors="coerce", utc=True)
    match_date_values = match_date_series.dt.date
    scope_date_values = pd.to_datetime(enriched["_scope_fixture_date"], errors="coerce").dt.date

    unresolved_mask = (
        enriched["provider"].isna()
        | enriched["provider_league_id"].isna()
        | enriched["competition_key"].isna()
        | enriched["season_label"].isna()
        | enriched["provider_season_id"].isna()
    )
    if bool(unresolved_mask.any()):
        # H2H can occasionally return fixture ids that the provider still tags with the
        # requested league/season but that do not exist in the authoritative fixture scope.
        # Drop only those rows and keep the guardrail for any remaining unresolved data.
        filtered = enriched.loc[~unresolved_mask].copy()
        if not filtered.empty:
            enriched = filtered
            match_date_series = pd.to_datetime(enriched["match_date"], errors="coerce", utc=True)
            match_date_values = match_date_series.dt.date
            scope_date_values = pd.to_datetime(enriched["_scope_fixture_date"], errors="coerce").dt.date
            unresolved_mask = (
                enriched["provider"].isna()
                | enriched["provider_league_id"].isna()
                | enriched["competition_key"].isna()
                | enriched["season_label"].isna()
                | enriched["provider_season_id"].isna()
            )
        if bool(unresolved_mask.any()):
            sample = (
                enriched.loc[unresolved_mask, ["pair_team_id", "pair_opponent_id", "fixture_id"]]
                .drop_duplicates()
                .head(10)
                .to_dict("records")
            )
            raise RuntimeError(
                "Nao foi possivel resolver identidade semantica de head_to_head "
                f"para todas as linhas. Fixtures sem match confiavel: {sample}"
            )

    mismatch_mask = (
        (enriched["pair_team_id"] != enriched["_scope_pair_team_id"])
        | (enriched["pair_opponent_id"] != enriched["_scope_pair_opponent_id"])
        | (enriched["league_id"] != enriched["provider_league_id"])
        | (enriched["season_id"] != enriched["provider_season_id"])
        | (match_date_values != scope_date_values)
    )
    if bool(mismatch_mask.any()):
        sample = (
            enriched.loc[
                mismatch_mask,
                [
                    "pair_team_id",
                    "pair_opponent_id",
                    "fixture_id",
                    "league_id",
                    "season_id",
                    "match_date",
                    "_scope_pair_team_id",
                    "_scope_pair_opponent_id",
                    "provider_league_id",
                    "provider_season_id",
                    "_scope_fixture_date",
                ],
            ]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        raise RuntimeError(
            "Head_to_head fora do escopo autoritativo de raw.fixtures. "
            f"Linhas de amostra: {sample}"
        )

    return enriched.drop(
        columns=[
            "_scope_provider",
            "_scope_provider_league_id",
            "_scope_competition_key",
            "_scope_season_label",
            "_scope_provider_season_id",
            "_scope_fixture_date",
            "_scope_pair_team_id",
            "_scope_pair_opponent_id",
        ]
    )


def _prepare_head_to_head_load_df(
    conn,
    load_df: pd.DataFrame,
    run_id: str,
    *,
    league_id: int,
    season: int,
) -> pd.DataFrame:
    scope_df = _read_head_to_head_scope_fixtures(conn, league_id=league_id, season=season)
    return _enrich_head_to_head_semantics(load_df, scope_df=scope_df, run_id=run_id)


def _stage_and_upsert_with_classified_counts(
    conn,
    *,
    target_table: str,
    load_df: pd.DataFrame,
    target_columns: list[str],
    conflict_keys: list[str],
    compare_columns: list[str],
) -> tuple[int, int, int]:
    staging_table = f"staging_{target_table}"
    distinct_predicate = " OR ".join([f"t.{col} IS DISTINCT FROM s.{col}" for col in compare_columns]) or "FALSE"
    join_predicate = " AND ".join([f"t.{col} = s.{col}" for col in conflict_keys])
    first_key = conflict_keys[0]
    insert_cols = ", ".join(target_columns)
    staged_select_cols = ", ".join([f"c.{col}" for col in target_columns])
    update_columns = list(compare_columns)
    if "ingested_run" in target_columns and "ingested_run" not in conflict_keys:
        update_columns.append("ingested_run")
    update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns] + ["updated_at = now()"])
    conflict_where = (
        " OR ".join([f"raw.{target_table}.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in compare_columns]) or "FALSE"
    )
    action_sql = text(
        f"""
        WITH classified AS MATERIALIZED (
            SELECT
                {", ".join([f"s.{col}" for col in target_columns])},
                CASE
                    WHEN t.{first_key} IS NULL THEN 'inserted'
                    WHEN {distinct_predicate} THEN 'updated'
                    ELSE 'ignored'
                END AS row_action
            FROM {staging_table} s
            LEFT JOIN raw.{target_table} t
              ON {join_predicate}
        ),
        upserted AS (
            INSERT INTO raw.{target_table} ({insert_cols})
            SELECT {staged_select_cols}
            FROM classified c
            WHERE c.row_action IN ('inserted', 'updated')
            ON CONFLICT ({", ".join(conflict_keys)}) DO UPDATE
            SET {update_set}
            WHERE {conflict_where}
            RETURNING 1
        )
        SELECT
            COUNT(*) FILTER (WHERE row_action = 'inserted')::bigint AS inserted,
            COUNT(*) FILTER (WHERE row_action = 'updated')::bigint AS updated,
            COUNT(*) FILTER (WHERE row_action = 'ignored')::bigint AS ignored,
            (SELECT COUNT(*)::bigint FROM upserted) AS changed_rows
        FROM classified
        """
    )

    conn.execute(text(f"CREATE TEMP TABLE {staging_table} (LIKE raw.{target_table} INCLUDING DEFAULTS) ON COMMIT DROP"))
    load_df.to_sql(staging_table, con=conn, if_exists="append", index=False, method="multi")
    counts = conn.execute(action_sql).mappings().one()
    inserted = int(counts["inserted"] or 0)
    updated = int(counts["updated"] or 0)
    ignored = int(counts["ignored"] or 0)
    changed_rows = int(counts["changed_rows"] or 0)
    if inserted + updated != changed_rows:
        raise RuntimeError(
            f"Contagem classificada divergente no upsert de raw.{target_table}: "
            f"inserted={inserted} updated={updated} changed_rows={changed_rows}"
        )
    return inserted, updated, ignored


def _load_generic_silver_to_raw(
    *,
    context,
    league_id: int,
    season: int,
    dataset: str,
    prefix: str,
    suffix: str,
    target_table: str,
    target_columns: list[str],
    required_columns: list[str],
    conflict_keys: list[str],
    int_columns: list[str] | None = None,
    bool_columns: list[str] | None = None,
    date_columns: list[str] | None = None,
    datetime_columns: list[str] | None = None,
    text_columns: list[str] | None = None,
    json_columns: list[str] | None = None,
    prepare_load_df: Callable[..., pd.DataFrame] | None = None,
) -> None:
    int_columns = int_columns or []
    bool_columns = bool_columns or []
    date_columns = date_columns or []
    datetime_columns = datetime_columns or []
    text_columns = text_columns or []
    json_columns = json_columns or []

    s3_client = _s3_client()
    engine = create_engine(_get_required_env("FOOTBALL_PG_DSN"))
    run_id, run_keys = _read_latest_parquet_run(s3_client, prefix=prefix, suffix=suffix)

    read_rows = 0
    frames = []
    with StepMetrics(
        service="airflow",
        module="warehouse_service",
        step=f"load_{dataset}_silver_to_raw",
        context=context,
        dataset=f"raw.{target_table}",
        table=f"raw.{target_table}",
    ) as metric:
        for key in run_keys:
            obj = s3_client.get_object(Bucket=SILVER_BUCKET, Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()))
            _assert_columns(df, required_columns, key)
            read_rows += len(df)
            frames.append(df)

        load_df = pd.concat(frames, ignore_index=True)

        for col in target_columns:
            if col not in load_df.columns and col != "ingested_run":
                load_df[col] = pd.NA

        for col in int_columns:
            if col in load_df.columns:
                load_df[col] = pd.to_numeric(load_df[col], errors="coerce").astype("Int64")
        for col in bool_columns:
            if col in load_df.columns:
                load_df[col] = _coerce_bool(load_df[col])
        for col in date_columns:
            if col in load_df.columns:
                load_df[col] = pd.to_datetime(load_df[col], errors="coerce").dt.date
        for col in datetime_columns:
            if col in load_df.columns:
                load_df[col] = pd.to_datetime(load_df[col], errors="coerce", utc=True)
        for col in text_columns:
            if col in load_df.columns:
                load_df[col] = load_df[col].astype("string")
        for col in json_columns:
            if col in load_df.columns:
                load_df[col] = load_df[col].map(_to_json_text)

        invalid_mask = pd.Series(False, index=load_df.index)
        for key_col in conflict_keys:
            invalid_mask = invalid_mask | load_df[key_col].isna()
        if int(invalid_mask.sum()):
            load_df = load_df[~invalid_mask].copy()

        load_df = load_df.drop_duplicates(subset=conflict_keys, keep="last").copy()
        load_df["ingested_run"] = run_id
        load_df = load_df[target_columns]

        compare_columns = [
            col
            for col in target_columns
            if col not in conflict_keys and col not in {"ingested_run", "created_at", "updated_at"}
        ]

        with engine.begin() as conn:
            _assert_target_columns(conn, schema="raw", table=target_table, expected=target_columns)
            if prepare_load_df is not None:
                load_df = prepare_load_df(conn, load_df, run_id)
            inserted, updated, ignored = _stage_and_upsert_with_classified_counts(
                conn,
                target_table=target_table,
                load_df=load_df,
                target_columns=target_columns,
                conflict_keys=conflict_keys,
                compare_columns=compare_columns,
            )

        metric.set_counts(rows_in=read_rows, rows_out=len(load_df), row_count=len(load_df))

    log_event(
        service="airflow",
        module="warehouse_service",
        step="summary",
        status="success",
        context=context,
        dataset=f"raw.{target_table}",
        rows_in=read_rows,
        rows_out=len(load_df),
        row_count=len(load_df),
        message=(
            f"Load {dataset} concluido | league_id={league_id} | season={season} | run={run_id} "
            f"| inseridas={inserted} | atualizadas={updated} | ignoradas={ignored}"
        ),
    )


def load_fixtures_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]

    s3_client = _s3_client()
    engine = create_engine(_get_required_env("FOOTBALL_PG_DSN"))

    prefix = f"fixtures/league={league_id}/season={season}/"
    keys = _list_all_keys(s3_client, bucket=SILVER_BUCKET, prefix=prefix)
    parquet_keys = [key for key in keys if key.endswith("fixtures.parquet")]
    if not parquet_keys:
        raise RuntimeError(f"Nenhum fixtures.parquet encontrado com prefixo {prefix}")

    run_id = _latest_run(parquet_keys)
    run_keys = sorted([key for key in parquet_keys if f"/run={run_id}/" in key])

    read_rows = 0
    frames = []
    with StepMetrics(
        service="airflow",
        module="warehouse_service",
        step="load_fixtures_silver_to_raw",
        context=context,
        dataset="raw.fixtures",
        table="raw.fixtures",
    ) as metric:
        for key in run_keys:
            obj = s3_client.get_object(Bucket=SILVER_BUCKET, Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()))
            _assert_columns(df, FIXTURES_REQUIRED_COLUMNS, key)
            read_rows += len(df)
            frames.append(df)

        load_df = pd.concat(frames, ignore_index=True)
        for col in FIXTURES_TARGET_COLUMNS:
            if col not in load_df.columns and col != "ingested_run":
                load_df[col] = pd.NA
        load_df["fixture_id"] = pd.to_numeric(load_df["fixture_id"], errors="coerce").astype("Int64")
        for col in [
            "timestamp",
            "referee_id",
            "venue_id",
            "league_id",
            "season",
            "stage_id",
            "round_id",
            "attendance",
            "home_team_id",
            "away_team_id",
            "home_goals",
            "away_goals",
            "home_goals_ht",
            "away_goals_ht",
            "home_goals_ft",
            "away_goals_ft",
            "provider_season_id",
        ]:
            if col in load_df.columns:
                load_df[col] = pd.to_numeric(load_df[col], errors="coerce").astype("Int64")
        for col in ["weather_temperature_c", "weather_wind_kph"]:
            if col in load_df.columns:
                load_df[col] = pd.to_numeric(load_df[col], errors="coerce")
        load_df["date_utc"] = pd.to_datetime(load_df["date_utc"], errors="coerce", utc=True)
        load_df["source_provider"] = load_df["source_provider"].astype("string")
        for col in ["competition_key", "competition_type", "season_label", "season_name", "source_run_id"]:
            if col in load_df.columns:
                load_df[col] = load_df[col].astype("string")
        for col in ["season_start_date", "season_end_date"]:
            if col in load_df.columns:
                load_df[col] = pd.to_datetime(load_df[col], errors="coerce").dt.date
        load_df["year"] = load_df["year"].astype("string")
        load_df["month"] = load_df["month"].astype("string")

        invalid_mask = load_df["fixture_id"].isna()
        invalid_rows = int(invalid_mask.sum())
        if invalid_rows:
            load_df = load_df[~invalid_mask].copy()
        load_df = load_df.drop_duplicates(subset=["fixture_id"], keep="last").copy()

        load_df["ingested_run"] = run_id
        load_df = load_df[FIXTURES_TARGET_COLUMNS]

        compare_columns = [
            col
            for col in FIXTURES_TARGET_COLUMNS
            if col not in {"fixture_id", "ingested_run", "created_at", "updated_at"}
        ]
        distinct_predicate = " OR ".join([f"t.{col} IS DISTINCT FROM s.{col}" for col in compare_columns])
        insert_cols = ", ".join(FIXTURES_TARGET_COLUMNS)
        select_cols = ", ".join([f"s.{col}" for col in FIXTURES_TARGET_COLUMNS])
        update_columns = list(compare_columns) + ["ingested_run"]
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        conflict_where = " OR ".join([f"raw.fixtures.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in compare_columns])

        with engine.begin() as conn:
            _assert_target_columns(conn, schema="raw", table="fixtures", expected=FIXTURES_TARGET_COLUMNS)
            scope_df = _read_fixture_semantic_scope(conn)
            load_df = _enrich_fixtures_semantics(load_df, scope_df=scope_df, run_id=run_id)
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
        module="warehouse_service",
        step="summary",
        status="success",
        context=context,
        dataset="raw.fixtures",
        rows_in=read_rows,
        rows_out=len(load_df),
        row_count=len(load_df),
        message=(
            f"Load fixtures concluido | league_id={league_id} | season={season} | run={run_id} "
            f"| inseridas={inserted} | atualizadas={updated} | ignoradas={ignored}"
        ),
    )


def load_statistics_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]

    s3_client = _s3_client()
    engine = create_engine(_get_required_env("FOOTBALL_PG_DSN"))

    prefix = f"statistics/league={league_id}/season={season}/"
    keys = _list_all_keys(s3_client, bucket=SILVER_BUCKET, prefix=prefix)
    parquet_keys = [key for key in keys if key.endswith("statistics.parquet")]
    if not parquet_keys:
        raise RuntimeError(f"Nenhum statistics.parquet encontrado com prefixo {prefix}")

    run_id = _latest_run(parquet_keys)
    run_keys = sorted([key for key in parquet_keys if f"/run={run_id}/" in key])

    read_rows = 0
    frames = []
    with StepMetrics(
        service="airflow",
        module="warehouse_service",
        step="load_statistics_silver_to_raw",
        context=context,
        dataset="raw.match_statistics",
        table="raw.match_statistics",
    ) as metric:
        for key in run_keys:
            obj = s3_client.get_object(Bucket=SILVER_BUCKET, Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()))
            _assert_columns(df, STATISTICS_REQUIRED_COLUMNS, key)
            read_rows += len(df)
            frames.append(df)

        load_df = pd.concat(frames, ignore_index=True)
        for col in STATISTICS_TARGET_COLUMNS:
            if col not in load_df.columns and col != "ingested_run":
                load_df[col] = pd.NA
        for col in STATISTICS_INT_COLUMNS:
            load_df[col] = pd.to_numeric(load_df[col], errors="coerce").astype("Int64")
        for col in STATISTICS_TEXT_COLUMNS:
            if col in load_df.columns:
                load_df[col] = load_df[col].astype("string")
        load_df["passes_pct"] = pd.to_numeric(load_df["passes_pct"], errors="coerce")

        invalid_mask = load_df["fixture_id"].isna() | load_df["team_id"].isna()
        if int(invalid_mask.sum()):
            load_df = load_df[~invalid_mask].copy()
        load_df = load_df.drop_duplicates(subset=["fixture_id", "team_id"], keep="last").copy()
        load_df["ingested_run"] = run_id

        compare_columns = [
            col
            for col in STATISTICS_TARGET_COLUMNS
            if col not in {"fixture_id", "team_id", "ingested_run", "created_at", "updated_at"}
        ]
        distinct_predicate = " OR ".join([f"t.{col} IS DISTINCT FROM s.{col}" for col in compare_columns])
        insert_cols = ", ".join(STATISTICS_TARGET_COLUMNS)
        select_cols = ", ".join([f"s.{col}" for col in STATISTICS_TARGET_COLUMNS])
        update_columns = list(compare_columns) + ["ingested_run"]
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns] + ["updated_at = now()"])
        conflict_where = " OR ".join([f"raw.match_statistics.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in compare_columns])

        with engine.begin() as conn:
            _assert_target_columns(conn, schema="raw", table="match_statistics", expected=STATISTICS_TARGET_COLUMNS)
            load_df = _prepare_statistics_load_df(conn, load_df, run_id)
            load_df = load_df[STATISTICS_TARGET_COLUMNS]
            conn.execute(text("CREATE TEMP TABLE staging_statistics (LIKE raw.match_statistics INCLUDING DEFAULTS) ON COMMIT DROP"))
            load_df.to_sql("staging_statistics", con=conn, if_exists="append", index=False, method="multi")

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

        metric.set_counts(rows_in=read_rows, rows_out=len(load_df), row_count=len(load_df))

    log_event(
        service="airflow",
        module="warehouse_service",
        step="summary",
        status="success",
        context=context,
        dataset="raw.match_statistics",
        rows_in=read_rows,
        rows_out=len(load_df),
        row_count=len(load_df),
        message=(
            f"Load statistics concluido | league_id={league_id} | season={season} | run={run_id} "
            f"| inseridas={inserted} | atualizadas={updated} | ignoradas={ignored}"
        ),
    )


def load_match_events_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]

    s3_client = _s3_client()
    engine = create_engine(_get_required_env("FOOTBALL_PG_DSN"))

    prefix = f"events/season={season}/league_id={league_id}/"
    keys = _list_all_keys(s3_client, bucket=SILVER_BUCKET, prefix=prefix)
    parquet_keys = [key for key in keys if key.endswith("match_events.parquet")]
    if not parquet_keys:
        raise RuntimeError(f"Nenhum match_events.parquet encontrado com prefixo {prefix}")

    run_id = _latest_run(parquet_keys)
    run_keys = sorted([key for key in parquet_keys if f"/run={run_id}/" in key])

    read_rows = 0
    frames = []
    with StepMetrics(
        service="airflow",
        module="warehouse_service",
        step="load_match_events_silver_to_raw",
        context=context,
        dataset="raw.match_events",
        table="raw.match_events",
    ) as metric:
        for key in run_keys:
            obj = s3_client.get_object(Bucket=SILVER_BUCKET, Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()))
            _assert_columns(df, EVENTS_REQUIRED_COLUMNS, key)
            read_rows += len(df)
            frames.append(df)

        load_df = pd.concat(frames, ignore_index=True)
        if "season" not in load_df.columns:
            load_df["season"] = season
        for col in EVENTS_TARGET_COLUMNS:
            if col not in load_df.columns and col not in {"ingested_run", "season"}:
                load_df[col] = pd.NA
        for col in EVENTS_INT_COLUMNS:
            load_df[col] = pd.to_numeric(load_df[col], errors="coerce").astype("Int64")
        for col in EVENTS_BOOL_COLUMNS:
            if col not in load_df.columns:
                load_df[col] = False
            load_df[col] = load_df[col].fillna(False).astype(bool)
        for col in EVENTS_TEXT_COLUMNS:
            load_df[col] = load_df[col].astype("string")

        invalid_mask = load_df["event_id"].isna() | load_df["fixture_id"].isna() | load_df["season"].isna()
        if int(invalid_mask.sum()):
            load_df = load_df[~invalid_mask].copy()
        load_df = load_df.drop_duplicates(subset=["event_id", "season"], keep="last").copy()
        load_df["ingested_run"] = run_id
        load_df = load_df[EVENTS_TARGET_COLUMNS]

        key_columns = ["provider", "season", "fixture_id", "event_id"]
        compare_columns = [
            col
            for col in EVENTS_TARGET_COLUMNS
            if col not in {*key_columns, "ingested_run", "created_at", "updated_at"}
        ]
        distinct_predicate = " OR ".join([f"t.{col} IS DISTINCT FROM s.{col}" for col in compare_columns])
        insert_cols = ", ".join(EVENTS_TARGET_COLUMNS)
        select_cols = ", ".join([f"s.{col}" for col in EVENTS_TARGET_COLUMNS])
        update_columns = list(compare_columns) + ["ingested_run"]
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns] + ["updated_at = now()"])
        conflict_where = " OR ".join([f"raw.match_events.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in compare_columns])

        with engine.begin() as conn:
            _assert_target_columns(conn, schema="raw", table="match_events", expected=EVENTS_TARGET_COLUMNS)
            load_df = _prepare_match_events_load_df(conn, load_df, run_id)
            conn.execute(text("CREATE TEMP TABLE staging_match_events (LIKE raw.match_events INCLUDING DEFAULTS) ON COMMIT DROP"))
            load_df = load_df[EVENTS_TARGET_COLUMNS]
            load_df.to_sql("staging_match_events", con=conn, if_exists="append", index=False, method="multi")

            inserted = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM staging_match_events s
                    LEFT JOIN raw.match_events t
                      ON t.provider = s.provider
                      AND t.season = s.season
                      AND t.fixture_id = s.fixture_id
                      AND t.event_id = s.event_id
                    WHERE t.event_id IS NULL
                    """
                )
            ).scalar_one()
            updated = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM staging_match_events s
                    JOIN raw.match_events t
                      ON t.provider = s.provider
                      AND t.season = s.season
                      AND t.fixture_id = s.fixture_id
                      AND t.event_id = s.event_id
                    WHERE {distinct_predicate}
                    """
                )
            ).scalar_one()
            conn.execute(
                text(
                    f"""
                    INSERT INTO raw.match_events ({insert_cols})
                    SELECT {select_cols}
                    FROM staging_match_events s
                    ON CONFLICT (provider, season, fixture_id, event_id) DO UPDATE
                    SET {update_set}
                    WHERE {conflict_where}
                    """
                )
            )
            ignored = len(load_df) - inserted - updated

        metric.set_counts(rows_in=read_rows, rows_out=len(load_df), row_count=len(load_df))

    log_event(
        service="airflow",
        module="warehouse_service",
        step="summary",
        status="success",
        context=context,
        dataset="raw.match_events",
        rows_in=read_rows,
        rows_out=len(load_df),
        row_count=len(load_df),
        message=(
            f"Load match_events concluido | league_id={league_id} | season={season} | run={run_id} "
            f"| inseridas={inserted} | atualizadas={updated} | ignoradas={ignored}"
        ),
    )


def load_competition_structure_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]

    base_prefix = f"competition_structure/league={league_id}/season={season}/"
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="competition_leagues",
        prefix=base_prefix,
        suffix="competition_leagues.parquet",
        target_table="competition_leagues",
        target_columns=COMPETITION_LEAGUES_TARGET_COLUMNS,
        required_columns=COMPETITION_LEAGUES_REQUIRED_COLUMNS,
        conflict_keys=COMPETITION_LEAGUES_CONFLICT_KEYS,
        int_columns=COMPETITION_LEAGUES_INT_COLUMNS,
        text_columns=COMPETITION_LEAGUES_TEXT_COLUMNS,
        json_columns=COMPETITION_LEAGUES_JSON_COLUMNS,
        prepare_load_df=_prepare_competition_leagues_load_df,
    )
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="competition_seasons",
        prefix=base_prefix,
        suffix="competition_seasons.parquet",
        target_table="competition_seasons",
        target_columns=COMPETITION_SEASONS_TARGET_COLUMNS,
        required_columns=COMPETITION_SEASONS_REQUIRED_COLUMNS,
        conflict_keys=COMPETITION_SEASONS_CONFLICT_KEYS,
        int_columns=COMPETITION_SEASONS_INT_COLUMNS,
        date_columns=COMPETITION_SEASONS_DATE_COLUMNS,
        text_columns=COMPETITION_SEASONS_TEXT_COLUMNS,
        json_columns=COMPETITION_SEASONS_JSON_COLUMNS,
        prepare_load_df=_prepare_catalog_season_scoped_load_df,
    )
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="competition_stages",
        prefix=base_prefix,
        suffix="competition_stages.parquet",
        target_table="competition_stages",
        target_columns=COMPETITION_STAGES_TARGET_COLUMNS,
        required_columns=COMPETITION_STAGES_REQUIRED_COLUMNS,
        conflict_keys=COMPETITION_STAGES_CONFLICT_KEYS,
        int_columns=COMPETITION_STAGES_INT_COLUMNS,
        bool_columns=COMPETITION_STAGES_BOOL_COLUMNS,
        date_columns=COMPETITION_STAGES_DATE_COLUMNS,
        text_columns=COMPETITION_STAGES_TEXT_COLUMNS,
        json_columns=COMPETITION_STAGES_JSON_COLUMNS,
        prepare_load_df=_prepare_catalog_season_scoped_load_df,
    )
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="competition_rounds",
        prefix=base_prefix,
        suffix="competition_rounds.parquet",
        target_table="competition_rounds",
        target_columns=COMPETITION_ROUNDS_TARGET_COLUMNS,
        required_columns=COMPETITION_ROUNDS_REQUIRED_COLUMNS,
        conflict_keys=COMPETITION_ROUNDS_CONFLICT_KEYS,
        int_columns=COMPETITION_ROUNDS_INT_COLUMNS,
        bool_columns=COMPETITION_ROUNDS_BOOL_COLUMNS,
        date_columns=COMPETITION_ROUNDS_DATE_COLUMNS,
        text_columns=COMPETITION_ROUNDS_TEXT_COLUMNS,
        json_columns=COMPETITION_ROUNDS_JSON_COLUMNS,
        prepare_load_df=_prepare_catalog_season_scoped_load_df,
    )


def load_standings_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="standings_snapshots",
        prefix=f"standings/league={league_id}/season={season}/",
        suffix="standings_snapshots.parquet",
        target_table="standings_snapshots",
        target_columns=STANDINGS_SNAPSHOTS_TARGET_COLUMNS,
        required_columns=STANDINGS_SNAPSHOTS_REQUIRED_COLUMNS,
        conflict_keys=STANDINGS_SNAPSHOTS_CONFLICT_KEYS,
        int_columns=STANDINGS_SNAPSHOTS_INT_COLUMNS,
        text_columns=STANDINGS_SNAPSHOTS_TEXT_COLUMNS,
        json_columns=STANDINGS_SNAPSHOTS_JSON_COLUMNS,
        prepare_load_df=_prepare_catalog_season_scoped_load_df,
    )


def load_lineups_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="fixture_lineups",
        prefix=f"lineups/league={league_id}/season={season}/",
        suffix="fixture_lineups.parquet",
        target_table="fixture_lineups",
        target_columns=FIXTURE_LINEUPS_TARGET_COLUMNS,
        required_columns=FIXTURE_LINEUPS_REQUIRED_COLUMNS,
        conflict_keys=FIXTURE_LINEUPS_CONFLICT_KEYS,
        int_columns=FIXTURE_LINEUPS_INT_COLUMNS,
        text_columns=FIXTURE_LINEUPS_TEXT_COLUMNS,
        json_columns=FIXTURE_LINEUPS_JSON_COLUMNS,
        prepare_load_df=_prepare_lineups_load_df,
    )


def load_fixture_player_statistics_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="fixture_player_statistics",
        prefix=f"fixture_player_statistics/league={league_id}/season={season}/",
        suffix="fixture_player_statistics.parquet",
        target_table="fixture_player_statistics",
        target_columns=FIXTURE_PLAYER_STATISTICS_TARGET_COLUMNS,
        required_columns=FIXTURE_PLAYER_STATISTICS_REQUIRED_COLUMNS,
        conflict_keys=FIXTURE_PLAYER_STATISTICS_CONFLICT_KEYS,
        int_columns=FIXTURE_PLAYER_STATISTICS_INT_COLUMNS,
        text_columns=FIXTURE_PLAYER_STATISTICS_TEXT_COLUMNS,
        json_columns=FIXTURE_PLAYER_STATISTICS_JSON_COLUMNS,
        prepare_load_df=_prepare_fixture_player_statistics_load_df,
    )


def load_player_season_statistics_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="player_season_statistics",
        prefix=f"player_season_statistics/league={league_id}/season={season}/",
        suffix="player_season_statistics.parquet",
        target_table="player_season_statistics",
        target_columns=PLAYER_SEASON_STATISTICS_TARGET_COLUMNS,
        required_columns=PLAYER_SEASON_STATISTICS_REQUIRED_COLUMNS,
        conflict_keys=PLAYER_SEASON_STATISTICS_CONFLICT_KEYS,
        int_columns=PLAYER_SEASON_STATISTICS_INT_COLUMNS,
        text_columns=PLAYER_SEASON_STATISTICS_TEXT_COLUMNS,
        json_columns=PLAYER_SEASON_STATISTICS_JSON_COLUMNS,
        prepare_load_df=_prepare_player_season_statistics_load_df,
    )


def load_player_transfers_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="player_transfers",
        prefix=f"player_transfers/league={league_id}/season={season}/",
        suffix="player_transfers.parquet",
        target_table="player_transfers",
        target_columns=PLAYER_TRANSFERS_TARGET_COLUMNS,
        required_columns=PLAYER_TRANSFERS_REQUIRED_COLUMNS,
        conflict_keys=PLAYER_TRANSFERS_CONFLICT_KEYS,
        int_columns=PLAYER_TRANSFERS_INT_COLUMNS,
        bool_columns=PLAYER_TRANSFERS_BOOL_COLUMNS,
        date_columns=PLAYER_TRANSFERS_DATE_COLUMNS,
        text_columns=PLAYER_TRANSFERS_TEXT_COLUMNS,
        json_columns=PLAYER_TRANSFERS_JSON_COLUMNS,
    )


def load_team_sidelined_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="team_sidelined",
        prefix=f"team_sidelined/league={league_id}/season={season}/",
        suffix="team_sidelined.parquet",
        target_table="team_sidelined",
        target_columns=TEAM_SIDELINED_TARGET_COLUMNS,
        required_columns=TEAM_SIDELINED_REQUIRED_COLUMNS,
        conflict_keys=TEAM_SIDELINED_CONFLICT_KEYS,
        int_columns=TEAM_SIDELINED_INT_COLUMNS,
        bool_columns=TEAM_SIDELINED_BOOL_COLUMNS,
        date_columns=TEAM_SIDELINED_DATE_COLUMNS,
        text_columns=TEAM_SIDELINED_TEXT_COLUMNS,
        json_columns=TEAM_SIDELINED_JSON_COLUMNS,
    )


def load_team_coaches_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="team_coaches",
        prefix=f"team_coaches/league={league_id}/season={season}/",
        suffix="team_coaches.parquet",
        target_table="team_coaches",
        target_columns=TEAM_COACHES_TARGET_COLUMNS,
        required_columns=TEAM_COACHES_REQUIRED_COLUMNS,
        conflict_keys=TEAM_COACHES_CONFLICT_KEYS,
        int_columns=TEAM_COACHES_INT_COLUMNS,
        bool_columns=TEAM_COACHES_BOOL_COLUMNS,
        date_columns=TEAM_COACHES_DATE_COLUMNS,
        text_columns=TEAM_COACHES_TEXT_COLUMNS,
        json_columns=TEAM_COACHES_JSON_COLUMNS,
    )


def load_coaches_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="coaches",
        prefix=f"coaches/league={league_id}/season={season}/",
        suffix="coaches.parquet",
        target_table="coaches",
        target_columns=COACHES_TARGET_COLUMNS,
        required_columns=COACHES_REQUIRED_COLUMNS,
        conflict_keys=COACHES_CONFLICT_KEYS,
        int_columns=COACHES_INT_COLUMNS,
        text_columns=COACHES_TEXT_COLUMNS,
        json_columns=COACHES_JSON_COLUMNS,
    )


def load_head_to_head_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    _load_generic_silver_to_raw(
        context=context,
        league_id=league_id,
        season=season,
        dataset="head_to_head_fixtures",
        prefix=f"head_to_head/league={league_id}/season={season}/",
        suffix="head_to_head_fixtures.parquet",
        target_table="head_to_head_fixtures",
        target_columns=HEAD_TO_HEAD_FIXTURES_TARGET_COLUMNS,
        required_columns=HEAD_TO_HEAD_FIXTURES_REQUIRED_COLUMNS,
        conflict_keys=HEAD_TO_HEAD_FIXTURES_CONFLICT_KEYS,
        int_columns=HEAD_TO_HEAD_FIXTURES_INT_COLUMNS,
        datetime_columns=HEAD_TO_HEAD_FIXTURES_DATETIME_COLUMNS,
        text_columns=HEAD_TO_HEAD_FIXTURES_TEXT_COLUMNS,
        json_columns=HEAD_TO_HEAD_FIXTURES_JSON_COLUMNS,
        prepare_load_df=lambda conn, load_df, run_id: _prepare_head_to_head_load_df(
            conn,
            load_df,
            run_id,
            league_id=league_id,
            season=season,
        ),
    )
