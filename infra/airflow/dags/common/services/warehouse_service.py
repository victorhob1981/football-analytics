from __future__ import annotations

import json
import os
import re
from io import BytesIO

import boto3
import pandas as pd
from airflow.operators.python import get_current_context
from sqlalchemy import create_engine, text

from common.competition_identity import derive_season_label, looks_like_provider_identifier
from common.discard_guardrails import LOADER_DISCARD_THRESHOLD, drop_duplicates_with_threshold, filter_with_threshold
from common.observability import StepMetrics, log_event
from common.runtime import resolve_runtime_params


SILVER_BUCKET = "football-silver"
H2H_SCOPE_REJECTION_THRESHOLD = 0.05

FIXTURES_TARGET_COLUMNS = [
    "fixture_id",
    "source_provider",
    "provider",
    "provider_league_id",
    "competition_key",
    "competition_type",
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
    "season_label",
    "provider_season_id",
    "season_name",
    "season_start_date",
    "season_end_date",
    "round",
    "stage_id",
    "stage_name",
    "round_id",
    "round_name",
    "group_name",
    "leg",
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
    "ingested_at",
    "source_run_id",
    "ingested_run",
]
FIXTURES_REQUIRED_COLUMNS = [
    "fixture_id",
    "source_provider",
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
]

STATISTICS_TARGET_COLUMNS = [
    "fixture_id",
    "team_id",
    "provider",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
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
    "ingested_at",
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
]

EVENTS_TARGET_COLUMNS = [
    "event_id",
    "season",
    "fixture_id",
    "provider",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "provider_event_id",
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
    "ingested_at",
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
    "provider_league_id",
    "provider_season_id",
    "provider_event_id",
    "time_elapsed",
    "time_extra",
    "team_id",
    "player_id",
    "assist_id",
]
EVENTS_BOOL_COLUMNS = ["is_time_elapsed_anomalous"]
EVENTS_TEXT_COLUMNS = [
    "event_id",
    "provider",
    "competition_key",
    "season_label",
    "team_name",
    "player_name",
    "assist_name",
    "type",
    "detail",
    "comments",
    "source_run_id",
]

COMPETITION_LEAGUES_TARGET_COLUMNS = [
    "provider",
    "league_id",
    "provider_league_id",
    "competition_key",
    "competition_type",
    "league_name",
    "country_id",
    "payload",
    "ingested_at",
    "source_run_id",
    "ingested_run",
]
COMPETITION_LEAGUES_REQUIRED_COLUMNS = ["provider", "league_id"]
COMPETITION_LEAGUES_CONFLICT_KEYS = ["provider", "league_id"]
COMPETITION_LEAGUES_INT_COLUMNS = ["league_id", "provider_league_id", "country_id"]
COMPETITION_LEAGUES_JSON_COLUMNS = ["payload"]
COMPETITION_LEAGUES_TEXT_COLUMNS = ["provider", "competition_key", "competition_type", "league_name", "source_run_id"]

COMPETITION_SEASONS_TARGET_COLUMNS = [
    "provider",
    "season_id",
    "provider_season_id",
    "league_id",
    "provider_league_id",
    "competition_key",
    "season_year",
    "season_label",
    "season_name",
    "starting_at",
    "ending_at",
    "payload",
    "ingested_at",
    "source_run_id",
    "ingested_run",
]
COMPETITION_SEASONS_REQUIRED_COLUMNS = ["provider", "season_id"]
COMPETITION_SEASONS_CONFLICT_KEYS = ["provider", "season_id"]
COMPETITION_SEASONS_INT_COLUMNS = ["season_id", "provider_season_id", "league_id", "provider_league_id", "season_year"]
COMPETITION_SEASONS_DATE_COLUMNS = ["starting_at", "ending_at"]
COMPETITION_SEASONS_JSON_COLUMNS = ["payload"]
COMPETITION_SEASONS_TEXT_COLUMNS = ["provider", "competition_key", "season_label", "season_name", "source_run_id"]

COMPETITION_STAGES_TARGET_COLUMNS = [
    "provider",
    "stage_id",
    "season_id",
    "provider_season_id",
    "league_id",
    "provider_league_id",
    "competition_key",
    "season_label",
    "stage_name",
    "sort_order",
    "finished",
    "is_current",
    "starting_at",
    "ending_at",
    "payload",
    "ingested_at",
    "source_run_id",
    "ingested_run",
]
COMPETITION_STAGES_REQUIRED_COLUMNS = ["provider", "stage_id"]
COMPETITION_STAGES_CONFLICT_KEYS = ["provider", "stage_id"]
COMPETITION_STAGES_INT_COLUMNS = [
    "stage_id",
    "season_id",
    "provider_season_id",
    "league_id",
    "provider_league_id",
    "sort_order",
]
COMPETITION_STAGES_BOOL_COLUMNS = ["finished", "is_current"]
COMPETITION_STAGES_DATE_COLUMNS = ["starting_at", "ending_at"]
COMPETITION_STAGES_JSON_COLUMNS = ["payload"]
COMPETITION_STAGES_TEXT_COLUMNS = ["provider", "competition_key", "season_label", "stage_name", "source_run_id"]

COMPETITION_ROUNDS_TARGET_COLUMNS = [
    "provider",
    "round_id",
    "stage_id",
    "season_id",
    "provider_season_id",
    "league_id",
    "provider_league_id",
    "competition_key",
    "season_label",
    "round_name",
    "finished",
    "is_current",
    "starting_at",
    "ending_at",
    "games_in_week",
    "payload",
    "ingested_at",
    "source_run_id",
    "ingested_run",
]
COMPETITION_ROUNDS_REQUIRED_COLUMNS = ["provider", "round_id"]
COMPETITION_ROUNDS_CONFLICT_KEYS = ["provider", "round_id"]
COMPETITION_ROUNDS_INT_COLUMNS = [
    "round_id",
    "stage_id",
    "season_id",
    "provider_season_id",
    "league_id",
    "provider_league_id",
    "games_in_week",
]
COMPETITION_ROUNDS_BOOL_COLUMNS = ["finished", "is_current"]
COMPETITION_ROUNDS_DATE_COLUMNS = ["starting_at", "ending_at"]
COMPETITION_ROUNDS_JSON_COLUMNS = ["payload"]
COMPETITION_ROUNDS_TEXT_COLUMNS = ["provider", "competition_key", "season_label", "round_name", "source_run_id"]

STANDINGS_SNAPSHOTS_TARGET_COLUMNS = [
    "provider",
    "league_id",
    "provider_league_id",
    "competition_key",
    "season_label",
    "season_id",
    "provider_season_id",
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
    "ingested_at",
    "source_run_id",
    "ingested_run",
]
STANDINGS_SNAPSHOTS_REQUIRED_COLUMNS = ["provider", "league_id", "season_id", "stage_id", "round_id", "team_id"]
STANDINGS_SNAPSHOTS_CONFLICT_KEYS = ["provider", "season_id", "stage_id", "round_id", "team_id"]
STANDINGS_SNAPSHOTS_INT_COLUMNS = [
    "league_id",
    "provider_league_id",
    "season_id",
    "provider_season_id",
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
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
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
    "ingested_at",
    "source_run_id",
    "ingested_run",
]
FIXTURE_LINEUPS_REQUIRED_COLUMNS = ["provider", "fixture_id", "team_id", "lineup_id"]
FIXTURE_LINEUPS_CONFLICT_KEYS = ["provider", "fixture_id", "team_id", "lineup_id"]
FIXTURE_LINEUPS_INT_COLUMNS = [
    "fixture_id",
    "provider_league_id",
    "provider_season_id",
    "team_id",
    "player_id",
    "lineup_id",
    "position_id",
    "lineup_type_id",
    "formation_position",
    "jersey_number",
]
FIXTURE_LINEUPS_JSON_COLUMNS = ["details", "payload"]
FIXTURE_LINEUPS_TEXT_COLUMNS = ["provider", "competition_key", "season_label", "position_name", "formation_field", "source_run_id"]

FIXTURE_PLAYER_STATISTICS_TARGET_COLUMNS = [
    "provider",
    "fixture_id",
    "provider_league_id",
    "competition_key",
    "season_label",
    "provider_season_id",
    "team_id",
    "player_id",
    "statistics",
    "payload",
    "ingested_at",
    "source_run_id",
    "ingested_run",
]
FIXTURE_PLAYER_STATISTICS_REQUIRED_COLUMNS = ["provider", "fixture_id", "team_id", "player_id"]
FIXTURE_PLAYER_STATISTICS_CONFLICT_KEYS = ["provider", "fixture_id", "team_id", "player_id"]
FIXTURE_PLAYER_STATISTICS_INT_COLUMNS = ["fixture_id", "provider_league_id", "provider_season_id", "team_id", "player_id"]
FIXTURE_PLAYER_STATISTICS_JSON_COLUMNS = ["statistics", "payload"]
FIXTURE_PLAYER_STATISTICS_TEXT_COLUMNS = ["provider", "competition_key", "season_label", "source_run_id"]

PLAYER_SEASON_STATISTICS_TARGET_COLUMNS = [
    "provider",
    "player_id",
    "season_id",
    "provider_season_id",
    "team_id",
    "league_id",
    "provider_league_id",
    "competition_key",
    "season_label",
    "season_name",
    "position_name",
    "statistics",
    "payload",
    "ingested_at",
    "source_run_id",
    "ingested_run",
]
PLAYER_SEASON_STATISTICS_REQUIRED_COLUMNS = ["provider", "player_id", "season_id", "team_id"]
PLAYER_SEASON_STATISTICS_CONFLICT_KEYS = ["provider", "player_id", "season_id", "team_id"]
PLAYER_SEASON_STATISTICS_INT_COLUMNS = [
    "player_id",
    "season_id",
    "provider_season_id",
    "team_id",
    "league_id",
    "provider_league_id",
]
PLAYER_SEASON_STATISTICS_JSON_COLUMNS = ["statistics", "payload"]
PLAYER_SEASON_STATISTICS_TEXT_COLUMNS = ["provider", "competition_key", "season_label", "season_name", "position_name", "source_run_id"]

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

HEAD_TO_HEAD_FIXTURES_TARGET_COLUMNS = [
    "provider",
    "pair_team_id",
    "pair_opponent_id",
    "fixture_id",
    "league_id",
    "provider_league_id",
    "competition_key",
    "season_label",
    "season_id",
    "provider_season_id",
    "match_date",
    "home_team_id",
    "away_team_id",
    "home_goals",
    "away_goals",
    "payload",
    "ingested_at",
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
    "provider_league_id",
    "season_id",
    "provider_season_id",
    "home_team_id",
    "away_team_id",
    "home_goals",
    "away_goals",
]
HEAD_TO_HEAD_FIXTURES_DATETIME_COLUMNS = ["match_date"]
HEAD_TO_HEAD_FIXTURES_JSON_COLUMNS = ["payload"]
HEAD_TO_HEAD_FIXTURES_TEXT_COLUMNS = ["provider", "competition_key", "season_label", "source_run_id"]


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


def _now_utc_timestamp() -> pd.Timestamp:
    return pd.Timestamp.utcnow()


def _competition_mapping_df(conn) -> pd.DataFrame:
    query = text(
        """
        SELECT
          m.provider,
          m.provider_league_id,
          m.competition_key,
          m.provider_name,
          c.competition_type
        FROM control.competition_provider_map m
        JOIN control.competitions c
          ON c.competition_key = m.competition_key
        WHERE m.is_active = TRUE
          AND c.is_active = TRUE
        """
    )
    rows = conn.execute(query).mappings().all()
    if not rows:
        return pd.DataFrame(columns=["provider", "provider_league_id", "competition_key", "provider_name", "competition_type"])
    return pd.DataFrame(rows)


def _season_identity_df(conn) -> pd.DataFrame:
    query = text(
        """
        SELECT
          provider,
          season_id,
          provider_season_id,
          provider_league_id,
          competition_key,
          season_label,
          season_name,
          starting_at,
          ending_at
        FROM raw.competition_seasons
        """
    )
    rows = conn.execute(query).mappings().all()
    if not rows:
        return pd.DataFrame(
            columns=[
                "provider",
                "season_id",
                "provider_season_id",
                "provider_league_id",
                "competition_key",
                "season_label",
                "season_name",
                "starting_at",
                "ending_at",
            ]
        )
    return pd.DataFrame(rows)


def _fixture_identity_df(conn, fixture_ids: list[int]) -> pd.DataFrame:
    if not fixture_ids:
        return pd.DataFrame(
            columns=[
                "fixture_id",
                "provider",
                "provider_league_id",
                "competition_key",
                "competition_type",
                "season_label",
                "provider_season_id",
                "season_name",
                "season_start_date",
                "season_end_date",
                "source_run_id",
            ]
        )
    query = text(
        """
        SELECT
          fixture_id,
          provider,
          provider_league_id,
          competition_key,
          competition_type,
          season_label,
          provider_season_id,
          season_name,
          season_start_date,
          season_end_date,
          source_run_id
        FROM raw.fixtures
        WHERE fixture_id = ANY(:fixture_ids)
        """
    )
    rows = conn.execute(query, {"fixture_ids": fixture_ids}).mappings().all()
    if not rows:
        return pd.DataFrame(
            columns=[
                "fixture_id",
                "provider",
                "provider_league_id",
                "competition_key",
                "competition_type",
                "season_label",
                "provider_season_id",
                "season_name",
                "season_start_date",
                "season_end_date",
                "source_run_id",
            ]
        )
    return pd.DataFrame(rows)


def _h2h_fixture_scope_df(conn, fixture_ids: list[int]) -> pd.DataFrame:
    if not fixture_ids:
        return pd.DataFrame(
            columns=[
                "fixture_id",
                "fixture_provider",
                "fixture_league_id",
                "fixture_competition_key",
                "fixture_season",
                "fixture_season_label",
                "fixture_provider_season_id",
            ]
        )
    query = text(
        """
        SELECT
          fixture_id,
          provider AS fixture_provider,
          league_id AS fixture_league_id,
          competition_key AS fixture_competition_key,
          season AS fixture_season,
          season_label AS fixture_season_label,
          provider_season_id AS fixture_provider_season_id
        FROM raw.fixtures
        WHERE fixture_id = ANY(:fixture_ids)
        """
    )
    rows = conn.execute(query, {"fixture_ids": fixture_ids}).mappings().all()
    if not rows:
        return pd.DataFrame(
            columns=[
                "fixture_id",
                "fixture_provider",
                "fixture_league_id",
                "fixture_competition_key",
                "fixture_season",
                "fixture_season_label",
                "fixture_provider_season_id",
            ]
        )
    return pd.DataFrame(rows)


def _merge_competition_mapping(df: pd.DataFrame, mapping_df: pd.DataFrame) -> pd.DataFrame:
    if mapping_df.empty or "provider" not in df.columns or "provider_league_id" not in df.columns:
        return df
    enriched = df.merge(mapping_df, how="left", on=["provider", "provider_league_id"], suffixes=("", "_catalog"))
    if "competition_key_catalog" in enriched.columns:
        enriched["competition_key"] = enriched["competition_key"].fillna(enriched["competition_key_catalog"])
        enriched = enriched.drop(columns=["competition_key_catalog"])
    if "competition_type_catalog" in enriched.columns:
        if "competition_type" in enriched.columns:
            enriched["competition_type"] = enriched["competition_type"].fillna(enriched["competition_type_catalog"])
        else:
            enriched["competition_type"] = enriched["competition_type_catalog"]
        enriched = enriched.drop(columns=["competition_type_catalog"])
    if "provider_name" in enriched.columns:
        enriched = enriched.drop(columns=["provider_name"])
    return enriched


def _apply_common_provenance(df: pd.DataFrame, *, run_id: str) -> pd.DataFrame:
    if "source_run_id" in df.columns:
        df["source_run_id"] = df["source_run_id"].fillna(run_id)
    if "ingested_at" in df.columns:
        df["ingested_at"] = df["ingested_at"].where(df["ingested_at"].notna(), _now_utc_timestamp())
    return df


def _enrich_fixtures_dataframe(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    df = load_df.copy()
    if "provider" in df.columns:
        df["provider"] = df["provider"].fillna(df.get("source_provider"))
    if "provider_league_id" in df.columns:
        df["provider_league_id"] = df["provider_league_id"].fillna(df.get("league_id"))
    if "source_run_id" in df.columns:
        df["source_run_id"] = run_id
    if "ingested_at" in df.columns:
        df["ingested_at"] = _now_utc_timestamp()
    if "round_name" in df.columns and "round" in df.columns:
        df["round_name"] = df["round_name"].fillna(df["round"])
    mapping_df = _competition_mapping_df(conn)
    df = _merge_competition_mapping(df, mapping_df)
    if "season_label" in df.columns:
        df["season_label"] = df.apply(
            lambda row: row["season_label"]
            if pd.notna(row.get("season_label"))
            else derive_season_label(
                season=row.get("season"),
                season_name=row.get("season_name"),
                start_date=row.get("season_start_date"),
                end_date=row.get("season_end_date"),
            ),
            axis=1,
        )
    return _apply_common_provenance(df, run_id=run_id)


def _enrich_competition_leagues_dataframe(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    df = load_df.copy()
    if "provider_league_id" in df.columns:
        df["provider_league_id"] = df["provider_league_id"].fillna(df.get("league_id"))
    df = _merge_competition_mapping(df, _competition_mapping_df(conn))
    return _apply_common_provenance(df, run_id=run_id)


def _enrich_competition_seasons_dataframe(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    df = load_df.copy()
    if "provider_league_id" in df.columns:
        df["provider_league_id"] = df["provider_league_id"].fillna(df.get("league_id"))
    if "provider_season_id" in df.columns:
        df["provider_season_id"] = df["provider_season_id"].fillna(df.get("season_id"))
    df = _merge_competition_mapping(df, _competition_mapping_df(conn))
    if "season_label" in df.columns:
        df["season_label"] = df.apply(
            lambda row: row["season_label"]
            if pd.notna(row.get("season_label"))
            else derive_season_label(
                season=row.get("season_year"),
                season_name=row.get("season_name"),
                start_date=row.get("starting_at"),
                end_date=row.get("ending_at"),
            ),
            axis=1,
        )
    return _apply_common_provenance(df, run_id=run_id)


def _enrich_with_season_identity(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    df = load_df.copy()
    if "provider_league_id" in df.columns and "league_id" in df.columns:
        df["provider_league_id"] = df["provider_league_id"].fillna(df["league_id"])
    seasons_df = _season_identity_df(conn)
    if not seasons_df.empty and "provider" in df.columns and "season_id" in df.columns:
        season_lookup = seasons_df.drop_duplicates(subset=["provider", "season_id"], keep="last")
        df = df.merge(
            season_lookup[
                [
                    "provider",
                    "season_id",
                    "provider_league_id",
                    "competition_key",
                    "season_label",
                    "provider_season_id",
                ]
            ],
            how="left",
            on=["provider", "season_id"],
            suffixes=("", "_season"),
        )
        for column in ["provider_league_id", "competition_key", "season_label", "provider_season_id"]:
            season_column = f"{column}_season"
            if season_column in df.columns:
                df[column] = df[column].fillna(df[season_column])
                df = df.drop(columns=[season_column])
    df = _merge_competition_mapping(df, _competition_mapping_df(conn))
    if "season_label" in df.columns:
        fill_mask = df["season_label"].isna()
        if fill_mask.any():
            df.loc[fill_mask, "season_label"] = df.loc[fill_mask].apply(
                lambda row: derive_season_label(
                    season=row.get("season_year", row.get("season")),
                    season_name=row.get("season_name"),
                    start_date=row.get("starting_at"),
                    end_date=row.get("ending_at"),
                ),
                axis=1,
            )
        bad_label_mask = df["season_label"].map(looks_like_provider_identifier)
        bad_label_count = int(bad_label_mask.sum())
        if bad_label_count:
            print(
                f"[enrichment] {bad_label_count} rows com season_label que parece ID de provider. "
                f"Definindo como NULL para evitar falsa identidade. run={run_id}"
            )
            df.loc[bad_label_mask, "season_label"] = pd.NA
    return _apply_common_provenance(df, run_id=run_id)


def _enrich_with_fixture_identity(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    df = load_df.copy()
    fixture_ids = [int(value) for value in pd.Series(df.get("fixture_id")).dropna().astype("int64").unique().tolist()]
    fixture_df = _fixture_identity_df(conn, fixture_ids)
    if not fixture_df.empty and "fixture_id" in df.columns:
        df = df.merge(fixture_df, how="left", on="fixture_id", suffixes=("", "_fixture"))
        for column in [
            "provider",
            "provider_league_id",
            "competition_key",
            "competition_type",
            "season_label",
            "provider_season_id",
            "season_name",
            "season_start_date",
            "season_end_date",
            "source_run_id",
        ]:
            fixture_column = f"{column}_fixture"
            if fixture_column in df.columns:
                if column in df.columns:
                    df[column] = df[column].fillna(df[fixture_column])
                else:
                    df[column] = df[fixture_column]
                df = df.drop(columns=[fixture_column])
    return _apply_common_provenance(df, run_id=run_id)


def _validate_h2h_scope(
    conn,
    load_df: pd.DataFrame,
    run_id: str,
    *,
    league_id: int | None,
    season: int | None,
    season_label: str | None,
    provider_season_id: int | None,
    rejection_threshold: float = H2H_SCOPE_REJECTION_THRESHOLD,
) -> pd.DataFrame:
    df = load_df.copy()
    total_received = len(df)
    if total_received == 0:
        return df

    reason_counts = {
        "missing_fixture_id": 0,
        "orphan_fixture_id": 0,
        "null_competition_key": 0,
        "outside_target_scope": 0,
        "scope_mismatch_with_fixture": 0,
    }

    if "fixture_id" not in df.columns:
        df["fixture_id"] = pd.Series(pd.NA, index=df.index, dtype="Int64")
    else:
        df["fixture_id"] = pd.to_numeric(df["fixture_id"], errors="coerce").astype("Int64")

    missing_fixture_mask = df["fixture_id"].isna()
    reason_counts["missing_fixture_id"] = int(missing_fixture_mask.sum())
    if reason_counts["missing_fixture_id"]:
        df = df[~missing_fixture_mask].copy()

    fixture_ids = [int(value) for value in df["fixture_id"].dropna().astype("int64").unique().tolist()]
    fixture_scope_df = _h2h_fixture_scope_df(conn, fixture_ids)
    df = df.merge(fixture_scope_df, how="left", on="fixture_id")

    orphan_mask = df["fixture_provider"].isna()
    reason_counts["orphan_fixture_id"] = int(orphan_mask.sum())
    if reason_counts["orphan_fixture_id"]:
        df = df[~orphan_mask].copy()

    null_competition_key_mask = df["competition_key"].isna()
    reason_counts["null_competition_key"] = int(null_competition_key_mask.sum())
    if reason_counts["null_competition_key"]:
        df = df[~null_competition_key_mask].copy()

    outside_target_scope_mask = pd.Series(False, index=df.index)
    if league_id is not None:
        outside_target_scope_mask = outside_target_scope_mask | df["fixture_league_id"].isna() | (
            pd.to_numeric(df["fixture_league_id"], errors="coerce").astype("Int64") != league_id
        )
    if season is not None:
        outside_target_scope_mask = outside_target_scope_mask | df["fixture_season"].isna() | (
            pd.to_numeric(df["fixture_season"], errors="coerce").astype("Int64") != season
        )
    if season_label is not None:
        outside_target_scope_mask = outside_target_scope_mask | df["fixture_season_label"].isna() | (
            df["fixture_season_label"].astype("string") != season_label
        )
    if provider_season_id is not None:
        outside_target_scope_mask = outside_target_scope_mask | df["fixture_provider_season_id"].isna() | (
            pd.to_numeric(df["fixture_provider_season_id"], errors="coerce").astype("Int64") != provider_season_id
        )
    reason_counts["outside_target_scope"] = int(outside_target_scope_mask.sum())
    if reason_counts["outside_target_scope"]:
        df = df[~outside_target_scope_mask].copy()

    scope_mismatch_mask = pd.Series(False, index=df.index)
    scope_mismatch_mask = scope_mismatch_mask | df["provider"].isna() | (
        df["provider"].astype("string") != df["fixture_provider"].astype("string")
    )
    scope_mismatch_mask = scope_mismatch_mask | df["competition_key"].isna() | (
        df["competition_key"].astype("string") != df["fixture_competition_key"].astype("string")
    )
    scope_mismatch_mask = scope_mismatch_mask | df["league_id"].isna() | (
        pd.to_numeric(df["league_id"], errors="coerce").astype("Int64")
        != pd.to_numeric(df["fixture_league_id"], errors="coerce").astype("Int64")
    )
    scope_mismatch_mask = scope_mismatch_mask | df["season_label"].isna() | (
        df["season_label"].astype("string") != df["fixture_season_label"].astype("string")
    )
    scope_mismatch_mask = scope_mismatch_mask | df["provider_season_id"].isna() | (
        pd.to_numeric(df["provider_season_id"], errors="coerce").astype("Int64")
        != pd.to_numeric(df["fixture_provider_season_id"], errors="coerce").astype("Int64")
    )
    if "season_id" in df.columns:
        scope_mismatch_mask = scope_mismatch_mask | df["season_id"].isna() | (
            pd.to_numeric(df["season_id"], errors="coerce").astype("Int64")
            != pd.to_numeric(df["fixture_provider_season_id"], errors="coerce").astype("Int64")
        )
    reason_counts["scope_mismatch_with_fixture"] = int(scope_mismatch_mask.sum())
    if reason_counts["scope_mismatch_with_fixture"]:
        df = df[~scope_mismatch_mask].copy()

    total_approved = len(df)
    total_rejected = total_received - total_approved
    reason_summary = ", ".join(f"{key}={value}" for key, value in reason_counts.items() if value) or "none"
    print(
        f"[h2h_barrier2] rows_received={total_received} rows_approved={total_approved} "
        f"rows_rejected={total_rejected} | reasons={reason_summary} "
        f"| scope=league_id={league_id} season={season} season_label={season_label} "
        f"provider_season_id={provider_season_id} | run={run_id}"
    )

    if total_received > 0 and (total_rejected / total_received) > rejection_threshold:
        raise RuntimeError(
            f"[h2h_barrier2] Taxa de rejeicao excessiva: "
            f"{total_rejected}/{total_received} rows ({total_rejected / total_received:.1%}) "
            f"| reasons={reason_summary} | run={run_id}. Verificar escopo da ingestao."
        )

    return df


def _enrich_and_validate_h2h_scope(
    conn,
    load_df: pd.DataFrame,
    run_id: str,
    *,
    league_id: int,
    season: int,
    season_label: str | None,
    provider_season_id: int | None,
) -> pd.DataFrame:
    enriched_df = _enrich_with_season_identity(conn, load_df, run_id)
    return _validate_h2h_scope(
        conn,
        enriched_df,
        run_id,
        league_id=league_id,
        season=season,
        season_label=season_label,
        provider_season_id=provider_season_id,
    )


def _sync_season_catalog_from_raw(conn, run_id: str) -> None:
    conn.execute(
        text(
            """
            INSERT INTO control.season_catalog (
              competition_key,
              season_label,
              season_start_date,
              season_end_date,
              is_closed,
              provider,
              provider_season_id,
              created_at,
              updated_at
            )
            SELECT
              competition_key,
              season_label,
              starting_at,
              ending_at,
              CASE
                WHEN ending_at IS NOT NULL AND ending_at < CURRENT_DATE THEN TRUE
                ELSE FALSE
              END AS is_closed,
              provider,
              provider_season_id,
              now(),
              now()
            FROM raw.competition_seasons
            WHERE ingested_run = :run_id
              AND competition_key IS NOT NULL
              AND season_label IS NOT NULL
            ON CONFLICT (competition_key, season_label, provider) DO UPDATE
            SET
              season_start_date = EXCLUDED.season_start_date,
              season_end_date = EXCLUDED.season_end_date,
              is_closed = EXCLUDED.is_closed,
              provider_season_id = EXCLUDED.provider_season_id,
              updated_at = now()
            """
        ),
        {"run_id": run_id},
    )


def _mark_sync_scope_validated(
    engine,
    *,
    provider_name: str,
    entity_type: str,
    scope_key: str,
    league_id: int,
    season: int,
    scope_validation_notes: str,
) -> None:
    from common.services.ingestion_service import _upsert_sync_state

    _upsert_sync_state(
        engine,
        provider_name=provider_name,
        entity_type=entity_type,
        scope_key=scope_key,
        league_id=league_id,
        season=season,
        cursor=None,
        status="success",
        update_last_successful_sync=False,
        scope_validated=True,
        scope_validation_notes=scope_validation_notes,
    )


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
    pre_upsert_transform: Callable[[Any, pd.DataFrame, str], pd.DataFrame] | None = None,
    post_upsert_hook: Callable[[Any, str], None] | None = None,
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

        with engine.begin() as conn:
            if pre_upsert_transform is not None:
                load_df = pre_upsert_transform(conn, load_df, run_id)

            invalid_mask = pd.Series(False, index=load_df.index)
            for key_col in conflict_keys:
                invalid_mask = invalid_mask | load_df[key_col].isna()
            load_df = filter_with_threshold(
                load_df,
                keep_mask=~invalid_mask,
                threshold=LOADER_DISCARD_THRESHOLD,
                context_label=f"raw.{target_table}",
                reason="null_conflict_key",
                subset=conflict_keys,
                channel="loader",
            )

            load_df = drop_duplicates_with_threshold(
                load_df,
                subset=conflict_keys,
                threshold=LOADER_DISCARD_THRESHOLD,
                context_label=f"raw.{target_table}",
                reason="duplicate_conflict_key",
                channel="loader",
            )
            load_df["ingested_run"] = run_id
            load_df = load_df[target_columns]

            compare_columns = [
                col
                for col in target_columns
                if col not in conflict_keys and col not in {"ingested_run", "created_at", "updated_at"}
            ]
            distinct_predicate = " OR ".join([f"t.{col} IS DISTINCT FROM s.{col}" for col in compare_columns]) or "FALSE"
            insert_cols = ", ".join(target_columns)
            select_cols = ", ".join([f"s.{col}" for col in target_columns])
            update_columns = list(compare_columns)
            if "ingested_run" in target_columns and "ingested_run" not in conflict_keys:
                update_columns.append("ingested_run")
            update_set_parts = [f"{col} = EXCLUDED.{col}" for col in update_columns] + ["updated_at = now()"]
            update_set = ", ".join(update_set_parts)
            conflict_where = " OR ".join(
                [f"raw.{target_table}.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in compare_columns]
            ) or "FALSE"
            join_predicate = " AND ".join([f"t.{col} = s.{col}" for col in conflict_keys])
            first_key = conflict_keys[0]

            _assert_target_columns(conn, schema="raw", table=target_table, expected=target_columns)
            conn.execute(text(f"CREATE TEMP TABLE staging_{target_table} (LIKE raw.{target_table} INCLUDING DEFAULTS) ON COMMIT DROP"))
            load_df.to_sql(f"staging_{target_table}", con=conn, if_exists="append", index=False, method="multi")

            inserted = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM staging_{target_table} s
                    LEFT JOIN raw.{target_table} t
                      ON {join_predicate}
                    WHERE t.{first_key} IS NULL
                    """
                )
            ).scalar_one()
            updated = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM staging_{target_table} s
                    JOIN raw.{target_table} t
                      ON {join_predicate}
                    WHERE {distinct_predicate}
                    """
                )
            ).scalar_one()
            conn.execute(
                text(
                    f"""
                    INSERT INTO raw.{target_table} ({insert_cols})
                    SELECT {select_cols}
                    FROM staging_{target_table} s
                    ON CONFLICT ({", ".join(conflict_keys)}) DO UPDATE
                    SET {update_set}
                    WHERE {conflict_where}
                    """
                )
            )
            if post_upsert_hook is not None:
                post_upsert_hook(conn, run_id)
            ignored = len(load_df) - inserted - updated

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
            "provider_league_id",
            "season",
            "provider_season_id",
            "stage_id",
            "round_id",
            "leg",
            "attendance",
            "home_team_id",
            "away_team_id",
            "home_goals",
            "away_goals",
            "home_goals_ht",
            "away_goals_ht",
            "home_goals_ft",
            "away_goals_ft",
        ]:
            load_df[col] = pd.to_numeric(load_df[col], errors="coerce").astype("Int64")
        for col in ["weather_temperature_c", "weather_wind_kph"]:
            load_df[col] = pd.to_numeric(load_df[col], errors="coerce")
        load_df["date_utc"] = pd.to_datetime(load_df["date_utc"], errors="coerce", utc=True)
        for col in ["season_start_date", "season_end_date"]:
            if col in load_df.columns:
                load_df[col] = pd.to_datetime(load_df[col], errors="coerce").dt.date
        for col in [
            "source_provider",
            "provider",
            "competition_key",
            "competition_type",
            "league_name",
            "season_label",
            "season_name",
            "round",
            "stage_name",
            "round_name",
            "group_name",
            "year",
            "month",
            "source_run_id",
        ]:
            if col in load_df.columns:
                load_df[col] = load_df[col].astype("string")

        invalid_mask = load_df["fixture_id"].isna()
        load_df = filter_with_threshold(
            load_df,
            keep_mask=~invalid_mask,
            threshold=LOADER_DISCARD_THRESHOLD,
            context_label="raw.fixtures",
            reason="null_conflict_key",
            subset=["fixture_id"],
            channel="loader",
        )

        with engine.begin() as conn:
            load_df = _enrich_fixtures_dataframe(conn, load_df, run_id)
            load_df = drop_duplicates_with_threshold(
                load_df,
                subset=["fixture_id"],
                threshold=LOADER_DISCARD_THRESHOLD,
                context_label="raw.fixtures",
                reason="duplicate_conflict_key",
                channel="loader",
            )
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
            conflict_where = " OR ".join(
                [f"raw.fixtures.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in compare_columns]
            )

            _assert_target_columns(conn, schema="raw", table="fixtures", expected=FIXTURES_TARGET_COLUMNS)
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
        load_df["team_name"] = load_df["team_name"].astype("string")
        load_df["passes_pct"] = pd.to_numeric(load_df["passes_pct"], errors="coerce")

        invalid_mask = load_df["fixture_id"].isna() | load_df["team_id"].isna()
        load_df = filter_with_threshold(
            load_df,
            keep_mask=~invalid_mask,
            threshold=LOADER_DISCARD_THRESHOLD,
            context_label="raw.match_statistics",
            reason="null_conflict_key",
            subset=["fixture_id", "team_id"],
            channel="loader",
        )

        with engine.begin() as conn:
            load_df = _enrich_with_fixture_identity(conn, load_df, run_id)
            load_df = drop_duplicates_with_threshold(
                load_df,
                subset=["fixture_id", "team_id"],
                threshold=LOADER_DISCARD_THRESHOLD,
                context_label="raw.match_statistics",
                reason="duplicate_conflict_key",
                channel="loader",
            )
            load_df["ingested_run"] = run_id
            load_df = load_df[STATISTICS_TARGET_COLUMNS]

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
            conflict_where = " OR ".join(
                [f"raw.match_statistics.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in compare_columns]
            )

            _assert_target_columns(conn, schema="raw", table="match_statistics", expected=STATISTICS_TARGET_COLUMNS)
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
            if col not in load_df.columns and col != "ingested_run":
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
        load_df = filter_with_threshold(
            load_df,
            keep_mask=~invalid_mask,
            threshold=LOADER_DISCARD_THRESHOLD,
            context_label="raw.match_events",
            reason="null_conflict_key",
            subset=["provider", "season", "fixture_id", "event_id"],
            channel="loader",
        )

        with engine.begin() as conn:
            load_df = _enrich_with_fixture_identity(conn, load_df, run_id)
            conflict_keys = ["provider", "season", "fixture_id", "event_id"]
            identity_null_mask = (
                load_df["provider"].isna()
                | load_df["season"].isna()
                | load_df["fixture_id"].isna()
                | load_df["event_id"].isna()
            )
            if int(identity_null_mask.sum()):
                sample_fixtures = ", ".join(
                    load_df.loc[identity_null_mask, "fixture_id"]
                    .dropna()
                    .astype(str)
                    .head(5)
                    .tolist()
                )
                raise RuntimeError(
                    "Match events sem identidade fisica completa apos enrichment com fixtures. "
                    f"fixtures={sample_fixtures or 'n/a'}"
                )

            load_df = drop_duplicates_with_threshold(
                load_df,
                subset=conflict_keys,
                threshold=LOADER_DISCARD_THRESHOLD,
                context_label="raw.match_events",
                reason="duplicate_conflict_key",
                channel="loader",
            )
            load_df["ingested_run"] = run_id
            load_df = load_df[EVENTS_TARGET_COLUMNS]

            compare_columns = [
                col
                for col in EVENTS_TARGET_COLUMNS
                if col not in set(conflict_keys + ["ingested_run", "created_at", "updated_at"])
            ]
            distinct_predicate = " OR ".join([f"t.{col} IS DISTINCT FROM s.{col}" for col in compare_columns])
            insert_cols = ", ".join(EVENTS_TARGET_COLUMNS)
            select_cols = ", ".join([f"s.{col}" for col in EVENTS_TARGET_COLUMNS])
            update_columns = list(compare_columns) + ["ingested_run"]
            update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns] + ["updated_at = now()"])
            conflict_where = " OR ".join([f"raw.match_events.{col} IS DISTINCT FROM EXCLUDED.{col}" for col in compare_columns])

            _assert_target_columns(conn, schema="raw", table="match_events", expected=EVENTS_TARGET_COLUMNS)
            conn.execute(text("CREATE TEMP TABLE staging_match_events (LIKE raw.match_events INCLUDING DEFAULTS) ON COMMIT DROP"))
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
        pre_upsert_transform=_enrich_competition_leagues_dataframe,
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
        pre_upsert_transform=_enrich_competition_seasons_dataframe,
        post_upsert_hook=_sync_season_catalog_from_raw,
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
        pre_upsert_transform=_enrich_with_season_identity,
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
        pre_upsert_transform=_enrich_with_season_identity,
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
        pre_upsert_transform=_enrich_with_season_identity,
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
        pre_upsert_transform=_enrich_with_fixture_identity,
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
        pre_upsert_transform=_enrich_with_fixture_identity,
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
        pre_upsert_transform=_enrich_with_season_identity,
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


def load_head_to_head_silver_to_raw():
    context = get_current_context()
    runtime = resolve_runtime_params(context)
    league_id = runtime["league_id"]
    season = runtime["season"]
    season_label = runtime.get("season_label")
    provider_season_id = runtime.get("provider_season_id")
    provider_name = runtime["provider"]
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
        pre_upsert_transform=lambda conn, load_df, run_id: _enrich_and_validate_h2h_scope(
            conn,
            load_df,
            run_id,
            league_id=league_id,
            season=season,
            season_label=season_label,
            provider_season_id=provider_season_id,
        ),
    )
    from common.services.ingestion_service import _sync_scope_key

    _mark_sync_scope_validated(
        create_engine(_get_required_env("FOOTBALL_PG_DSN")),
        provider_name=provider_name,
        entity_type="head_to_head",
        scope_key=_sync_scope_key(league_id=league_id, season=season) + "/entity=head_to_head",
        league_id=league_id,
        season=season,
        scope_validation_notes="validated_against_raw.fixtures",
    )
