with source_h2h as (
    select * from {{ source('postgres_raw', 'head_to_head_fixtures') }}
)
select
    provider,
    pair_team_id,
    pair_opponent_id,
    fixture_id,
    league_id,
    provider_league_id,
    competition_key,
    season_label,
    season_id,
    provider_season_id,
    match_date,
    home_team_id,
    away_team_id,
    home_goals,
    away_goals,
    payload,
    ingested_at,
    source_run_id,
    ingested_run,
    updated_at
from source_h2h
