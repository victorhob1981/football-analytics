with source_standings as (
    select * from {{ source('postgres_raw', 'standings_snapshots') }}
)
select
    provider,
    league_id,
    provider_league_id,
    nullif(trim(competition_key), '') as competition_key,
    season_id,
    nullif(trim(season_label), '') as season_label,
    provider_season_id,
    stage_id,
    round_id,
    team_id,
    position,
    points,
    games_played,
    won,
    draw,
    lost,
    goals_for,
    goals_against,
    goal_diff,
    nullif(trim(payload ->> 'group_id'), '') as raw_group_id,
    payload,
    ingested_run,
    ingested_at,
    source_run_id,
    updated_at
from source_standings
