with source_standings as (
    select * from {{ source('postgres_raw', 'standings_snapshots') }}
)
select
    provider,
    league_id,
    provider_league_id,
    competition_key,
    season_label,
    season_id,
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
    payload,
    ingested_at,
    source_run_id,
    ingested_run,
    updated_at
from source_standings
