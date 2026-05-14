with source_standings as (
    select * from {{ source('postgres_raw', 'standings_snapshots') }}
)
select
    provider,
    league_id,
    season_id,
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
    ingested_run,
    updated_at
from source_standings
