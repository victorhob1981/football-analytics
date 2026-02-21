with source_sidelined as (
    select * from {{ source('postgres_raw', 'team_sidelined') }}
)
select
    provider,
    sidelined_id,
    team_id,
    nullif(trim(coalesce(payload -> 'team' ->> 'name', payload ->> 'team_name')), '') as team_name,
    player_id,
    nullif(trim(coalesce(payload -> 'player' ->> 'name', payload ->> 'player_name')), '') as player_name,
    season_id,
    lower(nullif(trim(category), '')) as category,
    type_id,
    start_date,
    end_date,
    games_missed,
    completed,
    payload,
    ingested_run,
    updated_at
from source_sidelined
