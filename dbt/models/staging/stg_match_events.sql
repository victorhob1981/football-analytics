with source_events as (
    select * from {{ source('postgres_raw', 'match_events') }}
)
select
    event_id,
    season,
    fixture_id,
    time_elapsed,
    time_extra,
    team_id,
    team_name,
    player_id,
    player_name,
    assist_id as assist_player_id,
    assist_name as assist_player_name,
    type as event_type,
    detail as event_detail,
    comments,
    ingested_run,
    updated_at
from source_events
