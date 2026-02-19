with source_events as (
    select * from {{ source('postgres_raw', 'match_events') }}
)
select
    event_id,
    season,
    fixture_id,
    case
        when time_elapsed is not null and time_elapsed < 0 then null
        else time_elapsed
    end as time_elapsed,
    time_extra,
    case
        when coalesce(is_time_elapsed_anomalous, false) then true
        when time_elapsed is not null and time_elapsed < 0 then true
        else false
    end as is_time_elapsed_anomalous,
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
