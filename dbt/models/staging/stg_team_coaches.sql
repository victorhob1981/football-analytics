with source_coaches as (
    select * from {{ source('postgres_raw', 'team_coaches') }}
)
select
    provider,
    coach_tenure_id,
    team_id,
    nullif(trim(coalesce(payload -> 'team' ->> 'name', payload ->> 'team_name')), '') as team_name,
    coach_id,
    nullif(trim(coalesce(payload -> 'coach' ->> 'name', payload ->> 'coach_name')), '') as coach_name,
    position_id,
    active,
    temporary,
    start_date,
    end_date,
    payload,
    ingested_run,
    updated_at
from source_coaches
