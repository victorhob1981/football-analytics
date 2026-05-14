with source_stages as (
    select * from {{ source('postgres_raw', 'competition_stages') }}
)
select
    provider,
    stage_id,
    season_id,
    league_id,
    nullif(trim(stage_name), '') as stage_name,
    sort_order,
    finished,
    is_current,
    starting_at,
    ending_at,
    payload,
    ingested_run,
    updated_at
from source_stages
