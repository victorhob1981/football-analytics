with source_stages as (
    select * from {{ source('postgres_raw', 'competition_stages') }}
)
select
    provider,
    stage_id,
    season_id,
    league_id,
    provider_league_id,
    nullif(trim(competition_key), '') as competition_key,
    nullif(trim(season_label), '') as season_label,
    provider_season_id,
    nullif(trim(stage_name), '') as stage_name,
    sort_order,
    finished,
    is_current,
    starting_at,
    ending_at,
    payload,
    ingested_run,
    ingested_at,
    source_run_id,
    updated_at
from source_stages
