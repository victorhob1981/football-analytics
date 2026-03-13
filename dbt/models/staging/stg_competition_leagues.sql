with source_leagues as (
    select * from {{ source('postgres_raw', 'competition_leagues') }}
)
select
    provider,
    league_id,
    provider_league_id,
    competition_key,
    competition_type,
    nullif(trim(league_name), '') as league_name,
    country_id,
    payload,
    ingested_at,
    source_run_id,
    ingested_run,
    updated_at
from source_leagues
