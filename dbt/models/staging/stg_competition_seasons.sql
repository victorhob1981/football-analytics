with source_seasons as (
    select * from {{ source('postgres_raw', 'competition_seasons') }}
)
select
    provider,
    season_id,
    provider_season_id,
    league_id,
    provider_league_id,
    competition_key,
    season_year,
    season_label,
    nullif(trim(season_name), '') as season_name,
    starting_at,
    ending_at,
    payload,
    ingested_at,
    source_run_id,
    ingested_run,
    updated_at
from source_seasons
