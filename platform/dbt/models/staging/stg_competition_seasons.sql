with source_seasons as (
    select * from {{ source('postgres_raw', 'competition_seasons') }}
)
select
    provider,
    season_id,
    league_id,
    season_year,
    nullif(trim(season_name), '') as season_name,
    starting_at,
    ending_at,
    payload,
    ingested_run,
    updated_at
from source_seasons
