with source_rounds as (
    select * from {{ source('postgres_raw', 'competition_rounds') }}
)
select
    provider,
    round_id,
    stage_id,
    season_id,
    provider_season_id,
    league_id,
    provider_league_id,
    competition_key,
    season_label,
    nullif(trim(round_name), '') as round_name,
    finished,
    is_current,
    starting_at,
    ending_at,
    games_in_week,
    payload,
    ingested_at,
    source_run_id,
    ingested_run,
    updated_at
from source_rounds
