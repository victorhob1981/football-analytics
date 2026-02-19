with source_matches as (
    select * from {{ source('postgres_raw', 'fixtures') }}
)
select
    fixture_id,
    date_utc,
    "timestamp" as fixture_timestamp,
    timezone,
    referee,
    venue_id,
    venue_name,
    venue_city,
    cast(null as text) as venue_country,
    status_short,
    status_long,
    league_id,
    league_name,
    season,
    round,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_goals,
    away_goals,
    year,
    month,
    ingested_run
from source_matches
