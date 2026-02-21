select
    provider,
    sidelined_id as availability_id,
    team_id,
    team_name,
    player_id,
    player_name,
    season_id,
    category as status_type,
    type_id,
    start_date,
    end_date,
    games_missed,
    completed,
    payload,
    ingested_run,
    updated_at
from {{ ref('stg_team_sidelined') }}
