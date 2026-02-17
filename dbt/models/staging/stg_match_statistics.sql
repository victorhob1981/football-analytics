with source_statistics as (
    select * from {{ source('postgres_raw', 'match_statistics') }}
)
select
    fixture_id,
    team_id,
    team_name,
    shots_on_goal,
    shots_off_goal,
    total_shots,
    blocked_shots,
    shots_inside_box,
    shots_outside_box,
    fouls,
    corner_kicks,
    offsides,
    ball_possession,
    yellow_cards,
    red_cards,
    goalkeeper_saves,
    total_passes,
    passes_accurate,
    passes_pct,
    ingested_run,
    updated_at
from source_statistics
