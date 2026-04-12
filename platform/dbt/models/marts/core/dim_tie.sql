with tie_results as (
    select * from {{ ref('int_tie_results') }}
)
select
    tie_id,
    md5(concat('tie:', tie_id)) as tie_sk,
    provider,
    provider_league_id,
    competition_key,
    season_label,
    stage_id,
    stage_sk,
    tie_order,
    home_side_team_id,
    away_side_team_id,
    winner_team_id,
    resolution_type,
    is_inferred
from tie_results
