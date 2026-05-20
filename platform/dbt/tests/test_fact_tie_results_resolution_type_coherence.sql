select
    tie_id,
    competition_key,
    season_label,
    stage_name,
    match_count,
    home_side_goals,
    away_side_goals,
    has_extra_time_match,
    has_penalties_match,
    resolution_type
from {{ ref('fact_tie_results') }}
where
    (resolution_type = 'penalties' and coalesce(has_penalties_match, false) = false)
 or (resolution_type = 'extra_time' and (coalesce(has_penalties_match, false) = true or coalesce(has_extra_time_match, false) = false))
 or (resolution_type = 'aggregate' and (match_count < 2 or home_side_goals = away_side_goals or coalesce(has_penalties_match, false) = true or coalesce(has_extra_time_match, false) = true))
 or (resolution_type = 'single_match' and (match_count <> 1 or home_side_goals = away_side_goals or coalesce(has_penalties_match, false) = true or coalesce(has_extra_time_match, false) = true))
 or (resolution_type = 'stage_rule' and (coalesce(has_penalties_match, false) = true or coalesce(has_extra_time_match, false) = true or winner_team_id is null))
