select
    team_id
from {{ ref('team_serving_summary') }}
where competition_count < 0
   or season_count < 0
   or matches_played < 0
   or first_match_at > last_match_at
