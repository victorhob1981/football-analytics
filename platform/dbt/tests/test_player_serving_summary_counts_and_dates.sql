select
    player_id
from {{ ref('player_serving_summary') }}
where competition_count < 0
   or season_count < 0
   or team_count < 0
   or matches_played < 0
   or career_start_at > career_end_at;
