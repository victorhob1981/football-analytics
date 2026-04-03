select
    tie_id,
    competition_key,
    season_label,
    winner_team_id
from {{ ref('fact_tie_results') }}
where competition_key = 'copa_do_brasil'
  and season_label in ('2024', '2025')
  and winner_team_id is null
