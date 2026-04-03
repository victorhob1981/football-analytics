select
    match_id,
    competition_key,
    season_label,
    tie_id,
    leg_number
from {{ ref('fact_matches') }}
where competition_key = 'copa_do_brasil'
  and season_label in ('2024', '2025')
  and (
    tie_id is null
    or leg_number is null
  )
