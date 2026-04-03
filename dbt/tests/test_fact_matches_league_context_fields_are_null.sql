select
    match_id,
    competition_key,
    season_label,
    group_id,
    tie_id,
    leg_number
from {{ ref('fact_matches') }}
where competition_key in (
    'brasileirao_a',
    'brasileirao_b',
    'premier_league',
    'la_liga',
    'serie_a_it',
    'bundesliga',
    'ligue_1'
)
  and (
    group_id is not null
    or tie_id is not null
    or leg_number is not null
  )
