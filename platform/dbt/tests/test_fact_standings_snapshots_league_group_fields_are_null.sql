select
    standings_snapshot_id,
    competition_key,
    season_label,
    group_id,
    group_sk
from {{ ref('fact_standings_snapshots') }}
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
    or group_sk is not null
  )
