select
    provider,
    competition_key,
    season_label,
    stage_id,
    team_id,
    count(distinct group_id) as distinct_group_count
from {{ ref('fact_group_standings') }}
where competition_key = 'libertadores'
  and season_label in ('2024', '2025')
group by
    provider,
    competition_key,
    season_label,
    stage_id,
    team_id
having count(distinct group_id) > 1
