select
    t.tie_id,
    t.competition_key,
    t.season_label
from {{ ref('dim_tie') }} t
join {{ ref('competition_season_config') }} c
  on c.competition_key = t.competition_key
 and c.season_label = t.season_label
where c.format_family = 'knockout'
  and coalesce(t.is_inferred, false) = false
