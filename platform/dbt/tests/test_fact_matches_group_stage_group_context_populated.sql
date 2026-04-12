select
    fm.match_id,
    fm.competition_key,
    fm.season_label,
    fm.stage_id,
    fm.group_id
from {{ ref('fact_matches') }} fm
inner join {{ ref('dim_stage') }} st
  on st.stage_sk = fm.stage_sk
where fm.competition_key = 'libertadores'
  and fm.season_label in ('2024', '2025')
  and st.stage_format = 'group_table'
  and fm.group_id is null
