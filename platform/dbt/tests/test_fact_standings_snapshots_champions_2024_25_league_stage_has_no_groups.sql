select
    fs.standings_snapshot_id,
    fs.season_label,
    fs.stage_id,
    fs.group_id,
    fs.group_sk
from {{ ref('fact_standings_snapshots') }} fs
inner join {{ ref('dim_stage') }} st
  on st.stage_id = fs.stage_id
 and st.provider = fs.provider
where fs.competition_key = 'champions_league'
  and fs.season_label = '2024_25'
  and st.stage_code = 'league_stage'
  and (
    fs.group_id is not null
    or fs.group_sk is not null
  )
