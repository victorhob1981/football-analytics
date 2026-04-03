select
    fs.standings_snapshot_id,
    fs.competition_key,
    fs.season_label,
    fs.stage_id,
    fs.round_id,
    fs.team_id,
    fs.group_id,
    fs.group_sk
from {{ ref('fact_standings_snapshots') }} fs
inner join {{ ref('dim_stage') }} st
  on st.stage_sk = fs.stage_sk
where fs.competition_key = 'libertadores'
  and fs.season_label in ('2024', '2025')
  and st.stage_format = 'group_table'
  and (
    fs.group_id is null
    or fs.group_sk is null
  )
