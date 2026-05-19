select
    competition_key,
    season_label,
    stage_id,
    stage_name
from {{ ref('dim_stage') }}
where competition_key in ('copa_do_brasil', 'libertadores', 'champions_league')
  and stage_format is null
