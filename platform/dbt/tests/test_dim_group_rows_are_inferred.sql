select
    group_id,
    competition_key,
    season_label,
    stage_id,
    is_inferred
from {{ ref('dim_group') }}
where is_inferred is not true
