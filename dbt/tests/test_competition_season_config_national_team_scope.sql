select
    competition_key,
    season_label,
    participant_scope
from {{ ref('competition_season_config') }}
where participant_scope = 'national_team'
  and competition_key <> 'fifa_world_cup'
