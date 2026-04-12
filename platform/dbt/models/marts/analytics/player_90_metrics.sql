with season_summary as (
    select * from {{ ref('player_season_summary') }}
)
select
    competition_sk,
    season,
    player_sk,
    player_id,
    player_name,
    team_sk,
    team_id,
    team_name,
    matches,
    minutes_played,
    goals,
    assists,
    shots_total,
    shots_on_goal,
    key_passes,
    tackles,
    interceptions,
    duels,
    xg,
    avg_rating,
    case when minutes_played > 0 then goals * 90.0 / minutes_played else null end as goals_per_90,
    case when minutes_played > 0 then assists * 90.0 / minutes_played else null end as assists_per_90,
    case when minutes_played > 0 then shots_total * 90.0 / minutes_played else null end as shots_per_90,
    case when minutes_played > 0 then shots_on_goal * 90.0 / minutes_played else null end as shots_on_goal_per_90,
    case when minutes_played > 0 then key_passes * 90.0 / minutes_played else null end as key_passes_per_90,
    case when minutes_played > 0 then tackles * 90.0 / minutes_played else null end as tackles_per_90,
    case when minutes_played > 0 then interceptions * 90.0 / minutes_played else null end as interceptions_per_90,
    case when minutes_played > 0 then xg * 90.0 / minutes_played else null end as xg_per_90,
    updated_at
from season_summary
