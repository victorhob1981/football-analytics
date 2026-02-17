with matches as (
    select * from {{ ref('stg_matches') }}
)
select
    fixture_id as match_id,
    league_id,
    season,
    date_utc::date as date_day,
    round,
    home_team_id,
    away_team_id,
    venue_id,
    home_goals,
    away_goals,
    coalesce(home_goals, 0) + coalesce(away_goals, 0) as total_goals,
    case
        when coalesce(home_goals, 0) > coalesce(away_goals, 0) then 'Home Win'
        when coalesce(home_goals, 0) < coalesce(away_goals, 0) then 'Away Win'
        else 'Draw'
    end as result
from matches
where fixture_id is not null
  and date_utc is not null
  and home_team_id is not null
  and away_team_id is not null
