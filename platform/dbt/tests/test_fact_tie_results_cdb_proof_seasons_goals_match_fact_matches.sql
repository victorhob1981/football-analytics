with match_side_goals as (
    select
        fm.tie_id,
        sum(
            case
                when home_team_id = dt.home_side_team_id then home_goals
                when away_team_id = dt.home_side_team_id then away_goals
                else 0
            end
        ) as expected_home_side_goals,
        sum(
            case
                when home_team_id = dt.away_side_team_id then home_goals
                when away_team_id = dt.away_side_team_id then away_goals
                else 0
            end
        ) as expected_away_side_goals
    from {{ ref('fact_matches') }} fm
    join {{ ref('dim_tie') }} dt
      on dt.tie_id = fm.tie_id
    where fm.competition_key = 'copa_do_brasil'
      and fm.season_label in ('2024', '2025')
      and fm.tie_id is not null
    group by fm.tie_id
)
select
    tr.tie_id,
    tr.home_side_goals,
    tr.away_side_goals,
    msg.expected_home_side_goals,
    msg.expected_away_side_goals
from {{ ref('fact_tie_results') }} tr
join match_side_goals msg
  on msg.tie_id = tr.tie_id
where tr.competition_key = 'copa_do_brasil'
  and tr.season_label in ('2024', '2025')
  and (
    tr.home_side_goals <> msg.expected_home_side_goals
    or tr.away_side_goals <> msg.expected_away_side_goals
  )
