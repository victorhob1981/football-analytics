with final_group_rounds as (
    select
        provider,
        competition_key,
        season_label,
        stage_id,
        group_id,
        max(round_key) as final_round_key
    from {{ ref('fact_group_standings') }}
    where competition_key = 'libertadores'
      and season_label in ('2024', '2025')
    group by
        provider,
        competition_key,
        season_label,
        stage_id,
        group_id
),
final_standings as (
    select
        fgs.provider,
        fgs.competition_key,
        fgs.season_label,
        fgs.stage_id,
        fgs.group_id,
        fgs.team_id,
        fgs.games_played,
        fgs.won,
        fgs.draw,
        fgs.lost,
        fgs.goals_for,
        fgs.goals_against,
        fgs.goal_diff,
        fgs.points
    from {{ ref('fact_group_standings') }} fgs
    inner join final_group_rounds fr
      on fr.provider = fgs.provider
     and fr.competition_key = fgs.competition_key
     and fr.season_label = fgs.season_label
     and fr.stage_id = fgs.stage_id
     and fr.group_id = fgs.group_id
     and fr.final_round_key = fgs.round_key
),
group_stage_matches as (
    select
        fm.provider,
        fm.competition_key,
        fm.season_label,
        fm.stage_id,
        fm.group_id,
        fm.home_team_id as team_id,
        1 as games_played,
        case when fm.home_goals > fm.away_goals then 1 else 0 end as won,
        case when fm.home_goals = fm.away_goals then 1 else 0 end as draw,
        case when fm.home_goals < fm.away_goals then 1 else 0 end as lost,
        fm.home_goals as goals_for,
        fm.away_goals as goals_against,
        (fm.home_goals - fm.away_goals) as goal_diff,
        case
            when fm.home_goals > fm.away_goals then 3
            when fm.home_goals = fm.away_goals then 1
            else 0
        end as points
    from {{ ref('fact_matches') }} fm
    inner join {{ ref('dim_stage') }} st
      on st.stage_sk = fm.stage_sk
    where fm.competition_key = 'libertadores'
      and fm.season_label in ('2024', '2025')
      and st.stage_format = 'group_table'
      and fm.group_id is not null

    union all

    select
        fm.provider,
        fm.competition_key,
        fm.season_label,
        fm.stage_id,
        fm.group_id,
        fm.away_team_id as team_id,
        1 as games_played,
        case when fm.away_goals > fm.home_goals then 1 else 0 end as won,
        case when fm.away_goals = fm.home_goals then 1 else 0 end as draw,
        case when fm.away_goals < fm.home_goals then 1 else 0 end as lost,
        fm.away_goals as goals_for,
        fm.home_goals as goals_against,
        (fm.away_goals - fm.home_goals) as goal_diff,
        case
            when fm.away_goals > fm.home_goals then 3
            when fm.away_goals = fm.home_goals then 1
            else 0
        end as points
    from {{ ref('fact_matches') }} fm
    inner join {{ ref('dim_stage') }} st
      on st.stage_sk = fm.stage_sk
    where fm.competition_key = 'libertadores'
      and fm.season_label in ('2024', '2025')
      and st.stage_format = 'group_table'
      and fm.group_id is not null
),
match_totals as (
    select
        provider,
        competition_key,
        season_label,
        stage_id,
        group_id,
        team_id,
        sum(games_played) as games_played,
        sum(won) as won,
        sum(draw) as draw,
        sum(lost) as lost,
        sum(goals_for) as goals_for,
        sum(goals_against) as goals_against,
        sum(goal_diff) as goal_diff,
        sum(points) as points
    from group_stage_matches
    group by
        provider,
        competition_key,
        season_label,
        stage_id,
        group_id,
        team_id
)
select
    coalesce(fs.provider, mt.provider) as provider,
    coalesce(fs.competition_key, mt.competition_key) as competition_key,
    coalesce(fs.season_label, mt.season_label) as season_label,
    coalesce(fs.stage_id, mt.stage_id) as stage_id,
    coalesce(fs.group_id, mt.group_id) as group_id,
    coalesce(fs.team_id, mt.team_id) as team_id,
    fs.games_played as standings_games_played,
    mt.games_played as match_games_played,
    fs.won as standings_won,
    mt.won as match_won,
    fs.draw as standings_draw,
    mt.draw as match_draw,
    fs.lost as standings_lost,
    mt.lost as match_lost,
    fs.goals_for as standings_goals_for,
    mt.goals_for as match_goals_for,
    fs.goals_against as standings_goals_against,
    mt.goals_against as match_goals_against,
    fs.goal_diff as standings_goal_diff,
    mt.goal_diff as match_goal_diff,
    fs.points as standings_points,
    mt.points as match_points
from final_standings fs
full outer join match_totals mt
  on mt.provider = fs.provider
 and mt.competition_key = fs.competition_key
 and mt.season_label = fs.season_label
 and mt.stage_id = fs.stage_id
 and mt.group_id = fs.group_id
 and mt.team_id = fs.team_id
where coalesce(fs.games_played, -1) <> coalesce(mt.games_played, -1)
   or coalesce(fs.won, -1) <> coalesce(mt.won, -1)
   or coalesce(fs.draw, -1) <> coalesce(mt.draw, -1)
   or coalesce(fs.lost, -1) <> coalesce(mt.lost, -1)
   or coalesce(fs.goals_for, -1) <> coalesce(mt.goals_for, -1)
   or coalesce(fs.goals_against, -1) <> coalesce(mt.goals_against, -1)
   or coalesce(fs.goal_diff, -1) <> coalesce(mt.goal_diff, -1)
   or coalesce(fs.points, -1) <> coalesce(mt.points, -1)
