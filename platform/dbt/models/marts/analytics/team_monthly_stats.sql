with
teams as (
    select team_sk, team_id, team_name from {{ ref('dim_team') }}
),
team_rows as (
    select * from {{ ref('int_team_match_rows') }}
),
aggregated as (
    select
        tr.season,
        tr.year,
        tr.month,
        tr.team_sk,
        tr.team_id,
        t.team_name,
        sum(tr.goals_for)::int as goals_for,
        sum(tr.goals_against)::int as goals_against,
        count(*)::int as matches,
        sum(tr.wins)::int as wins,
        sum(tr.draws)::int as draws,
        sum(tr.losses)::int as losses,
        (sum(tr.wins) * 3 + sum(tr.draws))::int as points,
        (sum(tr.goals_for) - sum(tr.goals_against))::int as goal_diff
    from team_rows tr
    left join teams t
      on t.team_sk = tr.team_sk
    group by tr.season, tr.year, tr.month, tr.team_sk, tr.team_id, t.team_name
)
select * from aggregated
