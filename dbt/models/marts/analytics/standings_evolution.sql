with match_rows as (
    select
        season,
        round_number as round,
        date_day,
        match_id,
        team_sk,
        team_id,
        goals_for,
        goals_against,
        points_round,
        wins as wins_round
    from {{ ref('int_team_match_rows') }}
),
per_round as (
    select
        season,
        round,
        team_sk,
        team_id,
        min(date_day) as round_date,
        min(match_id) as round_match_id,
        sum(points_round)::int as points_round,
        sum(goals_for)::int as goals_for_round,
        sum(goals_for - goals_against)::int as goal_diff_round,
        sum(wins_round)::int as wins_round
    from match_rows
    group by season, round, team_sk, team_id
),
accumulated as (
    select
        season,
        round,
        team_sk,
        team_id,
        sum(points_round) over (
            partition by season, team_sk
            order by round_date, round, round_match_id
            rows between unbounded preceding and current row
        )::int as points_accumulated,
        sum(goals_for_round) over (
            partition by season, team_sk
            order by round_date, round, round_match_id
            rows between unbounded preceding and current row
        )::int as goals_for_accumulated,
        sum(goal_diff_round) over (
            partition by season, team_sk
            order by round_date, round, round_match_id
            rows between unbounded preceding and current row
        )::int as goal_diff_accumulated,
        sum(wins_round) over (
            partition by season, team_sk
            order by round_date, round, round_match_id
            rows between unbounded preceding and current row
        )::int as wins_accumulated
    from per_round
),
ranked as (
    select
        season,
        round,
        team_sk,
        team_id,
        points_accumulated,
        goals_for_accumulated,
        goal_diff_accumulated,
        dense_rank() over (
            partition by season, round
            order by
                points_accumulated desc,
                wins_accumulated desc,
                goal_diff_accumulated desc,
                goals_for_accumulated desc,
                team_sk asc
        )::int as position
    from accumulated
)
select * from ranked
