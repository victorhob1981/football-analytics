with h2h as (
    select * from {{ ref('stg_head_to_head_fixtures') }}
),
competition as (
    select
        league_id,
        competition_sk
    from {{ ref('dim_competition') }}
),
teams as (
    select
        team_id,
        team_name
    from {{ ref('dim_team') }}
)
select
    h.provider,
    c.competition_sk,
    h.league_id,
    min(h.season_id) as first_season_id,
    max(h.season_id) as last_season_id,
    h.pair_team_id,
    max(t1.team_name) as pair_team_name,
    h.pair_opponent_id,
    max(t2.team_name) as pair_opponent_name,
    count(distinct h.fixture_id) as total_matches,
    sum(
        case
            when h.home_team_id = h.pair_team_id and coalesce(h.home_goals, 0) > coalesce(h.away_goals, 0) then 1
            when h.away_team_id = h.pair_team_id and coalesce(h.away_goals, 0) > coalesce(h.home_goals, 0) then 1
            else 0
        end
    ) as pair_team_wins,
    sum(
        case
            when h.home_team_id = h.pair_opponent_id and coalesce(h.home_goals, 0) > coalesce(h.away_goals, 0) then 1
            when h.away_team_id = h.pair_opponent_id and coalesce(h.away_goals, 0) > coalesce(h.home_goals, 0) then 1
            else 0
        end
    ) as pair_opponent_wins,
    sum(case when coalesce(h.home_goals, 0) = coalesce(h.away_goals, 0) then 1 else 0 end) as draws,
    sum(
        case
            when h.home_team_id = h.pair_team_id then coalesce(h.home_goals, 0)
            when h.away_team_id = h.pair_team_id then coalesce(h.away_goals, 0)
            else 0
        end
    ) as pair_team_goals_for,
    sum(
        case
            when h.home_team_id = h.pair_opponent_id then coalesce(h.home_goals, 0)
            when h.away_team_id = h.pair_opponent_id then coalesce(h.away_goals, 0)
            else 0
        end
    ) as pair_opponent_goals_for,
    max(h.updated_at) as updated_at
from h2h h
left join competition c
  on c.league_id = h.league_id
left join teams t1
  on t1.team_id = h.pair_team_id
left join teams t2
  on t2.team_id = h.pair_opponent_id
group by
    h.provider,
    c.competition_sk,
    h.league_id,
    h.pair_team_id,
    h.pair_opponent_id
