with matches as (
    select * from {{ ref('int_matches_enriched') }}
)
select
    season,
    extract(year from date_day)::int as year,
    lpad(extract(month from date_day)::int::text, 2, '0') as month,
    coalesce((regexp_match(round, '([0-9]+)'))[1]::int, 0) as round_number,
    date_day,
    match_id,
    md5(concat('team:', home_team_id::text)) as team_sk,
    home_team_id as team_id,
    coalesce(home_goals, 0) as goals_for,
    coalesce(away_goals, 0) as goals_against,
    case when coalesce(home_goals, 0) > coalesce(away_goals, 0) then 1 else 0 end as wins,
    case when coalesce(home_goals, 0) = coalesce(away_goals, 0) then 1 else 0 end as draws,
    case when coalesce(home_goals, 0) < coalesce(away_goals, 0) then 1 else 0 end as losses,
    case
        when coalesce(home_goals, 0) > coalesce(away_goals, 0) then 3
        when coalesce(home_goals, 0) = coalesce(away_goals, 0) then 1
        else 0
    end as points_round
from matches

union all

select
    season,
    extract(year from date_day)::int as year,
    lpad(extract(month from date_day)::int::text, 2, '0') as month,
    coalesce((regexp_match(round, '([0-9]+)'))[1]::int, 0) as round_number,
    date_day,
    match_id,
    md5(concat('team:', away_team_id::text)) as team_sk,
    away_team_id as team_id,
    coalesce(away_goals, 0) as goals_for,
    coalesce(home_goals, 0) as goals_against,
    case when coalesce(away_goals, 0) > coalesce(home_goals, 0) then 1 else 0 end as wins,
    case when coalesce(away_goals, 0) = coalesce(home_goals, 0) then 1 else 0 end as draws,
    case when coalesce(away_goals, 0) < coalesce(home_goals, 0) then 1 else 0 end as losses,
    case
        when coalesce(away_goals, 0) > coalesce(home_goals, 0) then 3
        when coalesce(away_goals, 0) = coalesce(home_goals, 0) then 1
        else 0
    end as points_round
from matches
