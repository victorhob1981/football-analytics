with snapshots as (
    select * from {{ ref('fact_standings_snapshots') }}
),
seasons as (
    select
        season_sk,
        season_year
    from {{ ref('dim_season') }}
)
select
    s.provider,
    s.competition_sk,
    s.competition_key,
    s.season_sk,
    coalesce(season_dim.season_year, nullif(split_part(s.season_label, '_', 1), '')::int) as season,
    s.season_label,
    s.provider_league_id,
    s.league_id,
    s.season_id,
    s.provider_season_id,
    s.stage_id,
    s.stage_sk,
    s.round_id,
    s.round_sk,
    s.round_key,
    s.round_key as round,
    s.team_sk,
    s.team_id,
    s.position,
    s.points as points_accumulated,
    s.goals_for as goals_for_accumulated,
    s.goal_diff as goal_diff_accumulated,
    coalesce(s.updated_at, now()) as updated_at
from snapshots s
left join seasons season_dim
  on season_dim.season_sk = s.season_sk
where s.competition_sk is not null
  and s.season_label is not null
  and s.team_id is not null
  and s.round_key is not null
