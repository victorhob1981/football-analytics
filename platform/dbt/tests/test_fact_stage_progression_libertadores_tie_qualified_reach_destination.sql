with stage_participants as (
    select distinct
        provider,
        competition_key,
        season_label,
        stage_id,
        team_id
    from {{ ref('fact_group_standings') }}

    union

    select distinct
        tie.provider,
        tie.competition_key,
        tie.season_label,
        tie.stage_id,
        team.team_id
    from {{ ref('fact_tie_results') }} tie
    cross join lateral (
        values
            (tie.home_side_team_id),
            (tie.away_side_team_id)
    ) as team(team_id)
),
qualified_tie_progressions as (
    select
        provider,
        competition_key,
        season_label,
        team_id,
        from_stage_id,
        to_stage_id
    from {{ ref('fact_stage_progression') }}
    where competition_key = 'libertadores'
      and season_label in ('2024', '2025')
      and progression_scope = 'tie_outcome'
      and progression_type = 'qualified'
      and to_stage_id is not null
)
select
    qtp.provider,
    qtp.competition_key,
    qtp.season_label,
    qtp.team_id,
    qtp.from_stage_id,
    qtp.to_stage_id
from qualified_tie_progressions qtp
left join stage_participants sp
  on sp.provider = qtp.provider
 and sp.competition_key = qtp.competition_key
 and sp.season_label = qtp.season_label
 and sp.stage_id = qtp.to_stage_id
 and sp.team_id = qtp.team_id
where sp.team_id is null
