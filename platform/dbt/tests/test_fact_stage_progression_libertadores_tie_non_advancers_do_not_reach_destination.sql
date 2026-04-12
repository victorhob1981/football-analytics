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
next_stages as (
    select distinct
        provider,
        competition_key,
        season_label,
        from_stage_id,
        to_stage_id
    from {{ ref('competition_structure_hub') }}
    where competition_key = 'libertadores'
      and season_label in ('2024', '2025')
      and progression_type = 'qualified'
      and to_stage_id is not null
),
non_advancers as (
    select
        provider,
        competition_key,
        season_label,
        team_id,
        from_stage_id,
        progression_type
    from {{ ref('fact_stage_progression') }}
    where competition_key = 'libertadores'
      and season_label in ('2024', '2025')
      and progression_scope = 'tie_outcome'
      and progression_type in ('eliminated', 'repechage')
)
select
    na.provider,
    na.competition_key,
    na.season_label,
    na.team_id,
    na.from_stage_id,
    ns.to_stage_id,
    na.progression_type
from non_advancers na
inner join next_stages ns
  on ns.provider = na.provider
 and ns.competition_key = na.competition_key
 and ns.season_label = na.season_label
 and ns.from_stage_id = na.from_stage_id
inner join stage_participants sp
  on sp.provider = na.provider
 and sp.competition_key = na.competition_key
 and sp.season_label = na.season_label
 and sp.stage_id = ns.to_stage_id
 and sp.team_id = na.team_id
