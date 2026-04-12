with group_non_advancers as (
    select
        season_label,
        team_id,
        progression_type
    from {{ ref('fact_stage_progression') }}
    where competition_key = 'libertadores'
      and season_label in ('2024', '2025')
      and progression_scope = 'group_position'
      and progression_type in ('eliminated', 'repechage')
),
round_of_16_participants as (
    select distinct
        tie.season_label,
        team.team_id
    from {{ ref('fact_tie_results') }} tie
    inner join {{ ref('dim_stage') }} st
      on st.stage_id = tie.stage_id
     and st.provider = tie.provider
     and st.stage_code = 'round_of_16'
    cross join lateral (
        values
            (tie.home_side_team_id),
            (tie.away_side_team_id)
    ) as team(team_id)
    where tie.competition_key = 'libertadores'
      and tie.season_label in ('2024', '2025')
)
select
    gna.season_label,
    gna.team_id,
    gna.progression_type
from group_non_advancers gna
inner join round_of_16_participants r16
  on r16.season_label = gna.season_label
 and r16.team_id = gna.team_id
