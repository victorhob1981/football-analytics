with group_qualifiers as (
    select
        season_label,
        team_id
    from {{ ref('fact_stage_progression') }}
    where competition_key = 'libertadores'
      and season_label in ('2024', '2025')
      and progression_scope = 'group_position'
      and progression_type = 'qualified'
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
    coalesce(gq.season_label, r16.season_label) as season_label,
    coalesce(gq.team_id, r16.team_id) as team_id
from group_qualifiers gq
full outer join round_of_16_participants r16
  on r16.season_label = gq.season_label
 and r16.team_id = gq.team_id
where gq.team_id is null
   or r16.team_id is null
