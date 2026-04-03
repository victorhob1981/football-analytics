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
final_group_table as (
    select
        fgs.provider,
        fgs.competition_key,
        fgs.season_label,
        fgs.stage_id,
        fgs.team_id,
        fgs.position
    from {{ ref('fact_group_standings') }} fgs
    inner join final_group_rounds fr
      on fr.provider = fgs.provider
     and fr.competition_key = fgs.competition_key
     and fr.season_label = fgs.season_label
     and fr.stage_id = fgs.stage_id
     and fr.group_id = fgs.group_id
     and fr.final_round_key = fgs.round_key
),
progressions as (
    select
        provider,
        competition_key,
        season_label,
        from_stage_id,
        team_id,
        progression_type
    from {{ ref('fact_stage_progression') }}
    where competition_key = 'libertadores'
      and season_label in ('2024', '2025')
      and progression_scope = 'group_position'
),
expected as (
    select
        provider,
        competition_key,
        season_label,
        stage_id as from_stage_id,
        team_id,
        case
            when position in (1, 2) then 'qualified'
            when position = 3 then 'repechage'
            when position = 4 then 'eliminated'
            else null
        end as expected_progression_type
    from final_group_table
)
select
    e.provider,
    e.competition_key,
    e.season_label,
    e.from_stage_id,
    e.team_id,
    e.expected_progression_type,
    p.progression_type
from expected e
left join progressions p
  on p.provider = e.provider
 and p.competition_key = e.competition_key
 and p.season_label = e.season_label
 and p.from_stage_id = e.from_stage_id
 and p.team_id = e.team_id
where p.progression_type is distinct from e.expected_progression_type
