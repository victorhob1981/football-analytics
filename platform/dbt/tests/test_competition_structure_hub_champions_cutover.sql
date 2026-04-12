with expected as (
    select *
    from (
        values
            ('2023_24', 'group_stage', 'round_of_16'),
            ('2024_25', 'league_stage', 'round_of_16'),
            ('2024_25', 'league_stage', 'knockout_round_play_offs'),
            ('2024_25', 'knockout_round_play_offs', 'round_of_16')
    ) as seeded (
        season_label,
        from_stage_code,
        to_stage_code
    )
),
actual as (
    select
        season_label,
        from_stage_code,
        to_stage_code
    from {{ ref('competition_structure_hub') }}
    where competition_key = 'champions_league'
      and season_label in ('2023_24', '2024_25')
      and progression_type = 'qualified'
)
select
    e.season_label,
    e.from_stage_code,
    e.to_stage_code
from expected e
left join actual a
  on a.season_label = e.season_label
 and a.from_stage_code = e.from_stage_code
 and a.to_stage_code = e.to_stage_code
where a.season_label is null
