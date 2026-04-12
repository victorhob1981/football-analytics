with expected as (
    select *
    from (
        values
            ('2024', '1st_round', '2nd_round', 1, 2),
            ('2024', '2nd_round', '3rd_round', 2, 3),
            ('2024', '3rd_round', 'group_stage', 3, 4),
            ('2024', 'group_stage', 'round_of_16', 4, 5),
            ('2024', 'round_of_16', 'quarter_finals', 5, 6),
            ('2024', 'quarter_finals', 'semi_finals', 6, 7),
            ('2024', 'semi_finals', 'final', 7, 8),
            ('2025', '1st_round', '2nd_round', 1, 2),
            ('2025', '2nd_round', '3rd_round', 2, 3),
            ('2025', '3rd_round', 'group_stage', 3, 4),
            ('2025', 'group_stage', 'round_of_16', 4, 5),
            ('2025', 'round_of_16', 'quarter_finals', 5, 6),
            ('2025', 'quarter_finals', 'semi_finals', 6, 7),
            ('2025', 'semi_finals', 'final', 7, 8)
    ) as seeded (
        season_label,
        from_stage_code,
        to_stage_code,
        from_stage_order,
        to_stage_order
    )
),
actual as (
    select
        season_label,
        from_stage_code,
        to_stage_code,
        from_stage_order,
        to_stage_order
    from {{ ref('competition_structure_hub') }}
    where competition_key = 'libertadores'
      and season_label in ('2024', '2025')
      and progression_type = 'qualified'
      and to_stage_code is not null
)
select
    coalesce(e.season_label, a.season_label) as season_label,
    coalesce(e.from_stage_code, a.from_stage_code) as from_stage_code,
    coalesce(e.to_stage_code, a.to_stage_code) as to_stage_code,
    e.from_stage_order as expected_from_stage_order,
    a.from_stage_order as actual_from_stage_order,
    e.to_stage_order as expected_to_stage_order,
    a.to_stage_order as actual_to_stage_order
from expected e
full outer join actual a
  on a.season_label = e.season_label
 and a.from_stage_code = e.from_stage_code
 and a.to_stage_code = e.to_stage_code
where a.season_label is null
   or e.season_label is null
   or a.from_stage_order <> e.from_stage_order
   or a.to_stage_order <> e.to_stage_order
