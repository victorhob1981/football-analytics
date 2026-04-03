with expected as (
    select *
    from (
        values
            ('2023_24', 'group_stage', 'round_of_16', 'qualified', 16),
            ('2024_25', 'league_stage', 'round_of_16', 'qualified', 8),
            ('2024_25', 'league_stage', 'knockout_round_play_offs', 'qualified', 16),
            ('2024_25', 'league_stage', null, 'eliminated', 12)
    ) as seeded (
        season_label,
        from_stage_code,
        to_stage_code,
        progression_type,
        expected_rows
    )
),
actual as (
    select
        fsp.season_label,
        from_stage.stage_code as from_stage_code,
        to_stage.stage_code as to_stage_code,
        fsp.progression_type,
        count(*) as actual_rows
    from {{ ref('fact_stage_progression') }} fsp
    inner join {{ ref('dim_stage') }} from_stage
      on from_stage.stage_id = fsp.from_stage_id
     and from_stage.provider = fsp.provider
    left join {{ ref('dim_stage') }} to_stage
      on to_stage.stage_id = fsp.to_stage_id
     and to_stage.provider = fsp.provider
    where fsp.competition_key = 'champions_league'
      and fsp.season_label in ('2023_24', '2024_25')
    group by
        fsp.season_label,
        from_stage.stage_code,
        to_stage.stage_code,
        fsp.progression_type
)
select
    e.season_label,
    e.from_stage_code,
    e.to_stage_code,
    e.progression_type,
    e.expected_rows,
    a.actual_rows
from expected e
left join actual a
  on a.season_label = e.season_label
 and a.from_stage_code = e.from_stage_code
 and a.to_stage_code is not distinct from e.to_stage_code
 and a.progression_type = e.progression_type
where coalesce(a.actual_rows, -1) <> e.expected_rows
