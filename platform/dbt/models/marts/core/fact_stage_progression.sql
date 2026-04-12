with structure_rules as (
    select * from {{ ref('competition_structure_hub') }}
),
final_group_rounds as (
    select
        provider,
        competition_key,
        season_label,
        stage_id,
        group_id,
        max(round_key) as final_round_key
    from {{ ref('fact_group_standings') }}
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
        fgs.stage_sk,
        fgs.group_id,
        fgs.group_name,
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
final_table_rounds as (
    select
        provider,
        competition_key,
        season_label,
        stage_id,
        max(round_key) as final_round_key
    from {{ ref('fact_standings_snapshots') }}
    where group_id is null
    group by
        provider,
        competition_key,
        season_label,
        stage_id
),
final_table_standings as (
    select
        fs.provider,
        fs.competition_key,
        fs.season_label,
        fs.stage_id,
        fs.stage_sk,
        fs.team_id,
        fs.position
    from {{ ref('fact_standings_snapshots') }} fs
    inner join final_table_rounds fr
      on fr.provider = fs.provider
     and fr.competition_key = fs.competition_key
     and fr.season_label = fs.season_label
     and fr.stage_id = fs.stage_id
     and fr.final_round_key = fs.round_key
    where fs.group_id is null
),
group_progressions as (
    select
        groups.provider,
        groups.competition_key,
        groups.season_label,
        groups.team_id,
        rules.from_stage_id,
        rules.from_stage_sk,
        rules.from_stage_name,
        rules.from_stage_format,
        rules.from_stage_order,
        rules.to_stage_id,
        rules.to_stage_sk,
        rules.to_stage_name,
        rules.to_stage_format,
        rules.to_stage_order,
        rules.progression_type,
        rules.progression_scope,
        groups.position as source_position,
        cast(null as text) as tie_outcome,
        groups.group_id,
        groups.group_name,
        true as is_inferred
    from final_group_table groups
    inner join structure_rules rules
      on rules.provider = groups.provider
     and rules.competition_key = groups.competition_key
     and rules.season_label = groups.season_label
     and rules.from_stage_id = groups.stage_id
     and rules.progression_scope = 'group_position'
     and groups.position between rules.position_from and rules.position_to
),
table_progressions as (
    select
        tables.provider,
        tables.competition_key,
        tables.season_label,
        tables.team_id,
        rules.from_stage_id,
        rules.from_stage_sk,
        rules.from_stage_name,
        rules.from_stage_format,
        rules.from_stage_order,
        rules.to_stage_id,
        rules.to_stage_sk,
        rules.to_stage_name,
        rules.to_stage_format,
        rules.to_stage_order,
        rules.progression_type,
        rules.progression_scope,
        tables.position as source_position,
        cast(null as text) as tie_outcome,
        cast(null as text) as group_id,
        cast(null as text) as group_name,
        true as is_inferred
    from final_table_standings tables
    inner join structure_rules rules
      on rules.provider = tables.provider
     and rules.competition_key = tables.competition_key
     and rules.season_label = tables.season_label
     and rules.from_stage_id = tables.stage_id
     and rules.progression_scope = 'table_position'
     and tables.position between rules.position_from and rules.position_to
),
tie_participants as (
    select
        tie.provider,
        tie.competition_key,
        tie.season_label,
        tie.stage_id as from_stage_id,
        tie.stage_sk as from_stage_sk,
        tie.stage_name as from_stage_name,
        tie.stage_format as from_stage_format,
        tie.tie_id,
        tie.home_side_team_id as team_id,
        case
            when tie.winner_team_id = tie.home_side_team_id then 'winner'
            else 'loser'
        end as tie_outcome
    from {{ ref('fact_tie_results') }} tie

    union all

    select
        tie.provider,
        tie.competition_key,
        tie.season_label,
        tie.stage_id as from_stage_id,
        tie.stage_sk as from_stage_sk,
        tie.stage_name as from_stage_name,
        tie.stage_format as from_stage_format,
        tie.tie_id,
        tie.away_side_team_id as team_id,
        case
            when tie.winner_team_id = tie.away_side_team_id then 'winner'
            else 'loser'
        end as tie_outcome
    from {{ ref('fact_tie_results') }} tie
),
tie_progressions as (
    select
        participants.provider,
        participants.competition_key,
        participants.season_label,
        participants.team_id,
        rules.from_stage_id,
        rules.from_stage_sk,
        rules.from_stage_name,
        rules.from_stage_format,
        rules.from_stage_order,
        rules.to_stage_id,
        rules.to_stage_sk,
        rules.to_stage_name,
        rules.to_stage_format,
        rules.to_stage_order,
        rules.progression_type,
        rules.progression_scope,
        cast(null as int) as source_position,
        participants.tie_outcome,
        cast(null as text) as group_id,
        cast(null as text) as group_name,
        true as is_inferred
    from tie_participants participants
    inner join structure_rules rules
      on rules.provider = participants.provider
     and rules.competition_key = participants.competition_key
     and rules.season_label = participants.season_label
     and rules.from_stage_id = participants.from_stage_id
     and rules.progression_scope = 'tie_outcome'
     and rules.tie_outcome = participants.tie_outcome
),
unioned as (
    select * from group_progressions
    union all
    select * from table_progressions
    union all
    select * from tie_progressions
)
select
    md5(
        concat_ws(
            '||',
            provider,
            competition_key,
            season_label,
            team_id::text,
            from_stage_id::text,
            coalesce(to_stage_id::text, 'terminal'),
            progression_scope,
            progression_type,
            coalesce(source_position::text, 'na'),
            coalesce(tie_outcome, 'na')
        )
    ) as stage_progression_id,
    provider,
    competition_key,
    season_label,
    team_id,
    from_stage_id,
    from_stage_sk,
    from_stage_name,
    from_stage_format,
    from_stage_order,
    to_stage_id,
    to_stage_sk,
    to_stage_name,
    to_stage_format,
    to_stage_order,
    progression_scope,
    progression_type,
    source_position,
    tie_outcome,
    group_id,
    group_name,
    is_inferred,
    now() as updated_at
from unioned
