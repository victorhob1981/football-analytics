with snapshots as (
    select * from {{ ref('stg_standings_snapshots') }}
),
stages as (
    select
        provider,
        stage_id,
        stage_sk,
        stage_format
    from {{ ref('dim_stage') }}
),
seeded_memberships as (
    select
        s.provider,
        s.competition_key,
        s.season_label,
        s.provider_season_id,
        s.season_id,
        s.stage_id,
        st.stage_sk,
        s.team_id,
        s.raw_group_id as source_group_id
    from snapshots s
    inner join stages st
      on st.provider = s.provider
     and st.stage_id = s.stage_id
    where st.stage_format = 'group_table'
      and s.raw_group_id is not null
      and s.competition_key is not null
      and s.season_label is not null
),
distinct_memberships as (
    select distinct
        provider,
        competition_key,
        season_label,
        provider_season_id,
        season_id,
        stage_id,
        stage_sk,
        team_id,
        source_group_id
    from seeded_memberships
),
ordered_groups as (
    select
        provider,
        competition_key,
        season_label,
        provider_season_id,
        season_id,
        stage_id,
        stage_sk,
        team_id,
        source_group_id,
        dense_rank() over (
            partition by provider, competition_key, season_label, stage_id
            order by
                case
                    when source_group_id ~ '^[0-9]+$' then source_group_id::bigint
                    else null
                end nulls last,
                source_group_id
        ) as group_order
    from distinct_memberships
)
select
    md5(
        concat(
            provider,
            ':group:',
            competition_key,
            ':',
            season_label,
            ':',
            stage_id::text,
            ':',
            source_group_id
        )
    ) as group_id,
    md5(
        concat(
            provider,
            ':group-sk:',
            competition_key,
            ':',
            season_label,
            ':',
            stage_id::text,
            ':',
            source_group_id
        )
    ) as group_sk,
    provider,
    competition_key,
    season_label,
    provider_season_id,
    season_id,
    stage_id,
    stage_sk,
    team_id,
    source_group_id,
    case
        when group_order between 1 and 26
            then concat('Group ', chr((64 + group_order)::int))
        else concat('Group ', group_order::text)
    end as group_name,
    group_order,
    true as is_inferred
from ordered_groups
