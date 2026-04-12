with memberships as (
    select * from {{ ref('int_group_memberships') }}
),
deduped as (
    select distinct
        group_id,
        group_sk,
        provider,
        competition_key,
        season_label,
        provider_season_id,
        season_id,
        stage_id,
        stage_sk,
        source_group_id,
        group_name,
        group_order,
        is_inferred
    from memberships
)
select
    group_id,
    group_sk,
    provider,
    competition_key,
    season_label,
    provider_season_id,
    season_id,
    stage_id,
    stage_sk,
    source_group_id,
    group_name,
    group_order,
    is_inferred,
    now() as updated_at
from deduped
