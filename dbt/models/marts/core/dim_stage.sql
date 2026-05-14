with stages as (
    select * from {{ ref('stg_competition_stages') }}
),
deduped as (
    select
        provider,
        stage_id,
        league_id,
        season_id,
        stage_name,
        sort_order,
        finished,
        is_current,
        starting_at,
        ending_at,
        row_number() over (
            partition by provider, stage_id
            order by updated_at desc nulls last, ingested_run desc nulls last
        ) as row_num
    from stages
    where stage_id is not null
)
select
    md5(concat(provider, ':stage:', stage_id::text)) as stage_sk,
    provider,
    stage_id,
    league_id,
    season_id,
    stage_name,
    sort_order,
    finished,
    is_current,
    starting_at,
    ending_at,
    now() as updated_at
from deduped
where row_num = 1
