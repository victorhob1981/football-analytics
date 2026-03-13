with duplicates as (
    select
        provider,
        season,
        fixture_id,
        event_id,
        count(*) as row_count
    from {{ source('postgres_raw', 'match_events') }}
    group by provider, season, fixture_id, event_id
    having count(*) > 1
),
nulls as (
    select
        provider,
        season,
        fixture_id,
        event_id
    from {{ source('postgres_raw', 'match_events') }}
    where provider is null
       or season is null
       or fixture_id is null
       or event_id is null
)
select *
from duplicates

union all

select
    provider,
    season,
    fixture_id,
    event_id,
    1 as row_count
from nulls
