with duplicates as (
    select
        fixture_id,
        count(*) as row_count
    from {{ source('postgres_raw', 'fixtures') }}
    group by fixture_id
    having count(*) > 1
),
nulls as (
    select fixture_id
    from {{ source('postgres_raw', 'fixtures') }}
    where fixture_id is null
)
select *
from duplicates

union all

select
    fixture_id,
    1 as row_count
from nulls
