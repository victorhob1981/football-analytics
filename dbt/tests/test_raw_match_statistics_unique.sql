with duplicates as (
    select
        fixture_id,
        team_id,
        count(*) as row_count
    from {{ source('postgres_raw', 'match_statistics') }}
    group by fixture_id, team_id
    having count(*) > 1
),
nulls as (
    select
        fixture_id,
        team_id
    from {{ source('postgres_raw', 'match_statistics') }}
    where fixture_id is null
       or team_id is null
)
select *
from duplicates

union all

select
    fixture_id,
    team_id,
    1 as row_count
from nulls
