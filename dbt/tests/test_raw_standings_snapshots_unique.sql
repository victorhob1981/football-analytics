with duplicates as (
    select
        provider,
        season_id,
        stage_id,
        round_id,
        team_id,
        count(*) as row_count
    from {{ source('postgres_raw', 'standings_snapshots') }}
    group by provider, season_id, stage_id, round_id, team_id
    having count(*) > 1
),
nulls as (
    select
        provider,
        season_id,
        stage_id,
        round_id,
        team_id
    from {{ source('postgres_raw', 'standings_snapshots') }}
    where provider is null
       or season_id is null
       or stage_id is null
       or round_id is null
       or team_id is null
)
select *
from duplicates

union all

select
    provider,
    season_id,
    stage_id,
    round_id,
    team_id,
    1 as row_count
from nulls
