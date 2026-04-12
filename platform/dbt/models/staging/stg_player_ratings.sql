with player_stats as (
    select * from {{ ref('stg_fixture_player_statistics') }}
)
select
    provider,
    fixture_id,
    team_id,
    player_id,
    player_name,
    rating,
    updated_at
from player_stats
where rating is not null
