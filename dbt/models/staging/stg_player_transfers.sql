with source_transfers as (
    select * from {{ source('postgres_raw', 'player_transfers') }}
)
select
    provider,
    transfer_id,
    player_id,
    nullif(trim(coalesce(payload -> 'player' ->> 'name', payload ->> 'player_name')), '') as player_name,
    from_team_id,
    to_team_id,
    transfer_date,
    completed,
    career_ended,
    type_id,
    position_id,
    amount,
    payload,
    ingested_run,
    updated_at
from source_transfers
