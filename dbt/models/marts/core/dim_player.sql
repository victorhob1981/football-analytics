{{ config(materialized='incremental', unique_key='player_sk', on_schema_change='sync_all_columns') }}

with events as (
    select * from {{ ref('stg_match_events') }}
),
players as (
    select distinct
        player_id,
        player_name
    from events
    where player_id is not null
      and player_name is not null
)
select
    md5(concat('player:', player_id::text)) as player_sk,
    player_id,
    player_name,
    now() as updated_at
from players
