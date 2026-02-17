{{ config(materialized='incremental', unique_key='venue_sk', on_schema_change='sync_all_columns') }}

with fixtures as (
    select * from {{ ref('stg_matches') }}
)
select distinct
    md5(concat('venue:', venue_id::text)) as venue_sk,
    venue_id,
    venue_name,
    venue_city,
    now() as updated_at
from fixtures
where venue_id is not null
  and venue_name is not null
