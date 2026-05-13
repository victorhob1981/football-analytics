{{ config(materialized='incremental', unique_key='competition_sk', on_schema_change='sync_all_columns') }}

with fixtures as (
    select * from {{ ref('stg_matches') }}
)
select distinct
    md5(concat('competition:', league_id::text)) as competition_sk,
    league_id,
    league_name,
    cast(null as text) as country,
    now() as updated_at
from fixtures
where league_id is not null
  and league_name is not null
