{{ config(materialized='incremental', unique_key='season_sk', on_schema_change='sync_all_columns') }}

with catalog as (
    select * from {{ source('control_plane', 'season_catalog') }}
),
observed as (
    select distinct
        provider,
        competition_key,
        season_label,
        provider_season_id,
        season_start_date,
        season_end_date,
        season,
        season_name
    from {{ ref('stg_matches') }}
    where provider is not null
      and competition_key is not null
      and season_label is not null
),
resolved as (
    select
        coalesce(o.provider, c.provider) as provider,
        coalesce(o.competition_key, c.competition_key) as competition_key,
        coalesce(o.season_label, c.season_label) as season_label,
        coalesce(o.provider_season_id, c.provider_season_id) as provider_season_id,
        coalesce(o.season_start_date, c.season_start_date) as season_start_date,
        coalesce(o.season_end_date, c.season_end_date) as season_end_date,
        case
            when c.is_closed is not null then c.is_closed
            when coalesce(o.season_end_date, c.season_end_date) < current_date then true
            else false
        end as is_closed,
        coalesce(o.season, nullif(split_part(coalesce(o.season_label, c.season_label), '_', 1), '')::int) as season_year,
        o.season_name
    from catalog c
    full outer join observed o
      on o.provider = c.provider
     and o.competition_key = c.competition_key
     and o.season_label = c.season_label
)
select distinct
    md5(concat(provider, ':season:', competition_key, ':', season_label)) as season_sk,
    provider,
    competition_key,
    season_label,
    provider_season_id,
    season_start_date,
    season_end_date,
    is_closed,
    season_year,
    season_name,
    now() as updated_at
from resolved
where provider is not null
  and competition_key is not null
  and season_label is not null
