{{ config(
  materialized='table',
  schema='serving_v2',
  post_hook=[
    "create index if not exists team_profile_name_trgm_idx on {{ this }} using gin (lower(team_name) gin_trgm_ops)"
  ]
) }}

with appearances as (
  select home_team_id as team_id, match_id, competition_key, edition_key, match_date
  from {{ source('mart_v2', 'fact_match') }}
  where publication_state = 'published'
  union all
  select away_team_id, match_id, competition_key, edition_key, match_date
  from {{ source('mart_v2', 'fact_match') }}
  where publication_state = 'published'
), aggregates as (
  select team_id, count(distinct match_id) as match_count,
         count(distinct competition_key) as competition_count,
         count(distinct edition_key) as edition_count,
         min(match_date) as first_match_date,
         max(match_date) as last_match_date
  from appearances
  group by team_id
), assets as (
  select distinct on (team_id) team_id, asset_url, asset_type
  from {{ source('mart_v2', 'team_asset') }}
  order by team_id, asset_type, asset_key
)
select
  t.team_id, t.team_name, t.country_or_territory, t.team_type, t.gender,
  t.category, t.identity_state, coalesce(a.match_count, 0) as match_count,
  coalesce(a.competition_count, 0) as competition_count,
  coalesce(a.edition_count, 0) as edition_count,
  a.first_match_date, a.last_match_date, x.asset_url, x.asset_type,
  t.metadata || jsonb_build_object('public_id_preserved', t.public_id_preserved) as metadata,
  {{ var('rebuild_run_id') }}::bigint as rebuild_run_id
from {{ source('mart_v2', 'dim_team') }} t
left join aggregates a using (team_id)
left join assets x using (team_id)
where t.is_active
