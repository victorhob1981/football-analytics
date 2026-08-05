{{ config(materialized='table', schema='serving_v2') }}

with usage as (
  select player_key, match_id, team_id from {{ source('mart_v2', 'fact_match_player_stats') }}
  union all
  select player_key, match_id, team_id from {{ source('mart_v2', 'fact_lineup') }}
  union all
  select player_key, match_id, team_id from {{ source('mart_v2', 'fact_match_event') }}
  where player_key is not null
), aggregates as (
  select player_key, count(distinct u.match_id) as match_count,
         count(distinct u.team_id) filter (where u.team_id is not null) as team_count,
         min(f.match_date) as first_match_date,
         max(f.match_date) as last_match_date
  from usage u
  join {{ source('mart_v2', 'fact_match') }} f using (match_id)
  where f.publication_state = 'published'
  group by player_key
)
select
  p.player_key, p.display_name, p.nationality, p.date_of_birth,
  p.position_name, p.preferred_foot, coalesce(a.match_count, 0) as match_count,
  coalesce(a.team_count, 0) as team_count, a.first_match_date,
  a.last_match_date, p.metadata,
  {{ var('rebuild_run_id') }}::bigint as rebuild_run_id
from {{ source('mart_v2', 'dim_player') }} p
join aggregates a using (player_key)
