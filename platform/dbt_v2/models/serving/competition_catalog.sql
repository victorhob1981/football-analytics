{{ config(materialized='table', schema='serving_v2') }}

select
  c.competition_key,
  c.competition_name,
  c.competition_type,
  c.country_name,
  c.confederation_name,
  c.is_international,
  c.is_world_cup,
  count(e.edition_key) as edition_count,
  count(*) filter (where e.publication_state = 'published' and e.published_match_count > 0) as selectable_edition_count,
  coalesce(sum(e.published_match_count), 0) as published_match_count,
  min(e.first_match_date) as first_match_date,
  max(e.last_match_date) as last_match_date,
  coalesce(bool_or(e.publication_state = 'published' and e.published_match_count > 0), false) as is_selectable,
  case when c.competition_key = 'fifa_world_cup_mens' then '/copa-do-mundo' else '/competitions/' || c.competition_key end as href,
  c.metadata || jsonb_build_object('catalog_source', 'mart_v2.dim_competition') as metadata,
  {{ var('rebuild_run_id') }}::bigint as rebuild_run_id
from {{ source('mart_v2', 'dim_competition') }} c
left join {{ source('mart_v2', 'dim_edition') }} e using (competition_key)
group by c.competition_key, c.competition_name, c.competition_type,
         c.country_name, c.confederation_name, c.is_international,
         c.is_world_cup, c.metadata
