{{ config(materialized='table', schema='serving_v2') }}

select
  e.edition_key,
  e.competition_key,
  c.competition_name,
  e.season_label,
  e.season_start_date,
  e.season_end_date,
  e.is_closed,
  e.publication_state,
  e.observed_source_count,
  e.published_match_count,
  e.first_match_date,
  e.last_match_date,
  e.publication_state = 'published' and e.published_match_count > 0 as is_selectable,
  case
    when e.competition_key = 'fifa_world_cup_mens'
      then '/copa-do-mundo/' || replace(e.season_label, '/', '_')
    else '/competitions/' || e.competition_key || '/seasons/' || replace(e.season_label, '/', '_')
  end as href,
  e.metadata || jsonb_build_object(
    'catalog_source', 'mart_v2.dim_edition',
    'empty_context_hidden_from_default_filters', e.published_match_count = 0
  ) as metadata,
  {{ var('rebuild_run_id') }}::bigint as rebuild_run_id
from {{ source('mart_v2', 'dim_edition') }} e
join {{ ref('competition_catalog') }} c using (competition_key)
