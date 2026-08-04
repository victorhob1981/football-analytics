{{ config(materialized='table', schema='serving_v2') }}

select
  competition_key,
  case when is_world_cup then 'world_cup_special' else 'standard' end as presentation_mode,
  case when is_world_cup then '/copa-do-mundo' else '/competitions/' || competition_key end as route_prefix,
  true as shares_core,
  jsonb_build_object('core_schema', 'mart_v2', 'serving_schema', 'serving_v2') as metadata,
  {{ var('rebuild_run_id') }}::bigint as rebuild_run_id
from {{ ref('competition_catalog') }}
