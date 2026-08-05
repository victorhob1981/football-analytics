{{ config(materialized='table', schema='serving_v2') }}

select 'published_matches' as metric_key, count(*)::bigint as metric_value,
       '{}'::jsonb as metadata, {{ var('rebuild_run_id') }}::bigint as rebuild_run_id
from {{ ref('match_catalog') }}
union all
select 'selectable_editions', count(*)::bigint, '{}'::jsonb, {{ var('rebuild_run_id') }}::bigint
from {{ ref('edition_catalog') }} where is_selectable
union all
select 'search_documents', count(*)::bigint, '{}'::jsonb, {{ var('rebuild_run_id') }}::bigint
from {{ ref('search_document') }}
union all
select 'teams_with_published_matches', count(*)::bigint, '{}'::jsonb, {{ var('rebuild_run_id') }}::bigint
from {{ ref('team_profile') }} where match_count > 0
union all
select 'players_with_published_matches', count(*)::bigint, '{}'::jsonb, {{ var('rebuild_run_id') }}::bigint
from {{ ref('player_profile') }} where match_count > 0
