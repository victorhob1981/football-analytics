{{ config(
  materialized='table',
  schema='serving_v2',
  post_hook=[
    "create index if not exists search_document_trgm_idx on {{ this }} using gin (search_text gin_trgm_ops)",
    "create index if not exists search_document_type_idx on {{ this }} (entity_type, publication_state)"
  ]
) }}

select
  'competition:' || competition_key as document_id, 'competition' as entity_type,
  competition_key as entity_id, competition_name as label,
  coalesce(country_name, competition_type) as subtitle,
  lower(unaccent(competition_name || ' ' || coalesce(country_name, '') || ' ' || competition_key)) as search_text,
  href, competition_key, null::text as edition_key, 'published' as publication_state,
  metadata, {{ var('rebuild_run_id') }}::bigint as rebuild_run_id
from {{ ref('competition_catalog') }}
where is_selectable
union all
select
  'edition:' || edition_key, 'edition', edition_key,
  competition_name || ' — ' || season_label,
  season_label || ' · ' || published_match_count || ' partidas',
  lower(unaccent(competition_name || ' ' || season_label || ' ' || edition_key)),
  href, competition_key, edition_key, 'published', metadata,
  {{ var('rebuild_run_id') }}::bigint
from {{ ref('edition_catalog') }}
where is_selectable
union all
select
  'team:' || team_id::text, 'team', team_id::text, team_name,
  coalesce(country_or_territory, team_type),
  lower(unaccent(team_name || ' ' || coalesce(country_or_territory, '') || ' ' || team_id::text)),
  '/clubs/' || team_id::text, null::text, null::text, 'published',
  metadata || jsonb_build_object('team_type', team_type, 'asset_url', asset_url, 'asset_type', asset_type),
  {{ var('rebuild_run_id') }}::bigint
from {{ ref('team_profile') }}
where match_count > 0
union all
select
  'player:' || player_key, 'player', player_key,
  coalesce(display_name, 'Jogador sem nome'), coalesce(position_name, nationality),
  lower(unaccent(coalesce(display_name, '') || ' ' || coalesce(nationality, '') || ' ' || player_key)),
  '/players/' || player_key, null::text, null::text, 'published',
  metadata || jsonb_build_object('position_name', position_name, 'nationality', nationality),
  {{ var('rebuild_run_id') }}::bigint
from {{ ref('player_profile') }}
where match_count > 0
union all
select
  'match:' || match_id::text, 'match', match_id::text,
  home_team_name || ' x ' || away_team_name,
  competition_name || ' · ' || season_label || ' · ' || match_date::text,
  lower(unaccent(home_team_name || ' ' || away_team_name || ' ' || competition_name || ' ' || season_label || ' ' || match_id::text)),
  href, competition_key, edition_key, 'published',
  metadata || jsonb_build_object(
    'home_team_id', home_team_id, 'home_team_name', home_team_name,
    'away_team_id', away_team_id, 'away_team_name', away_team_name,
    'home_goals', home_goals, 'away_goals', away_goals, 'match_date', match_date
  ),
  {{ var('rebuild_run_id') }}::bigint
from {{ ref('match_catalog') }}
