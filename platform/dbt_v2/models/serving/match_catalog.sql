{{ config(
  materialized='table',
  schema='serving_v2',
  post_hook=[
    "create index if not exists match_catalog_global_date_idx on {{ this }} (match_date desc, match_id desc)",
    "create index if not exists match_catalog_date_idx on {{ this }} (competition_key, season_label, match_date desc, match_id desc)",
    "create index if not exists match_catalog_team_date_idx on {{ this }} (home_team_id, match_date desc, match_id desc)",
    "create index if not exists match_catalog_away_date_idx on {{ this }} (away_team_id, match_date desc, match_id desc)"
  ]
) }}

select
  f.match_id, f.match_date, f.competition_key, c.competition_name,
  f.edition_key, e.season_label, s.stage_name, r.round_name,
  f.home_team_id, ht.team_name as home_team_name, f.away_team_id,
  at.team_name as away_team_name, f.home_goals, f.away_goals,
  coalesce(f.status_short, f.status_long) as status, c.is_world_cup,
  '/matches/' || f.match_id::text as href,
  f.metadata || jsonb_build_object('publication_state', f.publication_state) as metadata,
  {{ var('rebuild_run_id') }}::bigint as rebuild_run_id
from {{ source('mart_v2', 'fact_match') }} f
join {{ source('mart_v2', 'dim_competition') }} c using (competition_key)
join {{ source('mart_v2', 'dim_edition') }} e using (edition_key)
join {{ source('mart_v2', 'dim_team') }} ht on ht.team_id = f.home_team_id
join {{ source('mart_v2', 'dim_team') }} at on at.team_id = f.away_team_id
left join {{ source('mart_v2', 'dim_stage') }} s on s.stage_key = f.stage_key
left join {{ source('mart_v2', 'dim_round') }} r on r.round_key = f.round_key
where f.publication_state = 'published'
