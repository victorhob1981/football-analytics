\set ON_ERROR_STOP on
\if :{?rebuild_run_key}
\else
  \set rebuild_run_key 'mart-v2-local-current'
\endif

SELECT rebuild_run_id
FROM control.rebuild_run
WHERE run_key = :'rebuild_run_key'\gset rebuild_

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

DROP SCHEMA IF EXISTS serving_v2 CASCADE;
CREATE SCHEMA serving_v2;

CREATE TABLE serving_v2.competition_catalog (
  competition_key        text PRIMARY KEY REFERENCES mart_v2.dim_competition(competition_key),
  competition_name       text NOT NULL,
  competition_type       text,
  country_name           text,
  confederation_name     text,
  is_international       boolean,
  is_world_cup           boolean NOT NULL DEFAULT false,
  edition_count          bigint NOT NULL DEFAULT 0,
  selectable_edition_count bigint NOT NULL DEFAULT 0,
  published_match_count  bigint NOT NULL DEFAULT 0,
  first_match_date       date,
  last_match_date        date,
  is_selectable          boolean NOT NULL DEFAULT false,
  href                   text NOT NULL,
  metadata               jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id         bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE serving_v2.edition_catalog (
  edition_key            text PRIMARY KEY REFERENCES mart_v2.dim_edition(edition_key),
  competition_key        text NOT NULL REFERENCES serving_v2.competition_catalog(competition_key),
  competition_name       text NOT NULL,
  season_label           text NOT NULL,
  season_start_date      date,
  season_end_date        date,
  is_closed              boolean,
  publication_state      text NOT NULL,
  observed_source_count  bigint NOT NULL DEFAULT 0,
  published_match_count  bigint NOT NULL DEFAULT 0,
  first_match_date       date,
  last_match_date        date,
  is_selectable          boolean NOT NULL DEFAULT false,
  href                   text NOT NULL,
  metadata               jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id         bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE INDEX edition_catalog_competition_idx
  ON serving_v2.edition_catalog (competition_key, is_selectable, season_label DESC);

CREATE TABLE serving_v2.team_profile (
  team_id                bigint PRIMARY KEY REFERENCES mart_v2.dim_team(team_id),
  team_name              text NOT NULL,
  country_or_territory   text,
  team_type              text,
  gender                 text,
  category               text,
  identity_state         text NOT NULL,
  match_count            bigint NOT NULL DEFAULT 0,
  competition_count      bigint NOT NULL DEFAULT 0,
  edition_count          bigint NOT NULL DEFAULT 0,
  first_match_date       date,
  last_match_date        date,
  asset_url              text,
  asset_type             text,
  metadata               jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id         bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE INDEX team_profile_name_trgm_idx
  ON serving_v2.team_profile USING gin (lower(team_name) gin_trgm_ops);

CREATE TABLE serving_v2.player_profile (
  player_key             text PRIMARY KEY REFERENCES mart_v2.dim_player(player_key),
  display_name           text,
  nationality             text,
  date_of_birth           date,
  position_name           text,
  preferred_foot          text,
  match_count             bigint NOT NULL DEFAULT 0,
  team_count              bigint NOT NULL DEFAULT 0,
  first_match_date        date,
  last_match_date         date,
  metadata                jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id          bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE INDEX player_profile_name_trgm_idx
  ON serving_v2.player_profile USING gin (lower(coalesce(display_name, '')) gin_trgm_ops);

CREATE TABLE serving_v2.match_catalog (
  match_id               bigint PRIMARY KEY REFERENCES mart_v2.fact_match(match_id),
  match_date              date NOT NULL,
  competition_key         text NOT NULL,
  competition_name        text NOT NULL,
  edition_key             text NOT NULL,
  season_label            text NOT NULL,
  stage_name              text,
  round_name              text,
  home_team_id            bigint NOT NULL,
  home_team_name          text NOT NULL,
  away_team_id            bigint NOT NULL,
  away_team_name          text NOT NULL,
  home_goals              integer,
  away_goals              integer,
  status                   text,
  is_world_cup             boolean NOT NULL DEFAULT false,
  href                    text NOT NULL,
  metadata                jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id          bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE INDEX match_catalog_date_idx
  ON serving_v2.match_catalog (competition_key, season_label, match_date DESC, match_id DESC);
CREATE INDEX match_catalog_global_date_idx
  ON serving_v2.match_catalog (match_date DESC, match_id DESC);
CREATE INDEX match_catalog_team_date_idx
  ON serving_v2.match_catalog (home_team_id, match_date DESC, match_id DESC);
CREATE INDEX match_catalog_away_date_idx
  ON serving_v2.match_catalog (away_team_id, match_date DESC, match_id DESC);

CREATE TABLE serving_v2.search_document (
  document_id            text PRIMARY KEY,
  entity_type            text NOT NULL CHECK (entity_type IN ('competition', 'edition', 'team', 'player', 'match')),
  entity_id              text NOT NULL,
  label                  text NOT NULL,
  subtitle               text,
  search_text            text NOT NULL,
  href                   text NOT NULL,
  competition_key        text,
  edition_key            text,
  publication_state      text NOT NULL DEFAULT 'published',
  metadata               jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id         bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  UNIQUE (entity_type, entity_id)
);

CREATE INDEX search_document_trgm_idx
  ON serving_v2.search_document USING gin (search_text gin_trgm_ops);
CREATE INDEX search_document_type_idx
  ON serving_v2.search_document (entity_type, publication_state);

CREATE TABLE serving_v2.competition_presentation (
  competition_key        text PRIMARY KEY REFERENCES serving_v2.competition_catalog(competition_key),
  presentation_mode      text NOT NULL,
  route_prefix           text NOT NULL,
  shares_core            boolean NOT NULL DEFAULT true,
  metadata               jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id         bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE serving_v2.publication_metrics (
  metric_key             text PRIMARY KEY,
  metric_value           bigint NOT NULL,
  metadata               jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id         bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

INSERT INTO serving_v2.competition_catalog (
  competition_key, competition_name, competition_type, country_name,
  confederation_name, is_international, is_world_cup, edition_count,
  selectable_edition_count, published_match_count, first_match_date,
  last_match_date, is_selectable, href, metadata, rebuild_run_id
)
SELECT
  c.competition_key,
  c.competition_name,
  c.competition_type,
  c.country_name,
  c.confederation_name,
  c.is_international,
  c.is_world_cup,
  count(e.edition_key),
  count(*) FILTER (WHERE e.publication_state = 'published' AND e.published_match_count > 0),
  coalesce(sum(e.published_match_count), 0),
  min(e.first_match_date),
  max(e.last_match_date),
  coalesce(bool_or(e.publication_state = 'published' AND e.published_match_count > 0), false),
  CASE WHEN c.competition_key = 'fifa_world_cup_mens' THEN '/copa-do-mundo' ELSE '/competitions/' || c.competition_key END,
  c.metadata || jsonb_build_object('catalog_source', 'mart_v2.dim_competition'),
  :rebuild_rebuild_run_id
FROM mart_v2.dim_competition c
LEFT JOIN mart_v2.dim_edition e ON e.competition_key = c.competition_key
GROUP BY c.competition_key, c.competition_name, c.competition_type,
         c.country_name, c.confederation_name, c.is_international,
         c.is_world_cup, c.metadata;

INSERT INTO serving_v2.edition_catalog (
  edition_key, competition_key, competition_name, season_label,
  season_start_date, season_end_date, is_closed, publication_state,
  observed_source_count, published_match_count, first_match_date,
  last_match_date, is_selectable, href, metadata, rebuild_run_id
)
SELECT
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
  e.publication_state = 'published' AND e.published_match_count > 0,
  CASE
    WHEN e.competition_key = 'fifa_world_cup_mens'
      THEN '/copa-do-mundo/' || replace(e.season_label, '/', '_')
    ELSE '/competitions/' || e.competition_key || '/seasons/' || replace(e.season_label, '/', '_')
  END,
  e.metadata || jsonb_build_object(
    'catalog_source', 'mart_v2.dim_edition',
    'empty_context_hidden_from_default_filters', e.published_match_count = 0
  ),
  :rebuild_rebuild_run_id
FROM mart_v2.dim_edition e
JOIN serving_v2.competition_catalog c ON c.competition_key = e.competition_key;

WITH appearances AS (
  SELECT home_team_id AS team_id, match_id, competition_key, edition_key, match_date
  FROM mart_v2.fact_match
  WHERE publication_state = 'published'
  UNION ALL
  SELECT away_team_id, match_id, competition_key, edition_key, match_date
  FROM mart_v2.fact_match
  WHERE publication_state = 'published'
), aggregates AS (
  SELECT team_id, count(DISTINCT match_id) AS match_count,
         count(DISTINCT competition_key) AS competition_count,
         count(DISTINCT edition_key) AS edition_count,
         min(match_date) AS first_match_date,
         max(match_date) AS last_match_date
  FROM appearances
  GROUP BY team_id
), assets AS (
  SELECT DISTINCT ON (team_id) team_id, asset_url, asset_type
  FROM mart_v2.team_asset
  ORDER BY team_id, asset_type, asset_key
)
INSERT INTO serving_v2.team_profile (
  team_id, team_name, country_or_territory, team_type, gender, category,
  identity_state, match_count, competition_count, edition_count,
  first_match_date, last_match_date, asset_url, asset_type, metadata,
  rebuild_run_id
)
SELECT
  t.team_id, t.team_name, t.country_or_territory, t.team_type, t.gender,
  t.category, t.identity_state, coalesce(a.match_count, 0),
  coalesce(a.competition_count, 0), coalesce(a.edition_count, 0),
  a.first_match_date, a.last_match_date, x.asset_url, x.asset_type,
  t.metadata || jsonb_build_object('public_id_preserved', t.public_id_preserved),
  :rebuild_rebuild_run_id
FROM mart_v2.dim_team t
LEFT JOIN aggregates a ON a.team_id = t.team_id
LEFT JOIN assets x ON x.team_id = t.team_id
WHERE t.is_active;

WITH usage AS (
  SELECT player_key, match_id, team_id FROM mart_v2.fact_match_player_stats
  UNION ALL
  SELECT player_key, match_id, team_id FROM mart_v2.fact_lineup
  UNION ALL
  SELECT player_key, match_id, team_id FROM mart_v2.fact_match_event
  WHERE player_key IS NOT NULL
), aggregates AS (
  SELECT player_key, count(DISTINCT u.match_id) AS match_count,
         count(DISTINCT u.team_id) FILTER (WHERE u.team_id IS NOT NULL) AS team_count,
         min(f.match_date) AS first_match_date,
         max(f.match_date) AS last_match_date
  FROM usage u
  JOIN mart_v2.fact_match f ON f.match_id = u.match_id
  WHERE f.publication_state = 'published'
  GROUP BY player_key
)
INSERT INTO serving_v2.player_profile (
  player_key, display_name, nationality, date_of_birth, position_name,
  preferred_foot, match_count, team_count, first_match_date, last_match_date,
  metadata, rebuild_run_id
)
SELECT
  p.player_key, p.display_name, p.nationality, p.date_of_birth,
  p.position_name, p.preferred_foot, coalesce(a.match_count, 0),
  coalesce(a.team_count, 0), a.first_match_date, a.last_match_date,
  p.metadata, :rebuild_rebuild_run_id
FROM mart_v2.dim_player p
JOIN aggregates a ON a.player_key = p.player_key;

INSERT INTO serving_v2.match_catalog (
  match_id, match_date, competition_key, competition_name, edition_key,
  season_label, stage_name, round_name, home_team_id, home_team_name,
  away_team_id, away_team_name, home_goals, away_goals, status,
  is_world_cup, href, metadata, rebuild_run_id
)
SELECT
  f.match_id, f.match_date, f.competition_key, c.competition_name,
  f.edition_key, e.season_label, s.stage_name, r.round_name,
  f.home_team_id, ht.team_name, f.away_team_id, at.team_name,
  f.home_goals, f.away_goals, coalesce(f.status_short, f.status_long),
  c.is_world_cup, '/matches/' || f.match_id::text,
  f.metadata || jsonb_build_object('publication_state', f.publication_state),
  :rebuild_rebuild_run_id
FROM mart_v2.fact_match f
JOIN mart_v2.dim_competition c ON c.competition_key = f.competition_key
JOIN mart_v2.dim_edition e ON e.edition_key = f.edition_key
JOIN mart_v2.dim_team ht ON ht.team_id = f.home_team_id
JOIN mart_v2.dim_team at ON at.team_id = f.away_team_id
LEFT JOIN mart_v2.dim_stage s ON s.stage_key = f.stage_key
LEFT JOIN mart_v2.dim_round r ON r.round_key = f.round_key
WHERE f.publication_state = 'published';

INSERT INTO serving_v2.search_document (
  document_id, entity_type, entity_id, label, subtitle, search_text, href,
  competition_key, edition_key, publication_state, metadata, rebuild_run_id
)
SELECT
  'competition:' || competition_key, 'competition', competition_key,
  competition_name, coalesce(country_name, competition_type),
  lower(unaccent(competition_name || ' ' || coalesce(country_name, '') || ' ' || competition_key)),
  href, competition_key, NULL, 'published', metadata, :rebuild_rebuild_run_id
FROM serving_v2.competition_catalog
WHERE is_selectable;

INSERT INTO serving_v2.search_document (
  document_id, entity_type, entity_id, label, subtitle, search_text, href,
  competition_key, edition_key, publication_state, metadata, rebuild_run_id
)
SELECT
  'edition:' || edition_key, 'edition', edition_key,
  competition_name || ' — ' || season_label,
  season_label || ' · ' || published_match_count || ' partidas',
  lower(unaccent(competition_name || ' ' || season_label || ' ' || edition_key)),
  href, competition_key, edition_key, 'published', metadata, :rebuild_rebuild_run_id
FROM serving_v2.edition_catalog
WHERE is_selectable;

INSERT INTO serving_v2.search_document (
  document_id, entity_type, entity_id, label, subtitle, search_text, href,
  competition_key, edition_key, publication_state, metadata, rebuild_run_id
)
SELECT
  'team:' || team_id::text, 'team', team_id::text, team_name,
  coalesce(country_or_territory, team_type),
  lower(unaccent(team_name || ' ' || coalesce(country_or_territory, '') || ' ' || team_id::text)),
  '/clubs/' || team_id::text, NULL, NULL, 'published',
  metadata || jsonb_build_object(
    'team_type', team_type,
    'asset_url', asset_url,
    'asset_type', asset_type
  ),
  :rebuild_rebuild_run_id
FROM serving_v2.team_profile
WHERE match_count > 0;

INSERT INTO serving_v2.search_document (
  document_id, entity_type, entity_id, label, subtitle, search_text, href,
  competition_key, edition_key, publication_state, metadata, rebuild_run_id
)
SELECT
  'player:' || player_key, 'player', player_key,
  coalesce(display_name, 'Jogador sem nome'),
  coalesce(position_name, nationality),
  lower(unaccent(coalesce(display_name, '') || ' ' || coalesce(nationality, '') || ' ' || player_key)),
  '/players/' || player_key, NULL, NULL, 'published',
  metadata || jsonb_build_object(
    'position_name', position_name,
    'nationality', nationality
  ),
  :rebuild_rebuild_run_id
FROM serving_v2.player_profile
WHERE match_count > 0;

INSERT INTO serving_v2.search_document (
  document_id, entity_type, entity_id, label, subtitle, search_text, href,
  competition_key, edition_key, publication_state, metadata, rebuild_run_id
)
SELECT
  'match:' || match_id::text, 'match', match_id::text,
  home_team_name || ' x ' || away_team_name,
  competition_name || ' · ' || season_label || ' · ' || match_date::text,
  lower(unaccent(home_team_name || ' ' || away_team_name || ' ' || competition_name || ' ' || season_label || ' ' || match_id::text)),
  href, competition_key, edition_key, 'published',
  metadata || jsonb_build_object(
    'home_team_id', home_team_id,
    'home_team_name', home_team_name,
    'away_team_id', away_team_id,
    'away_team_name', away_team_name,
    'home_goals', home_goals,
    'away_goals', away_goals,
    'match_date', match_date
  ),
  :rebuild_rebuild_run_id
FROM serving_v2.match_catalog;

INSERT INTO serving_v2.competition_presentation (
  competition_key, presentation_mode, route_prefix, shares_core, metadata, rebuild_run_id
)
SELECT
  competition_key,
  CASE WHEN is_world_cup THEN 'world_cup_special' ELSE 'standard' END,
  CASE WHEN is_world_cup THEN '/copa-do-mundo' ELSE '/competitions/' || competition_key END,
  true,
  jsonb_build_object('core_schema', 'mart_v2', 'serving_schema', 'serving_v2'),
  :rebuild_rebuild_run_id
FROM serving_v2.competition_catalog;

INSERT INTO serving_v2.publication_metrics (metric_key, metric_value, metadata, rebuild_run_id)
SELECT 'published_matches', count(*), '{}'::jsonb, :rebuild_rebuild_run_id
FROM serving_v2.match_catalog
UNION ALL
SELECT 'selectable_editions', count(*), '{}'::jsonb, :rebuild_rebuild_run_id
FROM serving_v2.edition_catalog WHERE is_selectable
UNION ALL
SELECT 'search_documents', count(*), '{}'::jsonb, :rebuild_rebuild_run_id
FROM serving_v2.search_document
UNION ALL
SELECT 'teams_with_published_matches', count(*), '{}'::jsonb, :rebuild_rebuild_run_id
FROM serving_v2.team_profile WHERE match_count > 0
UNION ALL
SELECT 'players_with_published_matches', count(*), '{}'::jsonb, :rebuild_rebuild_run_id
FROM serving_v2.player_profile WHERE match_count > 0;

ANALYZE serving_v2.competition_catalog;
ANALYZE serving_v2.edition_catalog;
ANALYZE serving_v2.team_profile;
ANALYZE serving_v2.player_profile;
ANALYZE serving_v2.match_catalog;
ANALYZE serving_v2.search_document;

UPDATE control.rebuild_run
SET phase = 'serving_v2', status = 'succeeded', finished_at = now()
WHERE rebuild_run_id = :rebuild_rebuild_run_id;
