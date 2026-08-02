\set ON_ERROR_STOP on
\if :{?rebuild_run_key}
\else
  \set rebuild_run_key 'mart-v2-local-current'
\endif

SELECT rebuild_run_id
FROM control.rebuild_run
WHERE run_key = :'rebuild_run_key'\gset rebuild_

CREATE EXTENSION IF NOT EXISTS unaccent;

WITH tm_competition AS (
  SELECT
    g.competition_id,
    CASE
      WHEN g.competition_id = 'FIWC' THEN 'fifa_world_cup_mens'
      ELSE coalesce(p.competition_key, 'transfermarkt:' || lower(g.competition_id))
    END AS competition_key,
    max(NULLIF(trim(c.name), '')) AS competition_name,
    max(NULLIF(trim(c.type), '')) AS competition_type,
    max(NULLIF(trim(c.country_name), '')) AS country_name,
    max(NULLIF(trim(c.confederation), '')) AS confederation_name,
    count(*)::integer AS source_count
  FROM raw_src.tm_games g
  LEFT JOIN raw_src.tm_competitions c ON c.competition_id = g.competition_id
  LEFT JOIN control.competition_provider_map p
    ON p.provider = 'transfermarkt'
   AND p.provider_league_code = g.competition_id
  GROUP BY g.competition_id,
           CASE WHEN g.competition_id = 'FIWC' THEN 'fifa_world_cup_mens'
                ELSE coalesce(p.competition_key, 'transfermarkt:' || lower(g.competition_id)) END
)
INSERT INTO mart_v2.dim_competition (
  competition_key, competition_name, competition_type, country_name,
  confederation_name, is_international, is_world_cup, source_count,
  metadata, rebuild_run_id
)
SELECT
  competition_key,
  coalesce(competition_name, competition_key),
  competition_type,
  country_name,
  confederation_name,
  competition_key ILIKE '%world_cup%' OR competition_type = 'national_team_competition',
  competition_key ILIKE '%world_cup%',
  source_count,
  jsonb_build_object('catalog_source', 'raw.tm_games', 'provider_competition_id', competition_id),
  :rebuild_rebuild_run_id
FROM tm_competition
ON CONFLICT (competition_key) DO UPDATE
SET competition_name = coalesce(EXCLUDED.competition_name, mart_v2.dim_competition.competition_name),
    competition_type = coalesce(EXCLUDED.competition_type, mart_v2.dim_competition.competition_type),
    country_name = coalesce(EXCLUDED.country_name, mart_v2.dim_competition.country_name),
    confederation_name = coalesce(EXCLUDED.confederation_name, mart_v2.dim_competition.confederation_name),
    source_count = greatest(mart_v2.dim_competition.source_count, EXCLUDED.source_count),
    metadata = mart_v2.dim_competition.metadata || EXCLUDED.metadata;

WITH tm_context AS (
  SELECT
    CASE
      WHEN g.competition_id = 'FIWC' THEN 'fifa_world_cup_mens'
      ELSE coalesce(p.competition_key, 'transfermarkt:' || lower(g.competition_id))
    END AS competition_key,
    CASE
      WHEN g.competition_id = 'FIWC' THEN extract(year FROM g.match_date_raw::date)::integer::text
      WHEN g.competition_id = 'BRA1' THEN extract(year FROM g.match_date_raw::date)::integer::text
      ELSE g.season
    END AS season_label,
    min(g.match_date_raw::date) AS first_date,
    max(g.match_date_raw::date) AS last_date,
    count(*)::bigint AS observed_rows
  FROM raw_src.tm_games g
  LEFT JOIN control.competition_provider_map p
    ON p.provider = 'transfermarkt'
   AND p.provider_league_code = g.competition_id
  GROUP BY
    CASE WHEN g.competition_id = 'FIWC' THEN 'fifa_world_cup_mens'
         ELSE coalesce(p.competition_key, 'transfermarkt:' || lower(g.competition_id)) END,
    CASE WHEN g.competition_id = 'FIWC' THEN extract(year FROM g.match_date_raw::date)::integer::text
         WHEN g.competition_id = 'BRA1' THEN extract(year FROM g.match_date_raw::date)::integer::text
         ELSE g.season END
), brasileirao_context AS (
  SELECT
    x.competition_key,
    extract(year FROM x.match_date)::integer::text AS season_label,
    min(x.match_date) AS first_date,
    max(x.match_date) AS last_date,
    count(*)::bigint AS observed_rows
  FROM control.external_match_publication_xref x
  WHERE x.source = 'dataset_brasileirao'
    AND x.publication_status = 'publishable'
    AND x.competition_key IS NOT NULL
  GROUP BY x.competition_key, extract(year FROM x.match_date)::integer::text
)
INSERT INTO mart_v2.dim_edition (
  edition_key, competition_key, season_label, is_closed,
  observed_source_count, first_match_date, last_match_date, metadata, rebuild_run_id
)
SELECT
  c.competition_key || ':' || c.season_label,
  c.competition_key,
  c.season_label,
  c.last_date < current_date,
  c.observed_rows,
  c.first_date,
  c.last_date,
  jsonb_build_object('catalog_source', 'historical_source_context'),
  :rebuild_rebuild_run_id
FROM (
  SELECT * FROM tm_context
  UNION ALL
  SELECT * FROM brasileirao_context
) c
WHERE c.competition_key IN (SELECT competition_key FROM mart_v2.dim_competition)
ON CONFLICT (edition_key) DO UPDATE
SET season_start_date = coalesce(mart_v2.dim_edition.season_start_date, EXCLUDED.first_match_date),
    season_end_date = coalesce(EXCLUDED.last_match_date, mart_v2.dim_edition.season_end_date),
    is_closed = coalesce(EXCLUDED.is_closed, mart_v2.dim_edition.is_closed),
    observed_source_count = greatest(mart_v2.dim_edition.observed_source_count, EXCLUDED.observed_source_count),
    first_match_date = least(mart_v2.dim_edition.first_match_date, EXCLUDED.first_match_date),
    last_match_date = greatest(mart_v2.dim_edition.last_match_date, EXCLUDED.last_match_date),
    metadata = mart_v2.dim_edition.metadata || EXCLUDED.metadata;

DROP TABLE IF EXISTS tmp_tm_games_prepared;
CREATE TEMP TABLE tmp_tm_games_prepared ON COMMIT PRESERVE ROWS AS
WITH team_source_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id,
    canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'transfermarkt'
    AND entity_type = 'team'
    AND mapping_state = 'approved'
    AND canonical_entity_id ~ '^[0-9]+$'
  ORDER BY source_entity_id, confidence DESC NULLS LAST
), team_name_map AS (
  SELECT DISTINCT ON (lower(regexp_replace(unaccent(team_name), '[^a-zA-Z0-9]+', '', 'g')))
    lower(regexp_replace(unaccent(team_name), '[^a-zA-Z0-9]+', '', 'g')) AS normalized_name,
    team_id
  FROM mart_v2.dim_team
  ORDER BY lower(regexp_replace(unaccent(team_name), '[^a-zA-Z0-9]+', '', 'g')), team_id
), tm_games_prepared AS (
  SELECT DISTINCT ON (g.game_id)
    g.*,
    CASE
      WHEN g.competition_id = 'FIWC' THEN 'fifa_world_cup_mens'
      ELSE coalesce(p.competition_key, 'transfermarkt:' || lower(g.competition_id))
    END AS normalized_competition_key,
    CASE
      WHEN g.competition_id = 'FIWC' THEN extract(year FROM g.match_date_raw::date)::integer::text
      WHEN g.competition_id = 'BRA1' THEN extract(year FROM g.match_date_raw::date)::integer::text
      ELSE g.season
    END AS normalized_season_label,
    coalesce(ts_h.canonical_id, tn_h.team_id) AS canonical_home_team_id,
    coalesce(ts_a.canonical_id, tn_a.team_id) AS canonical_away_team_id,
    x.local_fixture_id
  FROM raw_src.tm_games g
  LEFT JOIN control.competition_provider_map p
    ON p.provider = 'transfermarkt'
   AND p.provider_league_code = g.competition_id
  LEFT JOIN team_source_map ts_h ON ts_h.source_entity_id = g.home_club_id
  LEFT JOIN team_source_map ts_a ON ts_a.source_entity_id = g.away_club_id
  LEFT JOIN team_name_map tn_h
    ON tn_h.normalized_name = lower(regexp_replace(unaccent(g.home_club_name), '[^a-zA-Z0-9]+', '', 'g'))
  LEFT JOIN team_name_map tn_a
    ON tn_a.normalized_name = lower(regexp_replace(unaccent(g.away_club_name), '[^a-zA-Z0-9]+', '', 'g'))
  LEFT JOIN control.tm_game_fixture_xref x ON x.tm_game_id = g.game_id
  ORDER BY g.game_id, g.ingested_at DESC NULLS LAST
)
SELECT * FROM tm_games_prepared;

INSERT INTO mart_v2.fact_match (
  match_id, match_key, source_system, source_match_id, competition_key,
  edition_key, match_date, date_utc, status_short, status_long,
  home_team_id, away_team_id, home_team_name_raw, away_team_name_raw,
  home_goals, away_goals, venue_name, publication_state,
  publication_reason, source_run_id, ingested_at, metadata, rebuild_run_id
)
SELECT
  CASE WHEN p.local_fixture_id IS NOT NULL AND EXISTS (SELECT 1 FROM mart_v2.fact_match f WHERE f.match_id = p.local_fixture_id)
       THEN p.local_fixture_id ELSE 800000000000 + p.game_id::bigint END,
  'transfermarkt:' || p.game_id::text,
  'transfermarkt',
  p.game_id,
  p.normalized_competition_key,
  p.normalized_competition_key || ':' || p.normalized_season_label,
  p.match_date_raw::date,
  p.match_date_raw::date::timestamp AT TIME ZONE 'UTC',
  'FT',
  'Finished',
  p.canonical_home_team_id,
  p.canonical_away_team_id,
  p.home_club_name,
  p.away_club_name,
  CASE WHEN p.home_club_goals ~ '^-?[0-9]+$' THEN p.home_club_goals::integer END,
  CASE WHEN p.away_club_goals ~ '^-?[0-9]+$' THEN p.away_club_goals::integer END,
  p.stadium,
  CASE WHEN p.match_date_raw IS NOT NULL AND p.canonical_home_team_id IS NOT NULL AND p.canonical_away_team_id IS NOT NULL
       THEN 'published' ELSE 'quarantined' END,
  CASE WHEN p.canonical_home_team_id IS NULL OR p.canonical_away_team_id IS NULL THEN 'unresolved_team_identity'
       WHEN p.match_date_raw IS NULL THEN 'missing_match_date' END,
  'transfermarkt',
  p.ingested_at,
  jsonb_build_object('competition_provider_id', p.competition_id, 'source_season_label', p.season,
                     'round', p.round, 'game_url', p.url, 'linked_fixture_id', p.local_fixture_id),
  :rebuild_rebuild_run_id
FROM tmp_tm_games_prepared p
WHERE p.local_fixture_id IS NULL
   OR NOT EXISTS (SELECT 1 FROM mart_v2.fact_match f WHERE f.match_id = p.local_fixture_id)
ON CONFLICT (match_id) DO UPDATE
SET competition_key = EXCLUDED.competition_key,
    edition_key = EXCLUDED.edition_key,
    match_date = EXCLUDED.match_date,
    date_utc = EXCLUDED.date_utc,
    home_team_id = EXCLUDED.home_team_id,
    away_team_id = EXCLUDED.away_team_id,
    publication_state = EXCLUDED.publication_state,
    publication_reason = EXCLUDED.publication_reason,
    metadata = mart_v2.fact_match.metadata || EXCLUDED.metadata,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

WITH dataset_team_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id,
    canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'dataset_brasileirao'
    AND entity_type = 'team'
    AND mapping_state = 'approved'
    AND canonical_entity_id ~ '^[0-9]+$'
  ORDER BY source_entity_id, confidence DESC NULLS LAST
), dataset_alias_map AS (
  SELECT DISTINCT ON (lower(regexp_replace(unaccent(source_entity_id), '[^a-zA-Z0-9]+', '', 'g')))
    lower(regexp_replace(unaccent(source_entity_id), '[^a-zA-Z0-9]+', '', 'g')) AS normalized_name,
    canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'dataset_brasileirao'
    AND entity_type = 'team'
    AND mapping_state = 'approved'
    AND canonical_entity_id ~ '^[0-9]+$'
  ORDER BY lower(regexp_replace(unaccent(source_entity_id), '[^a-zA-Z0-9]+', '', 'g')), confidence DESC NULLS LAST
), dataset_name_map AS (
  SELECT DISTINCT ON (lower(regexp_replace(unaccent(team_name), '[^a-zA-Z0-9]+', '', 'g')))
    lower(regexp_replace(unaccent(team_name), '[^a-zA-Z0-9]+', '', 'g')) AS normalized_name,
    team_id
  FROM mart_v2.dim_team
  ORDER BY lower(regexp_replace(unaccent(team_name), '[^a-zA-Z0-9]+', '', 'g')), team_id
), dataset_matches AS (
  SELECT
    x.canonical_external_match_id,
    x.competition_key,
    x.match_date,
    bm.*,
    coalesce(dm_h.canonical_id, da_h.canonical_id, dn_h.team_id) AS canonical_home_team_id,
    coalesce(dm_a.canonical_id, da_a.canonical_id, dn_a.team_id) AS canonical_away_team_id
  FROM control.external_match_publication_xref x
  JOIN raw_src.brasileirao_matches bm ON bm.match_id = x.source_entity_id
  LEFT JOIN dataset_team_map dm_h
    ON dm_h.source_entity_id = lower(trim(bm.home_team_name))
  LEFT JOIN dataset_team_map dm_a
    ON dm_a.source_entity_id = lower(trim(bm.away_team_name))
  LEFT JOIN dataset_alias_map da_h
    ON da_h.normalized_name = lower(regexp_replace(unaccent(bm.home_team_name), '[^a-zA-Z0-9]+', '', 'g'))
  LEFT JOIN dataset_alias_map da_a
    ON da_a.normalized_name = lower(regexp_replace(unaccent(bm.away_team_name), '[^a-zA-Z0-9]+', '', 'g'))
  LEFT JOIN dataset_name_map dn_h
    ON dn_h.normalized_name = lower(regexp_replace(unaccent(bm.home_team_name), '[^a-zA-Z0-9]+', '', 'g'))
  LEFT JOIN dataset_name_map dn_a
    ON dn_a.normalized_name = lower(regexp_replace(unaccent(bm.away_team_name), '[^a-zA-Z0-9]+', '', 'g'))
  WHERE x.source = 'dataset_brasileirao'
    AND x.publication_status = 'publishable'
)
INSERT INTO mart_v2.fact_match (
  match_id, match_key, source_system, source_match_id, competition_key,
  edition_key, match_date, date_utc, status_short, status_long,
  home_team_id, away_team_id, home_team_name_raw, away_team_name_raw,
  home_goals, away_goals, publication_state, publication_reason,
  source_run_id, ingested_at, metadata, rebuild_run_id
)
SELECT
  d.canonical_external_match_id,
  'dataset_brasileirao:' || d.match_id,
  'dataset_brasileirao',
  d.match_id,
  d.competition_key,
  d.competition_key || ':' || extract(year FROM d.match_date)::integer::text,
  d.match_date,
  d.match_date::timestamp AT TIME ZONE 'UTC',
  'FT',
  'Finished',
  d.canonical_home_team_id,
  d.canonical_away_team_id,
  d.home_team_name,
  d.away_team_name,
  CASE WHEN d.home_score ~ '^-?[0-9]+$' THEN d.home_score::integer END,
  CASE WHEN d.away_score ~ '^-?[0-9]+$' THEN d.away_score::integer END,
  CASE WHEN d.canonical_home_team_id IS NOT NULL AND d.canonical_away_team_id IS NOT NULL THEN 'published' ELSE 'quarantined' END,
  CASE WHEN d.canonical_home_team_id IS NULL OR d.canonical_away_team_id IS NULL THEN 'unresolved_team_identity' END,
  'dataset_brasileirao',
  d.ingested_at,
  jsonb_build_object('round', d.rodada, 'venue_name', d.venue_name, 'source_match_id', d.match_id),
  :rebuild_rebuild_run_id
FROM dataset_matches d
ON CONFLICT (match_id) DO UPDATE
SET competition_key = EXCLUDED.competition_key,
    edition_key = EXCLUDED.edition_key,
    match_date = EXCLUDED.match_date,
    date_utc = EXCLUDED.date_utc,
    home_team_id = EXCLUDED.home_team_id,
    away_team_id = EXCLUDED.away_team_id,
    publication_state = EXCLUDED.publication_state,
    publication_reason = EXCLUDED.publication_reason,
    metadata = mart_v2.fact_match.metadata || EXCLUDED.metadata,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

INSERT INTO mart_v2.match_source (
  source_system, source_match_id, canonical_match_id, source_table,
  source_date, source_home_team_id, source_away_team_id,
  reconciliation_state, method, confidence, evidence, rebuild_run_id
)
SELECT
  'transfermarkt',
  p.game_id,
  CASE WHEN p.local_fixture_id IS NOT NULL AND EXISTS (SELECT 1 FROM mart_v2.fact_match f WHERE f.match_id = p.local_fixture_id)
       THEN p.local_fixture_id ELSE 800000000000 + p.game_id::bigint END,
  'raw.tm_games',
  p.match_date_raw::date,
  p.home_club_id,
  p.away_club_id,
  CASE WHEN p.canonical_home_team_id IS NOT NULL AND p.canonical_away_team_id IS NOT NULL THEN 'approved' ELSE 'quarantined' END,
  CASE WHEN p.local_fixture_id IS NOT NULL THEN 'approved_fixture_xref' ELSE 'new_historical_coverage' END,
  CASE WHEN p.canonical_home_team_id IS NOT NULL AND p.canonical_away_team_id IS NOT NULL THEN 0.95 ELSE 0.2 END,
  jsonb_build_object('competition_id', p.competition_id, 'source_season_label', p.season),
  :rebuild_rebuild_run_id
FROM tmp_tm_games_prepared p
ON CONFLICT (source_system, source_match_id) DO UPDATE
SET canonical_match_id = EXCLUDED.canonical_match_id,
    reconciliation_state = EXCLUDED.reconciliation_state,
    method = EXCLUDED.method,
    confidence = EXCLUDED.confidence,
    evidence = EXCLUDED.evidence,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

INSERT INTO mart_v2.match_source (
  source_system, source_match_id, canonical_match_id, source_table,
  source_date, reconciliation_state, method, confidence, evidence, rebuild_run_id
)
SELECT
  'dataset_brasileirao',
  x.source_entity_id,
  x.canonical_external_match_id,
  'control.external_match_publication_xref',
  x.match_date,
  CASE WHEN fm.publication_state = 'published' THEN 'approved' ELSE 'quarantined' END,
  x.match_method,
  CASE WHEN fm.publication_state = 'published' THEN 1.0 ELSE 0.2 END,
  x.source_evidence,
  :rebuild_rebuild_run_id
FROM control.external_match_publication_xref x
LEFT JOIN mart_v2.fact_match fm ON fm.match_id = x.canonical_external_match_id
WHERE x.source = 'dataset_brasileirao'
ON CONFLICT (source_system, source_match_id) DO UPDATE
SET canonical_match_id = EXCLUDED.canonical_match_id,
    reconciliation_state = EXCLUDED.reconciliation_state,
    method = EXCLUDED.method,
    confidence = EXCLUDED.confidence,
    evidence = EXCLUDED.evidence,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

UPDATE mart_v2.dim_edition e
SET published_match_count = x.published_match_count,
    publication_state = CASE WHEN x.published_match_count > 0 THEN 'published' ELSE 'pending' END,
    first_match_date = x.first_match_date,
    last_match_date = x.last_match_date
FROM (
  SELECT edition_key,
         count(*) FILTER (WHERE publication_state = 'published') AS published_match_count,
         min(match_date) FILTER (WHERE publication_state = 'published') AS first_match_date,
         max(match_date) FILTER (WHERE publication_state = 'published') AS last_match_date
  FROM mart_v2.fact_match
  WHERE edition_key IS NOT NULL
  GROUP BY edition_key
) x
WHERE e.edition_key = x.edition_key;

INSERT INTO control.publication_decision (entity_type, entity_key, publication_state, reason_code, rebuild_run_id, evidence)
SELECT 'match', match_id::text, publication_state, publication_reason, :rebuild_rebuild_run_id,
       jsonb_build_object('source_system', source_system, 'edition_key', edition_key)
FROM mart_v2.fact_match
ON CONFLICT (entity_type, entity_key) DO UPDATE
SET publication_state = EXCLUDED.publication_state,
    reason_code = EXCLUDED.reason_code,
    rebuild_run_id = EXCLUDED.rebuild_run_id,
    evidence = EXCLUDED.evidence,
    decided_at = now();

UPDATE control.rebuild_run
SET phase = 'historical_matches', status = 'succeeded', finished_at = now()
WHERE rebuild_run_id = :rebuild_rebuild_run_id;
