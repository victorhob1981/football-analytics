\set ON_ERROR_STOP on
\if :{?rebuild_run_key}
\else
  \set rebuild_run_key 'mart-v2-local-current'
\endif

SELECT rebuild_run_id
FROM control.rebuild_run
WHERE run_key = :'rebuild_run_key'\gset rebuild_

DROP SCHEMA IF EXISTS mart_v2 CASCADE;
CREATE SCHEMA mart_v2;

CREATE TABLE mart_v2.dim_competition (
  competition_key       text PRIMARY KEY CHECK (length(trim(competition_key)) > 0),
  competition_name      text NOT NULL,
  competition_type      text,
  country_name          text,
  confederation_name    text,
  tier                  smallint,
  is_international      boolean NOT NULL DEFAULT false,
  is_world_cup          boolean NOT NULL DEFAULT false,
  is_active             boolean NOT NULL DEFAULT true,
  display_priority      integer,
  source_count          integer NOT NULL DEFAULT 0,
  observed_first_date   date,
  observed_last_date    date,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE mart_v2.dim_edition (
  edition_key           text PRIMARY KEY,
  competition_key       text NOT NULL REFERENCES mart_v2.dim_competition(competition_key),
  season_label          text NOT NULL,
  season_start_date     date,
  season_end_date       date,
  is_closed             boolean,
  publication_state     text NOT NULL DEFAULT 'pending'
                        CHECK (publication_state IN ('published', 'pending', 'quarantined', 'rejected')),
  observed_source_count bigint NOT NULL DEFAULT 0,
  published_match_count bigint NOT NULL DEFAULT 0,
  first_match_date      date,
  last_match_date       date,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  UNIQUE (competition_key, season_label)
);

CREATE TABLE mart_v2.dim_team (
  team_id               bigint PRIMARY KEY,
  identity_team_id      bigint,
  team_name             text NOT NULL,
  country_or_territory  text,
  team_type             text,
  gender                text,
  category              text,
  identity_state        text NOT NULL DEFAULT 'active',
  is_active             boolean NOT NULL DEFAULT true,
  public_id_preserved   boolean NOT NULL DEFAULT true,
  source_count          integer NOT NULL DEFAULT 0,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE mart_v2.dim_stage (
  stage_key             text PRIMARY KEY,
  edition_key           text NOT NULL REFERENCES mart_v2.dim_edition(edition_key),
  stage_id              bigint,
  stage_name            text NOT NULL,
  sort_order            integer,
  is_inferred           boolean NOT NULL DEFAULT false,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  UNIQUE (edition_key, stage_id, stage_name)
);

CREATE TABLE mart_v2.dim_round (
  round_key             text PRIMARY KEY,
  edition_key           text NOT NULL REFERENCES mart_v2.dim_edition(edition_key),
  stage_key             text REFERENCES mart_v2.dim_stage(stage_key),
  round_id              bigint,
  round_name            text NOT NULL,
  starting_at           date,
  ending_at             date,
  is_inferred           boolean NOT NULL DEFAULT false,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  UNIQUE (edition_key, round_id, round_name)
);

CREATE TABLE mart_v2.dim_group (
  group_key             text PRIMARY KEY,
  edition_key           text NOT NULL REFERENCES mart_v2.dim_edition(edition_key),
  stage_key             text REFERENCES mart_v2.dim_stage(stage_key),
  group_name            text NOT NULL,
  is_inferred           boolean NOT NULL DEFAULT false,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  UNIQUE (edition_key, stage_key, group_name)
);

CREATE TABLE mart_v2.fact_match (
  match_id              bigint PRIMARY KEY,
  match_key             text NOT NULL UNIQUE,
  source_system         text NOT NULL,
  source_match_id       text NOT NULL,
  competition_key       text REFERENCES mart_v2.dim_competition(competition_key),
  edition_key           text REFERENCES mart_v2.dim_edition(edition_key),
  match_date            date,
  date_utc               timestamptz,
  status_short          text,
  status_long           text,
  home_team_id          bigint REFERENCES mart_v2.dim_team(team_id),
  away_team_id          bigint REFERENCES mart_v2.dim_team(team_id),
  home_team_name_raw    text,
  away_team_name_raw    text,
  home_goals            integer,
  away_goals            integer,
  home_goals_ht         integer,
  away_goals_ht         integer,
  venue_id              bigint,
  venue_name            text,
  venue_city            text,
  stage_key             text REFERENCES mart_v2.dim_stage(stage_key),
  round_key             text REFERENCES mart_v2.dim_round(round_key),
  group_key             text REFERENCES mart_v2.dim_group(group_key),
  leg_number            integer,
  publication_state     text NOT NULL CHECK (publication_state IN ('published', 'pending', 'quarantined', 'rejected')),
  publication_reason    text,
  source_run_id         text,
  ingested_at           timestamptz,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE mart_v2.match_source (
  source_system         text NOT NULL,
  source_match_id       text NOT NULL,
  canonical_match_id    bigint REFERENCES mart_v2.fact_match(match_id),
  source_table          text NOT NULL,
  source_date           date,
  source_home_team_id   text,
  source_away_team_id   text,
  reconciliation_state  text NOT NULL CHECK (reconciliation_state IN ('approved', 'pending', 'ambiguous', 'quarantined', 'rejected')),
  method                text,
  confidence            numeric(6,5),
  source_run_id         text,
  evidence              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  PRIMARY KEY (source_system, source_match_id)
);

CREATE INDEX fact_match_v2_edition_date_idx
  ON mart_v2.fact_match (edition_key, match_date, match_id);
CREATE INDEX fact_match_v2_home_idx ON mart_v2.fact_match (home_team_id, match_date);
CREATE INDEX fact_match_v2_away_idx ON mart_v2.fact_match (away_team_id, match_date);
CREATE INDEX fact_match_v2_publication_idx ON mart_v2.fact_match (publication_state, edition_key);
CREATE INDEX match_source_v2_canonical_idx ON mart_v2.match_source (canonical_match_id);

INSERT INTO mart_v2.dim_competition (
  competition_key, competition_name, competition_type, country_name,
  confederation_name, tier, is_international, is_world_cup, is_active,
  display_priority, source_count, observed_first_date, observed_last_date,
  metadata, rebuild_run_id
)
WITH competition_rows AS (
  SELECT
    c.competition_key,
    c.competition_name,
    c.competition_type,
    c.country_name,
    c.confederation_name,
    c.tier,
    c.is_active,
    c.display_priority,
    1 AS source_count,
    NULL::date AS first_date,
    NULL::date AS last_date,
    jsonb_build_object('catalog_source', 'control.competitions') AS metadata
  FROM control.competitions c
  UNION ALL
  SELECT
    trim(f.competition_key),
    max(NULLIF(trim(f.league_name), '')),
    max(NULLIF(trim(f.competition_type), '')),
    NULL,
    NULL,
    NULL,
    true,
    NULL,
    count(*)::integer,
    min(coalesce(f.date_utc::date, f.date)),
    max(coalesce(f.date_utc::date, f.date)),
    jsonb_build_object('catalog_source', 'raw.fixtures')
  FROM raw_src.fixtures f
  WHERE NULLIF(trim(f.competition_key), '') IS NOT NULL
  GROUP BY trim(f.competition_key)
  UNION ALL
  SELECT
    coalesce(NULLIF(trim(m.canonical_competition_key), ''), 'statsbomb:' || m.competition_id::text),
    max(NULLIF(trim(m.metadata -> 'competition' ->> 'competition_name'), '')),
    'external',
    max(NULLIF(trim(m.metadata -> 'competition' ->> 'country_name'), '')),
    NULL,
    NULL,
    true,
    NULL,
    count(*)::integer,
    min(m.match_date),
    max(m.match_date),
    jsonb_build_object('catalog_source', 'raw.statsbomb_matches')
  FROM raw_src.statsbomb_matches m
  WHERE m.match_date IS NOT NULL
  GROUP BY coalesce(NULLIF(trim(m.canonical_competition_key), ''), 'statsbomb:' || m.competition_id::text)
)
SELECT
  competition_key,
  coalesce(max(competition_name), competition_key),
  max(competition_type),
  max(country_name),
  max(confederation_name),
  max(tier),
  bool_or(coalesce(competition_type, '') IN ('continental_cup', 'international', 'cup')
          AND coalesce(country_name, '') IN ('Mundo', 'Europa', 'América do Sul', 'América', 'África')),
  competition_key ILIKE '%world_cup%' OR competition_key = 'fifa_world_cup_mens',
  bool_or(coalesce(is_active, true)),
  min(display_priority),
  sum(source_count),
  min(first_date),
  max(last_date),
  jsonb_agg(metadata) FILTER (WHERE metadata IS NOT NULL),
  :rebuild_rebuild_run_id
FROM competition_rows
WHERE NULLIF(trim(competition_key), '') IS NOT NULL
GROUP BY competition_key;

INSERT INTO mart_v2.dim_edition (
  edition_key, competition_key, season_label, season_start_date,
  season_end_date, is_closed, observed_source_count, first_match_date,
  last_match_date, metadata, rebuild_run_id
)
WITH context_rows AS (
  SELECT
    trim(f.competition_key) AS competition_key,
    trim(f.season_label) AS season_label,
    min(coalesce(f.date_utc::date, f.date)) AS first_date,
    max(coalesce(f.date_utc::date, f.date)) AS last_date,
    count(*)::bigint AS observed_rows,
    jsonb_build_object('source', 'raw.fixtures', 'provider', max(f.provider)) AS metadata
  FROM raw_src.fixtures f
  WHERE NULLIF(trim(f.competition_key), '') IS NOT NULL
    AND NULLIF(trim(f.season_label), '') IS NOT NULL
  GROUP BY trim(f.competition_key), trim(f.season_label)
  UNION ALL
  SELECT
    trim(s.competition_key),
    coalesce(NULLIF(trim(s.season_label), ''), s.season_year::text),
    min(s.starting_at),
    max(s.ending_at),
    count(*)::bigint,
    jsonb_build_object('source', 'raw.competition_seasons', 'provider', max(s.provider))
  FROM raw_src.competition_seasons s
  WHERE NULLIF(trim(s.competition_key), '') IS NOT NULL
  GROUP BY trim(s.competition_key), coalesce(NULLIF(trim(s.season_label), ''), s.season_year::text)
  UNION ALL
  SELECT
    coalesce(NULLIF(trim(m.canonical_competition_key), ''), 'statsbomb:' || m.competition_id::text),
    coalesce(NULLIF(trim(m.season_label), ''), 'unknown'),
    min(m.match_date),
    max(m.match_date),
    count(*)::bigint,
    jsonb_build_object('source', 'raw.statsbomb_matches', 'provider', 'statsbomb_open_data')
  FROM raw_src.statsbomb_matches m
  GROUP BY coalesce(NULLIF(trim(m.canonical_competition_key), ''), 'statsbomb:' || m.competition_id::text),
           coalesce(NULLIF(trim(m.season_label), ''), 'unknown')
  UNION ALL
  SELECT
    s.competition_key,
    s.season_label,
    s.season_start_date,
    s.season_end_date,
    0,
    jsonb_build_object('source', 'control.season_catalog', 'provider', s.provider)
  FROM control.season_catalog s
  WHERE NULLIF(trim(s.competition_key), '') IS NOT NULL
    AND NULLIF(trim(s.season_label), '') IS NOT NULL
)
SELECT
  competition_key || ':' || season_label,
  competition_key,
  season_label,
  min(first_date),
  max(last_date),
  max(last_date) < current_date,
  sum(observed_rows),
  min(first_date),
  max(last_date),
  jsonb_agg(metadata),
  :rebuild_rebuild_run_id
FROM context_rows
WHERE competition_key IN (SELECT competition_key FROM mart_v2.dim_competition)
GROUP BY competition_key, season_label;

INSERT INTO mart_v2.dim_team (
  team_id, identity_team_id, team_name, country_or_territory, team_type,
  gender, category, identity_state, is_active, public_id_preserved,
  source_count, metadata, rebuild_run_id
)
SELECT
  t.canonical_team_id,
  t.canonical_team_id,
  coalesce(NULLIF(trim(t.team_name), ''), 'Time ' || t.canonical_team_id::text),
  NULLIF(trim(t.country_or_territory), ''),
  t.team_type,
  t.gender,
  t.category,
  t.identity_state,
  t.identity_state = 'active',
  true,
  coalesce(s.source_count, 0),
  coalesce(t.decision_evidence, '{}'::jsonb),
  :rebuild_rebuild_run_id
FROM control.team_identity t
LEFT JOIN (
  SELECT canonical_entity_id::bigint AS team_id, count(*)::integer AS source_count
  FROM control.entity_source_identity
  WHERE entity_type = 'team'
    AND canonical_entity_id ~ '^[0-9]+$'
  GROUP BY canonical_entity_id
) s ON s.team_id = t.canonical_team_id;

INSERT INTO mart_v2.dim_stage (
  stage_key, edition_key, stage_id, stage_name, sort_order, is_inferred, rebuild_run_id
)
SELECT DISTINCT ON (edition_key || ':stage:' || coalesce(stage_id::text, lower(regexp_replace(stage_name, '[^a-zA-Z0-9]+', '_', 'g'))))
  edition_key || ':stage:' || coalesce(stage_id::text, lower(regexp_replace(stage_name, '[^a-zA-Z0-9]+', '_', 'g'))),
  edition_key,
  stage_id,
  stage_name,
  sort_order,
  stage_id IS NULL,
  :rebuild_rebuild_run_id
FROM (
  SELECT
    trim(f.competition_key) || ':' || trim(f.season_label) AS edition_key,
    f.stage_id,
    coalesce(NULLIF(trim(f.stage_name), ''), NULLIF(trim(f.round), ''), 'Fase não informada') AS stage_name,
    NULL::integer AS sort_order
  FROM raw_src.fixtures f
  WHERE NULLIF(trim(f.competition_key), '') IS NOT NULL
    AND NULLIF(trim(f.season_label), '') IS NOT NULL
    AND coalesce(NULLIF(trim(f.stage_name), ''), NULLIF(trim(f.round), '')) IS NOT NULL
  UNION ALL
  SELECT
    trim(s.competition_key) || ':' || coalesce(NULLIF(trim(s.season_label), ''), s.season_id::text),
    s.stage_id,
    NULLIF(trim(s.stage_name), ''),
    s.sort_order
  FROM raw_src.competition_stages s
  WHERE NULLIF(trim(s.competition_key), '') IS NOT NULL
    AND NULLIF(trim(s.stage_name), '') IS NOT NULL
) x
WHERE edition_key IN (SELECT edition_key FROM mart_v2.dim_edition)
ORDER BY edition_key || ':stage:' || coalesce(stage_id::text, lower(regexp_replace(stage_name, '[^a-zA-Z0-9]+', '_', 'g'))), sort_order NULLS LAST, stage_name;

INSERT INTO mart_v2.dim_round (
  round_key, edition_key, stage_key, round_id, round_name,
  starting_at, ending_at, is_inferred, rebuild_run_id
)
SELECT DISTINCT ON (x.edition_key || ':round:' || coalesce(x.round_id::text, lower(regexp_replace(x.round_name, '[^a-zA-Z0-9]+', '_', 'g'))))
  x.edition_key || ':round:' || coalesce(x.round_id::text, lower(regexp_replace(x.round_name, '[^a-zA-Z0-9]+', '_', 'g'))),
  x.edition_key,
  s.stage_key,
  x.round_id,
  x.round_name,
  x.starting_at,
  x.ending_at,
  x.round_id IS NULL,
  :rebuild_rebuild_run_id
FROM (
  SELECT
    trim(f.competition_key) || ':' || trim(f.season_label) AS edition_key,
    f.round_id,
    coalesce(NULLIF(trim(f.round_name), ''), NULLIF(trim(f.round), ''), 'Rodada não informada') AS round_name,
    NULL::date AS starting_at,
    NULL::date AS ending_at,
    f.stage_id
  FROM raw_src.fixtures f
  WHERE NULLIF(trim(f.competition_key), '') IS NOT NULL
    AND NULLIF(trim(f.season_label), '') IS NOT NULL
    AND coalesce(NULLIF(trim(f.round_name), ''), NULLIF(trim(f.round), '')) IS NOT NULL
  UNION ALL
  SELECT
    trim(r.competition_key) || ':' || coalesce(NULLIF(trim(r.season_label), ''), r.season_id::text),
    r.round_id,
    NULLIF(trim(r.round_name), ''),
    r.starting_at,
    r.ending_at,
    r.stage_id
  FROM raw_src.competition_rounds r
  WHERE NULLIF(trim(r.competition_key), '') IS NOT NULL
    AND NULLIF(trim(r.round_name), '') IS NOT NULL
) x
LEFT JOIN mart_v2.dim_stage s
  ON s.edition_key = x.edition_key
 AND (s.stage_id = x.stage_id OR (s.stage_id IS NULL AND x.stage_id IS NULL))
WHERE x.edition_key IN (SELECT edition_key FROM mart_v2.dim_edition)
ORDER BY x.edition_key || ':round:' || coalesce(x.round_id::text, lower(regexp_replace(x.round_name, '[^a-zA-Z0-9]+', '_', 'g'))), x.starting_at NULLS LAST, x.round_name;

INSERT INTO mart_v2.dim_group (
  group_key, edition_key, stage_key, group_name, is_inferred, rebuild_run_id
)
SELECT DISTINCT
  x.edition_key || ':group:' || lower(regexp_replace(x.group_name, '[^a-zA-Z0-9]+', '_', 'g')),
  x.edition_key,
  max(s.stage_key),
  x.group_name,
  true,
  :rebuild_rebuild_run_id
FROM (
  SELECT
    trim(f.competition_key) || ':' || trim(f.season_label) AS edition_key,
    NULLIF(trim(f.group_name), '') AS group_name,
    max(f.stage_id) AS stage_id
  FROM raw_src.fixtures f
  WHERE NULLIF(trim(f.competition_key), '') IS NOT NULL
    AND NULLIF(trim(f.season_label), '') IS NOT NULL
    AND NULLIF(trim(f.group_name), '') IS NOT NULL
  GROUP BY trim(f.competition_key), trim(f.season_label), NULLIF(trim(f.group_name), '')
) x
LEFT JOIN mart_v2.dim_stage s
  ON s.edition_key = x.edition_key
 AND (s.stage_id = x.stage_id OR (s.stage_id IS NULL AND x.stage_id IS NULL))
WHERE x.edition_key IN (SELECT edition_key FROM mart_v2.dim_edition)
GROUP BY x.edition_key, x.group_name;

INSERT INTO mart_v2.fact_match (
  match_id, match_key, source_system, source_match_id, competition_key,
  edition_key, match_date, date_utc, status_short, status_long,
  home_team_id, away_team_id, home_team_name_raw, away_team_name_raw,
  home_goals, away_goals, home_goals_ht, away_goals_ht, venue_id,
  venue_name, venue_city, stage_key, round_key, group_key, leg_number,
  publication_state, publication_reason, source_run_id, ingested_at,
  metadata, rebuild_run_id
)
WITH team_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id,
    canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'legacy_control'
    AND entity_type = 'team'
    AND mapping_state = 'approved'
    AND canonical_entity_id ~ '^[0-9]+$'
  ORDER BY source_entity_id, (context_key = '') DESC, confidence DESC NULLS LAST
), source_fixture AS (
  SELECT DISTINCT ON (f.fixture_id)
    f.*,
    h.canonical_id AS canonical_home_team_id,
    a.canonical_id AS canonical_away_team_id,
    coalesce(f.date_utc, f.date::timestamp AT TIME ZONE 'UTC') AS normalized_date_utc
  FROM raw_src.fixtures f
  LEFT JOIN team_map h ON h.source_entity_id = f.home_team_id::text
  LEFT JOIN team_map a ON a.source_entity_id = f.away_team_id::text
  ORDER BY f.fixture_id, f.ingested_at DESC NULLS LAST
)
SELECT
  f.fixture_id,
  'fixture:' || f.fixture_id::text,
  coalesce(NULLIF(trim(f.provider), ''), NULLIF(trim(f.source_provider), ''), 'sportmonks'),
  f.fixture_id::text,
  NULLIF(trim(f.competition_key), ''),
  CASE WHEN NULLIF(trim(f.competition_key), '') IS NOT NULL AND NULLIF(trim(f.season_label), '') IS NOT NULL
    THEN trim(f.competition_key) || ':' || trim(f.season_label) END,
  f.normalized_date_utc::date,
  f.normalized_date_utc,
  f.status_short,
  f.status_long,
  f.canonical_home_team_id,
  f.canonical_away_team_id,
  f.home_team_name,
  f.away_team_name,
  coalesce(f.home_goals_ft, f.home_goals),
  coalesce(f.away_goals_ft, f.away_goals),
  f.home_goals_ht,
  f.away_goals_ht,
  f.venue_id,
  f.venue_name,
  f.venue_city,
  s.stage_key,
  r.round_key,
  g.group_key,
  f.leg,
  CASE
    WHEN f.normalized_date_utc IS NULL THEN 'quarantined'
    WHEN f.competition_key IS NULL OR trim(f.competition_key) = '' THEN 'quarantined'
    WHEN f.season_label IS NULL OR trim(f.season_label) = '' THEN 'quarantined'
    WHEN f.canonical_home_team_id IS NULL OR f.canonical_away_team_id IS NULL THEN 'quarantined'
    ELSE 'published'
  END,
  CASE
    WHEN f.normalized_date_utc IS NULL THEN 'missing_match_date'
    WHEN f.competition_key IS NULL OR trim(f.competition_key) = '' THEN 'missing_competition'
    WHEN f.season_label IS NULL OR trim(f.season_label) = '' THEN 'missing_edition'
    WHEN f.canonical_home_team_id IS NULL OR f.canonical_away_team_id IS NULL THEN 'unresolved_team_identity'
    ELSE NULL
  END,
  f.source_run_id,
  f.ingested_at,
  jsonb_build_object(
    'provider_league_id', f.provider_league_id,
    'provider_season_id', f.provider_season_id,
    'season_name', f.season_name,
    'stage_name', f.stage_name,
    'round_name', f.round_name,
    'group_name', f.group_name,
    'source_provider', f.source_provider
  ),
  :rebuild_rebuild_run_id
FROM source_fixture f
LEFT JOIN mart_v2.dim_stage s
  ON s.edition_key = trim(f.competition_key) || ':' || trim(f.season_label)
 AND (s.stage_id = f.stage_id OR (s.stage_id IS NULL AND f.stage_id IS NULL))
LEFT JOIN mart_v2.dim_round r
  ON r.edition_key = trim(f.competition_key) || ':' || trim(f.season_label)
 AND (r.round_id = f.round_id OR (r.round_id IS NULL AND f.round_id IS NULL))
 AND r.round_name = coalesce(NULLIF(trim(f.round_name), ''), NULLIF(trim(f.round), ''), 'Rodada não informada')
LEFT JOIN mart_v2.dim_group g
  ON g.edition_key = trim(f.competition_key) || ':' || trim(f.season_label)
 AND g.group_name = NULLIF(trim(f.group_name), '');

INSERT INTO mart_v2.fact_match (
  match_id, match_key, source_system, source_match_id, competition_key,
  edition_key, match_date, date_utc, status_short, status_long,
  home_team_id, away_team_id, home_team_name_raw, away_team_name_raw,
  home_goals, away_goals, venue_id, venue_name, stage_key,
  publication_state, publication_reason, source_run_id, ingested_at,
  metadata, rebuild_run_id
)
WITH sb_team_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id,
    canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'statsbomb_open_data'
    AND entity_type = 'team'
    AND mapping_state = 'approved'
    AND canonical_entity_id ~ '^[0-9]+$'
  ORDER BY source_entity_id, confidence DESC NULLS LAST
)
SELECT
  900000000000 + m.match_id,
  'statsbomb:' || m.match_id::text,
  'statsbomb_open_data',
  m.match_id::text,
  coalesce(NULLIF(trim(m.canonical_competition_key), ''), 'statsbomb:' || m.competition_id::text),
  coalesce(NULLIF(trim(m.canonical_competition_key), ''), 'statsbomb:' || m.competition_id::text) || ':' || coalesce(NULLIF(trim(m.season_label), ''), 'unknown'),
  m.match_date,
  m.match_date::timestamp AT TIME ZONE 'UTC',
  m.match_status,
  m.match_status,
  h.canonical_id,
  a.canonical_id,
  m.home_team_name,
  m.away_team_name,
  m.home_score,
  m.away_score,
  m.stadium_id,
  m.stadium_name,
  s.stage_key,
  CASE WHEN m.match_date IS NOT NULL AND h.canonical_id IS NOT NULL AND a.canonical_id IS NOT NULL
    THEN 'published' ELSE 'quarantined' END,
  CASE WHEN m.match_date IS NULL THEN 'missing_match_date'
       WHEN h.canonical_id IS NULL OR a.canonical_id IS NULL THEN 'unresolved_team_identity'
       ELSE NULL END,
  'statsbomb_open_data',
  m.updated_at,
  jsonb_build_object('identity_status', m.identity_status, 'identity_confidence', m.identity_confidence, 'identity_reason', m.identity_reason),
  :rebuild_rebuild_run_id
FROM raw_src.statsbomb_matches m
LEFT JOIN sb_team_map h ON h.source_entity_id = m.home_team_id::text
LEFT JOIN sb_team_map a ON a.source_entity_id = m.away_team_id::text
LEFT JOIN mart_v2.dim_stage s
  ON s.edition_key = coalesce(NULLIF(trim(m.canonical_competition_key), ''), 'statsbomb:' || m.competition_id::text) || ':' || coalesce(NULLIF(trim(m.season_label), ''), 'unknown')
 AND (s.stage_id = m.competition_stage_id OR (s.stage_id IS NULL AND m.competition_stage_id IS NULL))
WHERE m.local_match_id IS NULL;

INSERT INTO mart_v2.match_source (
  source_system, source_match_id, canonical_match_id, source_table,
  source_date, source_home_team_id, source_away_team_id,
  reconciliation_state, method, confidence, source_run_id, evidence, rebuild_run_id
)
SELECT
  coalesce(NULLIF(trim(f.provider), ''), NULLIF(trim(f.source_provider), ''), 'sportmonks'),
  f.fixture_id::text,
  f.fixture_id,
  'raw.fixtures',
  coalesce(f.date_utc::date, f.date),
  f.home_team_id::text,
  f.away_team_id::text,
  CASE WHEN fm.publication_state = 'published' THEN 'approved' ELSE fm.publication_state END,
  'primary_fixture',
  CASE WHEN fm.publication_state = 'published' THEN 1.0 ELSE 0.2 END,
  f.source_run_id,
  jsonb_build_object('provider_league_id', f.provider_league_id, 'provider_season_id', f.provider_season_id),
  :rebuild_rebuild_run_id
FROM raw_src.fixtures f
JOIN mart_v2.fact_match fm ON fm.match_id = f.fixture_id
ON CONFLICT (source_system, source_match_id) DO UPDATE
SET canonical_match_id = EXCLUDED.canonical_match_id,
    reconciliation_state = EXCLUDED.reconciliation_state,
    confidence = EXCLUDED.confidence,
    evidence = EXCLUDED.evidence,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

INSERT INTO mart_v2.match_source (
  source_system, source_match_id, canonical_match_id, source_table,
  source_date, source_home_team_id, source_away_team_id,
  reconciliation_state, method, confidence, source_run_id, evidence, rebuild_run_id
)
SELECT
  'statsbomb_open_data',
  m.match_id::text,
  CASE WHEN m.local_match_id IS NOT NULL THEN m.local_match_id ELSE 900000000000 + m.match_id END,
  'raw.statsbomb_matches',
  m.match_date,
  m.home_team_id::text,
  m.away_team_id::text,
  CASE WHEN fm.publication_state = 'published' THEN 'approved' ELSE 'quarantined' END,
  CASE WHEN m.local_match_id IS NOT NULL THEN 'existing_local_match' ELSE 'new_external_match' END,
  coalesce(m.identity_confidence, CASE WHEN fm.publication_state = 'published' THEN 0.9 ELSE 0.2 END),
  'statsbomb_open_data',
  jsonb_build_object('local_match_id', m.local_match_id, 'identity_status', m.identity_status, 'identity_reason', m.identity_reason),
  :rebuild_rebuild_run_id
FROM raw_src.statsbomb_matches m
LEFT JOIN mart_v2.fact_match fm
  ON fm.match_id = CASE WHEN m.local_match_id IS NOT NULL THEN m.local_match_id ELSE 900000000000 + m.match_id END
ON CONFLICT (source_system, source_match_id) DO UPDATE
SET canonical_match_id = EXCLUDED.canonical_match_id,
    reconciliation_state = EXCLUDED.reconciliation_state,
    method = EXCLUDED.method,
    confidence = EXCLUDED.confidence,
    evidence = EXCLUDED.evidence,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

INSERT INTO mart_v2.match_source (
  source_system, source_match_id, canonical_match_id, source_table,
  source_date, source_home_team_id, source_away_team_id,
  reconciliation_state, method, confidence, evidence, rebuild_run_id
)
SELECT
  'transfermarkt',
  x.tm_game_id,
  x.local_fixture_id,
  'control.tm_game_fixture_xref',
  x.match_date,
  NULL,
  NULL,
  CASE WHEN x.review_status IN ('auto_approved', 'approved') AND fm.match_id IS NOT NULL THEN 'approved'
       WHEN x.identity_status IN ('ambiguous', 'manual_review') THEN 'ambiguous' ELSE 'pending' END,
  x.match_method,
  x.confidence,
  coalesce(x.source_evidence, '{}'::jsonb),
  :rebuild_rebuild_run_id
FROM control.tm_game_fixture_xref x
LEFT JOIN mart_v2.fact_match fm ON fm.match_id = x.local_fixture_id
ON CONFLICT (source_system, source_match_id) DO UPDATE
SET canonical_match_id = EXCLUDED.canonical_match_id,
    reconciliation_state = EXCLUDED.reconciliation_state,
    method = EXCLUDED.method,
    confidence = EXCLUDED.confidence,
    evidence = EXCLUDED.evidence,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

INSERT INTO mart_v2.match_source (
  source_system, source_match_id, canonical_match_id, source_table,
  source_date, source_home_team_id, source_away_team_id,
  reconciliation_state, method, confidence, evidence, rebuild_run_id
)
SELECT
  'eloratings',
  x.elo_match_hash,
  x.local_fixture_id,
  'control.elo_match_xref',
  x.match_date,
  NULL,
  NULL,
  CASE WHEN x.review_status IN ('auto_approved', 'approved') AND fm.match_id IS NOT NULL THEN 'approved'
       WHEN x.identity_status IN ('ambiguous', 'manual_review') THEN 'ambiguous' ELSE 'pending' END,
  x.match_method,
  x.confidence,
  coalesce(x.source_evidence, '{}'::jsonb),
  :rebuild_rebuild_run_id
FROM control.elo_match_xref x
LEFT JOIN mart_v2.fact_match fm ON fm.match_id = x.local_fixture_id
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
  x.brasileirao_match_id,
  x.local_fixture_id,
  'control.brasileirao_fixture_xref',
  x.match_date,
  CASE WHEN x.review_status IN ('auto_approved', 'approved') AND fm.match_id IS NOT NULL THEN 'approved'
       WHEN x.identity_status IN ('ambiguous', 'manual_review') THEN 'ambiguous' ELSE 'pending' END,
  x.match_method,
  x.confidence,
  coalesce(x.source_evidence, '{}'::jsonb),
  :rebuild_rebuild_run_id
FROM control.brasileirao_fixture_xref x
LEFT JOIN mart_v2.fact_match fm ON fm.match_id = x.local_fixture_id
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
SELECT DISTINCT ON (g.source_match_id)
  'fjelstul_worldcup',
  g.source_match_id,
  g.fixture_id,
  'raw.wc_goals',
  NULL,
  CASE WHEN fm.match_id IS NOT NULL THEN 'approved' ELSE 'pending' END,
  'world_cup_fixture_link',
  CASE WHEN fm.match_id IS NOT NULL THEN 1.0 ELSE 0.2 END,
  jsonb_build_object('edition_key', g.edition_key, 'internal_match_id', g.internal_match_id),
  :rebuild_rebuild_run_id
FROM raw_src.wc_goals g
LEFT JOIN mart_v2.fact_match fm ON fm.match_id = g.fixture_id
WHERE NULLIF(trim(g.source_match_id), '') IS NOT NULL
ORDER BY g.source_match_id, g.updated_at DESC NULLS LAST
ON CONFLICT (source_system, source_match_id) DO UPDATE
SET canonical_match_id = EXCLUDED.canonical_match_id,
    reconciliation_state = EXCLUDED.reconciliation_state,
    confidence = EXCLUDED.confidence,
    evidence = EXCLUDED.evidence,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

UPDATE mart_v2.dim_edition e
SET published_match_count = x.published_match_count,
    publication_state = CASE WHEN x.published_match_count > 0 THEN 'published' ELSE 'pending' END,
    first_match_date = x.first_match_date,
    last_match_date = x.last_match_date
FROM (
  SELECT edition_key, count(*) FILTER (WHERE publication_state = 'published') AS published_match_count,
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

INSERT INTO control.publication_decision (entity_type, entity_key, publication_state, reason_code, rebuild_run_id, evidence)
SELECT 'edition', edition_key, publication_state,
       CASE WHEN published_match_count = 0 THEN 'no_published_matches' ELSE NULL END,
       :rebuild_rebuild_run_id,
       jsonb_build_object('published_match_count', published_match_count)
FROM mart_v2.dim_edition
ON CONFLICT (entity_type, entity_key) DO UPDATE
SET publication_state = EXCLUDED.publication_state,
    reason_code = EXCLUDED.reason_code,
    rebuild_run_id = EXCLUDED.rebuild_run_id,
    evidence = EXCLUDED.evidence,
    decided_at = now();

INSERT INTO control.quality_issue (rebuild_run_id, entity_type, entity_key, rule_code, severity, disposition, evidence)
SELECT :rebuild_rebuild_run_id, 'match', match_id::text, publication_reason,
       CASE WHEN publication_reason = 'unresolved_team_identity' THEN 'error' ELSE 'warning' END,
       'quarantined',
       jsonb_build_object('competition_key', competition_key, 'edition_key', edition_key, 'date_utc', date_utc)
FROM mart_v2.fact_match
WHERE publication_state <> 'published'
ON CONFLICT (rebuild_run_id, entity_type, entity_key, rule_code) DO NOTHING;

INSERT INTO control.coverage_snapshot (
  rebuild_run_id, entity_type, source_system, competition_key, edition_key,
  observed_rows, accepted_rows, quarantined_rows, first_date, last_date, metadata
)
SELECT :rebuild_rebuild_run_id, 'match', source_system, competition_key, edition_key,
       count(*), count(*) FILTER (WHERE publication_state = 'published'),
       count(*) FILTER (WHERE publication_state <> 'published'),
       min(match_date), max(match_date), '{}'::jsonb
FROM mart_v2.fact_match
GROUP BY source_system, competition_key, edition_key
ON CONFLICT (rebuild_run_id, entity_type, source_system, competition_key, edition_key) DO UPDATE
SET observed_rows = EXCLUDED.observed_rows,
    accepted_rows = EXCLUDED.accepted_rows,
    quarantined_rows = EXCLUDED.quarantined_rows,
    first_date = EXCLUDED.first_date,
    last_date = EXCLUDED.last_date;

UPDATE control.rebuild_run
SET phase = 'core_structure', status = 'succeeded', finished_at = now()
WHERE rebuild_run_id = :rebuild_rebuild_run_id;
