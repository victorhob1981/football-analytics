-- migrate:up
-- Contract for one-shot historical competition statistics sourced from Wikipedia.
-- `competition_key` remains the canonical project key; provider ids are auxiliary.

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS control.historical_stat_definitions (
  stat_code     TEXT NOT NULL,
  stat_group    TEXT NOT NULL,
  display_name  TEXT NOT NULL,
  description   TEXT,
  entity_type   TEXT,
  value_unit    TEXT,
  is_ranking    BOOLEAN NOT NULL DEFAULT FALSE,
  is_active     BOOLEAN NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_historical_stat_definitions PRIMARY KEY (stat_code),
  CONSTRAINT chk_historical_stat_definitions_stat_code
    CHECK (stat_code ~ '^[a-z0-9_]+$'),
  CONSTRAINT chk_historical_stat_definitions_group
    CHECK (stat_group IN ('champions', 'scorers', 'team_records', 'match_records', 'player_records')),
  CONSTRAINT chk_historical_stat_definitions_entity_type
    CHECK (entity_type IS NULL OR entity_type IN ('team', 'player', 'match')),
  CONSTRAINT chk_historical_stat_definitions_display_name
    CHECK (btrim(display_name) <> '')
);

CREATE TABLE IF NOT EXISTS control.competition_wiki_mapping (
  competition_key   TEXT NOT NULL,
  competition_id    BIGINT,
  competition_name  TEXT NOT NULL,
  wiki_main_url     TEXT NOT NULL,
  wiki_scorers_url  TEXT,
  wiki_records_url  TEXT,
  is_active         BOOLEAN NOT NULL DEFAULT TRUE,
  notes             TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_competition_wiki_mapping PRIMARY KEY (competition_key),
  CONSTRAINT fk_competition_wiki_mapping_competition
    FOREIGN KEY (competition_key) REFERENCES control.competitions (competition_key),
  CONSTRAINT chk_competition_wiki_mapping_name
    CHECK (btrim(competition_name) <> ''),
  CONSTRAINT chk_competition_wiki_mapping_main_url
    CHECK (wiki_main_url ~ '^https?://'),
  CONSTRAINT chk_competition_wiki_mapping_scorers_url
    CHECK (wiki_scorers_url IS NULL OR wiki_scorers_url ~ '^https?://'),
  CONSTRAINT chk_competition_wiki_mapping_records_url
    CHECK (wiki_records_url IS NULL OR wiki_records_url ~ '^https?://')
);

CREATE TABLE IF NOT EXISTS raw.wikipedia_competition_tables (
  id                BIGSERIAL PRIMARY KEY,
  ingestion_run_id  TEXT NOT NULL,
  competition_key   TEXT NOT NULL,
  source_url        TEXT NOT NULL,
  table_index       INTEGER,
  table_caption     TEXT,
  source_hash       TEXT,
  payload_jsonb     JSONB,
  fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  parse_status      TEXT NOT NULL,
  error_message     TEXT,
  metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT fk_wikipedia_competition_tables_mapping
    FOREIGN KEY (competition_key) REFERENCES control.competition_wiki_mapping (competition_key),
  CONSTRAINT chk_wikipedia_competition_tables_run_id
    CHECK (btrim(ingestion_run_id) <> ''),
  CONSTRAINT chk_wikipedia_competition_tables_source_url
    CHECK (source_url ~ '^https?://'),
  CONSTRAINT chk_wikipedia_competition_tables_table_index
    CHECK (table_index IS NULL OR table_index >= 0),
  CONSTRAINT chk_wikipedia_competition_tables_status
    CHECK (parse_status IN ('success', 'failed_fetch', 'no_table', 'unsupported_structure', 'parsed_empty', 'failed_parse')),
  CONSTRAINT chk_wikipedia_competition_tables_metadata_object
    CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE IF NOT EXISTS mart.competition_historical_stats (
  id                BIGSERIAL PRIMARY KEY,
  competition_key   TEXT NOT NULL,
  competition_id    BIGINT,
  stat_code         TEXT NOT NULL,
  stat_group        TEXT NOT NULL,
  entity_type       TEXT,
  entity_id         BIGINT,
  entity_name       TEXT,
  value_numeric     NUMERIC,
  value_label       TEXT,
  rank              INTEGER,
  season_label      TEXT,
  occurred_on       DATE,
  source            TEXT NOT NULL DEFAULT 'wikipedia',
  source_url        TEXT NOT NULL,
  as_of_year        INTEGER NOT NULL,
  record_key        TEXT NOT NULL,
  metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
  ingestion_run_id  TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT fk_competition_historical_stats_competition
    FOREIGN KEY (competition_key) REFERENCES control.competitions (competition_key),
  CONSTRAINT fk_competition_historical_stats_definition
    FOREIGN KEY (stat_code) REFERENCES control.historical_stat_definitions (stat_code),
  CONSTRAINT chk_competition_historical_stats_group
    CHECK (stat_group IN ('champions', 'scorers', 'team_records', 'match_records', 'player_records')),
  CONSTRAINT chk_competition_historical_stats_entity_type
    CHECK (entity_type IS NULL OR entity_type IN ('team', 'player', 'match')),
  CONSTRAINT chk_competition_historical_stats_rank
    CHECK (rank IS NULL OR rank > 0),
  CONSTRAINT chk_competition_historical_stats_source
    CHECK (source = 'wikipedia'),
  CONSTRAINT chk_competition_historical_stats_source_url
    CHECK (source_url ~ '^https?://'),
  CONSTRAINT chk_competition_historical_stats_as_of_year
    CHECK (as_of_year >= 1800),
  CONSTRAINT chk_competition_historical_stats_record_key
    CHECK (btrim(record_key) <> ''),
  CONSTRAINT chk_competition_historical_stats_run_id
    CHECK (btrim(ingestion_run_id) <> ''),
  CONSTRAINT chk_competition_historical_stats_metadata_object
    CHECK (jsonb_typeof(metadata) = 'object'),
  CONSTRAINT uq_competition_historical_stats_record
    UNIQUE (competition_key, stat_code, as_of_year, source, record_key)
);

CREATE INDEX IF NOT EXISTS idx_historical_stat_definitions_group
  ON control.historical_stat_definitions (stat_group);

CREATE INDEX IF NOT EXISTS idx_competition_wiki_mapping_active
  ON control.competition_wiki_mapping (is_active, competition_key);

CREATE INDEX IF NOT EXISTS idx_wikipedia_competition_tables_run
  ON raw.wikipedia_competition_tables (ingestion_run_id, competition_key);

CREATE INDEX IF NOT EXISTS idx_wikipedia_competition_tables_status
  ON raw.wikipedia_competition_tables (parse_status, competition_key);

CREATE INDEX IF NOT EXISTS idx_competition_historical_stats_lookup
  ON mart.competition_historical_stats (competition_key, as_of_year, stat_group, stat_code, rank);

CREATE INDEX IF NOT EXISTS idx_competition_historical_stats_entity
  ON mart.competition_historical_stats (entity_type, entity_id)
  WHERE entity_id IS NOT NULL;

-- migrate:down
DROP TABLE IF EXISTS mart.competition_historical_stats;
DROP TABLE IF EXISTS raw.wikipedia_competition_tables;
DROP TABLE IF EXISTS control.competition_wiki_mapping;
DROP TABLE IF EXISTS control.historical_stat_definitions;
