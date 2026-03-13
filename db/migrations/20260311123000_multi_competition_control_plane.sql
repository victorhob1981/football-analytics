-- migrate:up
CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.competitions (
  competition_key  TEXT PRIMARY KEY,
  competition_name TEXT NOT NULL,
  competition_type TEXT NOT NULL,
  country_name     TEXT,
  confederation_name TEXT,
  tier             SMALLINT,
  is_active        BOOLEAN NOT NULL DEFAULT TRUE,
  display_priority INT NOT NULL DEFAULT 100,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_control_competitions_key
    CHECK (competition_key ~ '^[a-z0-9_]+$'),
  CONSTRAINT chk_control_competitions_type
    CHECK (competition_type IN ('league', 'cup', 'continental_cup'))
);

CREATE TABLE IF NOT EXISTS control.competition_provider_map (
  competition_key    TEXT NOT NULL,
  provider           TEXT NOT NULL,
  provider_league_id BIGINT NOT NULL,
  provider_name      TEXT,
  is_active          BOOLEAN NOT NULL DEFAULT TRUE,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_control_competition_provider_map PRIMARY KEY (competition_key, provider),
  CONSTRAINT fk_control_competition_provider_map_competition
    FOREIGN KEY (competition_key) REFERENCES control.competitions (competition_key),
  CONSTRAINT uq_control_competition_provider_map_provider_league
    UNIQUE (provider, provider_league_id)
);

CREATE TABLE IF NOT EXISTS control.season_catalog (
  competition_key    TEXT NOT NULL,
  season_label       TEXT NOT NULL,
  season_start_date  DATE,
  season_end_date    DATE,
  is_closed          BOOLEAN NOT NULL DEFAULT FALSE,
  provider           TEXT NOT NULL,
  provider_season_id BIGINT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_control_season_catalog PRIMARY KEY (competition_key, season_label, provider),
  CONSTRAINT fk_control_season_catalog_competition
    FOREIGN KEY (competition_key) REFERENCES control.competitions (competition_key),
  CONSTRAINT chk_control_season_catalog_label
    CHECK (season_label ~ '^[0-9]{4}(_[0-9]{2})?$')
);

CREATE TABLE IF NOT EXISTS control.backfill_manifest (
  manifest_id        BIGSERIAL PRIMARY KEY,
  competition_key    TEXT NOT NULL,
  season_label       TEXT NOT NULL,
  provider           TEXT NOT NULL,
  provider_league_id BIGINT,
  provider_season_id BIGINT,
  enabled_domains    JSONB NOT NULL DEFAULT '[]'::jsonb,
  expected_status    TEXT NOT NULL DEFAULT 'closed',
  priority           INT NOT NULL DEFAULT 100,
  backfill_status    TEXT NOT NULL DEFAULT 'not_started',
  last_success_at    TIMESTAMPTZ,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT fk_control_backfill_manifest_season
    FOREIGN KEY (competition_key, season_label, provider)
    REFERENCES control.season_catalog (competition_key, season_label, provider),
  CONSTRAINT uq_control_backfill_manifest_scope
    UNIQUE (competition_key, season_label, provider),
  CONSTRAINT chk_control_backfill_manifest_expected_status
    CHECK (expected_status IN ('closed', 'open', 'unknown')),
  CONSTRAINT chk_control_backfill_manifest_backfill_status
    CHECK (backfill_status IN ('not_started', 'in_progress', 'loaded_raw', 'validated', 'ready_for_frontend', 'partial_coverage', 'blocked')),
  CONSTRAINT chk_control_backfill_manifest_enabled_domains_array
    CHECK (jsonb_typeof(enabled_domains) = 'array')
);

CREATE TABLE IF NOT EXISTS control.backfill_runs (
  backfill_run_id BIGSERIAL PRIMARY KEY,
  manifest_id     BIGINT NOT NULL,
  domain          TEXT NOT NULL,
  run_status      TEXT NOT NULL DEFAULT 'started',
  airflow_dag_id  TEXT,
  airflow_run_id  TEXT,
  source_run_id   TEXT,
  started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at     TIMESTAMPTZ,
  rows_in         BIGINT,
  rows_out        BIGINT,
  inserted_count  BIGINT,
  updated_count   BIGINT,
  metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message   TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT fk_control_backfill_runs_manifest
    FOREIGN KEY (manifest_id) REFERENCES control.backfill_manifest (manifest_id),
  CONSTRAINT chk_control_backfill_runs_status
    CHECK (run_status IN ('started', 'success', 'failed', 'skipped')),
  CONSTRAINT chk_control_backfill_runs_metadata_object
    CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_control_competitions_type
  ON control.competitions (competition_type);

CREATE INDEX IF NOT EXISTS idx_control_competitions_priority
  ON control.competitions (display_priority);

CREATE INDEX IF NOT EXISTS idx_control_season_catalog_provider_season_id
  ON control.season_catalog (provider, provider_season_id);

CREATE INDEX IF NOT EXISTS idx_control_backfill_manifest_status_priority
  ON control.backfill_manifest (backfill_status, priority, competition_key, season_label);

CREATE INDEX IF NOT EXISTS idx_control_backfill_runs_manifest_domain
  ON control.backfill_runs (manifest_id, domain, started_at DESC);

INSERT INTO control.competitions (
  competition_key,
  competition_name,
  competition_type,
  country_name,
  confederation_name,
  tier,
  is_active,
  display_priority
)
VALUES
  ('premier_league', 'Premier League', 'league', 'England', 'UEFA', 1, TRUE, 10),
  ('la_liga', 'La Liga', 'league', 'Spain', 'UEFA', 1, TRUE, 20),
  ('serie_a_it', 'Serie A', 'league', 'Italy', 'UEFA', 1, TRUE, 30),
  ('bundesliga', 'Bundesliga', 'league', 'Germany', 'UEFA', 1, TRUE, 40),
  ('ligue_1', 'Ligue 1', 'league', 'France', 'UEFA', 1, TRUE, 50),
  ('champions_league', 'UEFA Champions League', 'continental_cup', NULL, 'UEFA', NULL, TRUE, 60),
  ('libertadores', 'CONMEBOL Libertadores', 'continental_cup', NULL, 'CONMEBOL', NULL, TRUE, 70),
  ('brasileirao_a', 'Campeonato Brasileiro Serie A', 'league', 'Brazil', 'CONMEBOL', 1, TRUE, 80),
  ('brasileirao_b', 'Campeonato Brasileiro Serie B', 'league', 'Brazil', 'CONMEBOL', 2, TRUE, 90),
  ('copa_do_brasil', 'Copa do Brasil', 'cup', 'Brazil', 'CONMEBOL', NULL, TRUE, 100)
ON CONFLICT (competition_key) DO UPDATE
SET
  competition_name = EXCLUDED.competition_name,
  competition_type = EXCLUDED.competition_type,
  country_name = EXCLUDED.country_name,
  confederation_name = EXCLUDED.confederation_name,
  tier = EXCLUDED.tier,
  is_active = EXCLUDED.is_active,
  display_priority = EXCLUDED.display_priority,
  updated_at = now();

-- migrate:down
DROP TABLE IF EXISTS control.backfill_runs;
DROP TABLE IF EXISTS control.backfill_manifest;
DROP TABLE IF EXISTS control.season_catalog;
DROP TABLE IF EXISTS control.competition_provider_map;
DROP TABLE IF EXISTS control.competitions;
DROP SCHEMA IF EXISTS control;
