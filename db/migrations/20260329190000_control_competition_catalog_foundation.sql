-- migrate:up
-- Foundation canônica mínima do catálogo control para permitir replay limpo
-- dos seeds de competições e seasons.

CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.competitions (
  competition_key     TEXT NOT NULL,
  competition_name    TEXT NOT NULL,
  competition_type    TEXT NOT NULL,
  country_name        TEXT,
  confederation_name  TEXT,
  tier                SMALLINT,
  is_active           BOOLEAN NOT NULL DEFAULT TRUE,
  display_priority    INTEGER NOT NULL DEFAULT 100,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT competitions_pkey PRIMARY KEY (competition_key),
  CONSTRAINT chk_control_competitions_key
    CHECK (competition_key ~ '^[a-z0-9_]+$'),
  CONSTRAINT chk_control_competitions_type
    CHECK (competition_type IN ('league', 'cup', 'continental_cup'))
);

CREATE TABLE IF NOT EXISTS control.competition_provider_map (
  competition_key      TEXT NOT NULL,
  provider             TEXT NOT NULL,
  provider_league_id   BIGINT NOT NULL,
  provider_name        TEXT,
  is_active            BOOLEAN NOT NULL DEFAULT TRUE,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_control_competition_provider_map PRIMARY KEY (competition_key, provider),
  CONSTRAINT uq_control_competition_provider_map_provider_league UNIQUE (provider, provider_league_id),
  CONSTRAINT fk_control_competition_provider_map_competition
    FOREIGN KEY (competition_key) REFERENCES control.competitions (competition_key)
);

CREATE TABLE IF NOT EXISTS control.season_catalog (
  competition_key      TEXT NOT NULL,
  season_label         TEXT NOT NULL,
  season_start_date    DATE,
  season_end_date      DATE,
  is_closed            BOOLEAN NOT NULL DEFAULT FALSE,
  provider             TEXT NOT NULL,
  provider_season_id   BIGINT,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_control_season_catalog PRIMARY KEY (competition_key, season_label, provider),
  CONSTRAINT chk_control_season_catalog_label
    CHECK (season_label ~ '^[0-9]{4}(_[0-9]{2})?$'),
  CONSTRAINT fk_control_season_catalog_competition
    FOREIGN KEY (competition_key) REFERENCES control.competitions (competition_key)
);

CREATE INDEX IF NOT EXISTS idx_control_competitions_priority
  ON control.competitions (display_priority);

CREATE INDEX IF NOT EXISTS idx_control_competitions_type
  ON control.competitions (competition_type);

CREATE INDEX IF NOT EXISTS idx_control_season_catalog_provider_season_id
  ON control.season_catalog (provider, provider_season_id);

-- migrate:down
SELECT 1;
