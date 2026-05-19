-- migrate:up
-- Copa do Mundo | Bloco 5
-- Fundacao minima para normalizacao silver inicial do recorte 2022.

CREATE TABLE IF NOT EXISTS silver.wc_fixtures (
  edition_key                TEXT NOT NULL,
  internal_match_id          TEXT NOT NULL,
  source_name                TEXT NOT NULL,
  source_version             TEXT NOT NULL,
  source_match_id            TEXT NOT NULL,
  supporting_source_name     TEXT,
  supporting_source_version  TEXT,
  supporting_source_match_id TEXT,
  stage_internal_id          TEXT NOT NULL,
  stage_key                  TEXT NOT NULL,
  group_internal_id          TEXT,
  group_key                  TEXT,
  match_date                 DATE NOT NULL,
  home_team_internal_id      TEXT NOT NULL,
  away_team_internal_id      TEXT NOT NULL,
  home_team_score            INTEGER NOT NULL,
  away_team_score            INTEGER NOT NULL,
  extra_time                 BOOLEAN NOT NULL DEFAULT FALSE,
  penalty_shootout           BOOLEAN NOT NULL DEFAULT FALSE,
  home_penalty_score         INTEGER,
  away_penalty_score         INTEGER,
  materialized_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_wc_fixtures PRIMARY KEY (edition_key, internal_match_id),
  CONSTRAINT uq_wc_fixtures_source_match UNIQUE (source_name, source_match_id),
  CONSTRAINT chk_wc_fixtures_source_name
    CHECK (source_name = 'fjelstul_worldcup')
);

CREATE INDEX IF NOT EXISTS idx_wc_fixtures_stage_key
  ON silver.wc_fixtures (edition_key, stage_key);

CREATE INDEX IF NOT EXISTS idx_wc_fixtures_group_key
  ON silver.wc_fixtures (edition_key, group_key);

CREATE INDEX IF NOT EXISTS idx_wc_fixtures_match_date
  ON silver.wc_fixtures (match_date);

CREATE TABLE IF NOT EXISTS silver.wc_stages (
  edition_key                TEXT NOT NULL,
  stage_internal_id          TEXT NOT NULL,
  stage_key                  TEXT NOT NULL,
  stage_name                 TEXT NOT NULL,
  stage_type                 TEXT NOT NULL,
  stage_order                INTEGER NOT NULL,
  source_name                TEXT NOT NULL,
  source_version             TEXT NOT NULL,
  supporting_source_name     TEXT,
  supporting_source_version  TEXT,
  materialized_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_wc_stages PRIMARY KEY (edition_key, stage_internal_id),
  CONSTRAINT uq_wc_stages_stage_key UNIQUE (edition_key, stage_key),
  CONSTRAINT chk_wc_stages_source_name
    CHECK (source_name = 'fjelstul_worldcup'),
  CONSTRAINT chk_wc_stages_stage_type
    CHECK (stage_type IN ('group_stage', 'knockout_stage'))
);

CREATE TABLE IF NOT EXISTS silver.wc_groups (
  edition_key        TEXT NOT NULL,
  group_internal_id  TEXT NOT NULL,
  stage_internal_id  TEXT NOT NULL,
  stage_key          TEXT NOT NULL,
  group_key          TEXT NOT NULL,
  group_name         TEXT NOT NULL,
  count_teams        INTEGER NOT NULL,
  source_name        TEXT NOT NULL,
  source_version     TEXT NOT NULL,
  source_group_id    TEXT NOT NULL,
  materialized_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_wc_groups PRIMARY KEY (edition_key, group_internal_id),
  CONSTRAINT uq_wc_groups_stage_group UNIQUE (edition_key, stage_key, group_key),
  CONSTRAINT chk_wc_groups_source_name
    CHECK (source_name = 'fjelstul_worldcup')
);

CREATE INDEX IF NOT EXISTS idx_wc_groups_stage_key
  ON silver.wc_groups (edition_key, stage_key);

CREATE TABLE IF NOT EXISTS silver.wc_group_standings (
  edition_key          TEXT NOT NULL,
  stage_internal_id    TEXT NOT NULL,
  stage_key            TEXT NOT NULL,
  group_internal_id    TEXT NOT NULL,
  group_key            TEXT NOT NULL,
  team_internal_id     TEXT NOT NULL,
  source_name          TEXT NOT NULL,
  source_version       TEXT NOT NULL,
  source_row_id        TEXT NOT NULL,
  final_position       INTEGER NOT NULL,
  team_name            TEXT NOT NULL,
  team_code            TEXT NOT NULL,
  played               INTEGER NOT NULL,
  wins                 INTEGER NOT NULL,
  draws                INTEGER NOT NULL,
  losses               INTEGER NOT NULL,
  goals_for            INTEGER NOT NULL,
  goals_against        INTEGER NOT NULL,
  goal_difference      INTEGER NOT NULL,
  points               INTEGER NOT NULL,
  advanced             BOOLEAN NOT NULL,
  materialized_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_wc_group_standings PRIMARY KEY (edition_key, stage_key, group_key, team_internal_id),
  CONSTRAINT uq_wc_group_standings_source_row UNIQUE (source_name, source_row_id),
  CONSTRAINT chk_wc_group_standings_source_name
    CHECK (source_name = 'fjelstul_worldcup')
);

CREATE INDEX IF NOT EXISTS idx_wc_group_standings_group
  ON silver.wc_group_standings (edition_key, stage_key, group_key);

CREATE TABLE IF NOT EXISTS silver.wc_lineups (
  edition_key         TEXT NOT NULL,
  internal_match_id   TEXT NOT NULL,
  team_internal_id    TEXT NOT NULL,
  player_internal_id  TEXT NOT NULL,
  source_name         TEXT NOT NULL,
  source_version      TEXT NOT NULL,
  source_match_id     TEXT NOT NULL,
  source_team_id      TEXT NOT NULL,
  source_player_id    TEXT NOT NULL,
  team_name           TEXT NOT NULL,
  player_name         TEXT NOT NULL,
  player_nickname     TEXT,
  jersey_number       INTEGER,
  is_starter          BOOLEAN NOT NULL,
  start_reason        TEXT,
  first_position_name TEXT,
  first_position_id   INTEGER,
  payload             JSONB NOT NULL,
  materialized_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_wc_lineups PRIMARY KEY (edition_key, internal_match_id, team_internal_id, player_internal_id),
  CONSTRAINT chk_wc_lineups_source_name
    CHECK (source_name = 'statsbomb_open_data')
);

CREATE INDEX IF NOT EXISTS idx_wc_lineups_match_team
  ON silver.wc_lineups (edition_key, internal_match_id, team_internal_id);

CREATE INDEX IF NOT EXISTS idx_wc_lineups_starters
  ON silver.wc_lineups (edition_key, internal_match_id, team_internal_id, is_starter);

CREATE TABLE IF NOT EXISTS silver.wc_match_events (
  edition_key         TEXT NOT NULL,
  internal_match_id   TEXT NOT NULL,
  source_name         TEXT NOT NULL,
  source_version      TEXT NOT NULL,
  source_match_id     TEXT NOT NULL,
  source_event_id     TEXT NOT NULL,
  event_index         INTEGER NOT NULL,
  team_internal_id    TEXT,
  player_internal_id  TEXT,
  event_type_id       INTEGER,
  event_type          TEXT NOT NULL,
  period              INTEGER,
  minute              INTEGER,
  second              NUMERIC(8,3),
  timestamp_label     TEXT,
  possession          INTEGER,
  play_pattern        TEXT,
  location_x          NUMERIC(10,4),
  location_y          NUMERIC(10,4),
  has_three_sixty_frame BOOLEAN NOT NULL DEFAULT FALSE,
  payload             JSONB NOT NULL,
  materialized_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_wc_match_events PRIMARY KEY (source_name, source_match_id, source_event_id),
  CONSTRAINT uq_wc_match_events_match_index UNIQUE (edition_key, internal_match_id, event_index),
  CONSTRAINT chk_wc_match_events_source_name
    CHECK (source_name = 'statsbomb_open_data')
);

CREATE INDEX IF NOT EXISTS idx_wc_match_events_match
  ON silver.wc_match_events (edition_key, internal_match_id);

CREATE INDEX IF NOT EXISTS idx_wc_match_events_team
  ON silver.wc_match_events (team_internal_id);

CREATE INDEX IF NOT EXISTS idx_wc_match_events_player
  ON silver.wc_match_events (player_internal_id);

CREATE INDEX IF NOT EXISTS idx_wc_match_events_type
  ON silver.wc_match_events (event_type);

-- migrate:down
SELECT 1;
