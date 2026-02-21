-- migrate:up
-- P3 foundation: entidades avancadas SportMonks (competition structure, lineups,
-- player stats, transfers, availability, coaches, head-to-head).

ALTER TABLE raw.fixtures
  ADD COLUMN IF NOT EXISTS source_provider TEXT,
  ADD COLUMN IF NOT EXISTS referee_id BIGINT,
  ADD COLUMN IF NOT EXISTS stage_id BIGINT,
  ADD COLUMN IF NOT EXISTS round_id BIGINT,
  ADD COLUMN IF NOT EXISTS attendance INT,
  ADD COLUMN IF NOT EXISTS weather_description TEXT,
  ADD COLUMN IF NOT EXISTS weather_temperature_c NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS weather_wind_kph NUMERIC(6,2),
  ADD COLUMN IF NOT EXISTS home_goals_ht INT,
  ADD COLUMN IF NOT EXISTS away_goals_ht INT,
  ADD COLUMN IF NOT EXISTS home_goals_ft INT,
  ADD COLUMN IF NOT EXISTS away_goals_ft INT;

CREATE INDEX IF NOT EXISTS idx_raw_fixtures_stage_id
  ON raw.fixtures (stage_id);

CREATE INDEX IF NOT EXISTS idx_raw_fixtures_round_id
  ON raw.fixtures (round_id);

CREATE INDEX IF NOT EXISTS idx_raw_fixtures_source_provider
  ON raw.fixtures (source_provider);

CREATE TABLE IF NOT EXISTS raw.competition_leagues (
  provider      TEXT NOT NULL,
  league_id     BIGINT NOT NULL,
  league_name   TEXT,
  country_id    BIGINT,
  payload       JSONB,
  ingested_run  TEXT,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_competition_leagues PRIMARY KEY (provider, league_id)
);

CREATE INDEX IF NOT EXISTS idx_competition_leagues_league_id
  ON raw.competition_leagues (league_id);

CREATE TABLE IF NOT EXISTS raw.competition_seasons (
  provider      TEXT NOT NULL,
  season_id     BIGINT NOT NULL,
  league_id     BIGINT,
  season_year   INT,
  season_name   TEXT,
  starting_at   DATE,
  ending_at     DATE,
  payload       JSONB,
  ingested_run  TEXT,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_competition_seasons PRIMARY KEY (provider, season_id)
);

CREATE INDEX IF NOT EXISTS idx_competition_seasons_league_id
  ON raw.competition_seasons (league_id);

CREATE INDEX IF NOT EXISTS idx_competition_seasons_year
  ON raw.competition_seasons (season_year);

CREATE TABLE IF NOT EXISTS raw.competition_stages (
  provider      TEXT NOT NULL,
  stage_id      BIGINT NOT NULL,
  season_id     BIGINT,
  league_id     BIGINT,
  stage_name    TEXT,
  sort_order    INT,
  finished      BOOLEAN,
  is_current    BOOLEAN,
  starting_at   DATE,
  ending_at     DATE,
  payload       JSONB,
  ingested_run  TEXT,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_competition_stages PRIMARY KEY (provider, stage_id)
);

CREATE INDEX IF NOT EXISTS idx_competition_stages_season_id
  ON raw.competition_stages (season_id);

CREATE INDEX IF NOT EXISTS idx_competition_stages_league_id
  ON raw.competition_stages (league_id);

CREATE TABLE IF NOT EXISTS raw.competition_rounds (
  provider          TEXT NOT NULL,
  round_id          BIGINT NOT NULL,
  stage_id          BIGINT,
  season_id         BIGINT,
  league_id         BIGINT,
  round_name        TEXT,
  finished          BOOLEAN,
  is_current        BOOLEAN,
  starting_at       DATE,
  ending_at         DATE,
  games_in_week     INT,
  payload           JSONB,
  ingested_run      TEXT,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_competition_rounds PRIMARY KEY (provider, round_id)
);

CREATE INDEX IF NOT EXISTS idx_competition_rounds_stage_id
  ON raw.competition_rounds (stage_id);

CREATE INDEX IF NOT EXISTS idx_competition_rounds_season_id
  ON raw.competition_rounds (season_id);

CREATE TABLE IF NOT EXISTS raw.standings_snapshots (
  provider          TEXT NOT NULL,
  league_id         BIGINT NOT NULL,
  season_id         BIGINT NOT NULL,
  stage_id          BIGINT NOT NULL DEFAULT 0,
  round_id          BIGINT NOT NULL DEFAULT 0,
  team_id           BIGINT NOT NULL,
  position          INT,
  points            INT,
  games_played      INT,
  won               INT,
  draw              INT,
  lost              INT,
  goals_for         INT,
  goals_against     INT,
  goal_diff         INT,
  payload           JSONB,
  ingested_run      TEXT,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_standings_snapshots PRIMARY KEY (provider, season_id, stage_id, round_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_standings_snapshots_league_season
  ON raw.standings_snapshots (league_id, season_id);

CREATE INDEX IF NOT EXISTS idx_standings_snapshots_team_id
  ON raw.standings_snapshots (team_id);

CREATE TABLE IF NOT EXISTS raw.fixture_lineups (
  provider            TEXT NOT NULL,
  fixture_id          BIGINT NOT NULL,
  team_id             BIGINT NOT NULL,
  player_id           BIGINT NOT NULL,
  lineup_id           BIGINT,
  position_id         BIGINT,
  position_name       TEXT,
  lineup_type_id      INT,
  formation_field     TEXT,
  formation_position  INT,
  jersey_number       INT,
  details             JSONB NOT NULL DEFAULT '[]'::jsonb,
  payload             JSONB,
  ingested_run        TEXT,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_fixture_lineups PRIMARY KEY (provider, fixture_id, team_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_fixture_lineups_fixture
  ON raw.fixture_lineups (fixture_id);

CREATE INDEX IF NOT EXISTS idx_fixture_lineups_team
  ON raw.fixture_lineups (team_id);

CREATE INDEX IF NOT EXISTS idx_fixture_lineups_player
  ON raw.fixture_lineups (player_id);

CREATE TABLE IF NOT EXISTS raw.fixture_player_statistics (
  provider         TEXT NOT NULL,
  fixture_id       BIGINT NOT NULL,
  team_id          BIGINT NOT NULL,
  player_id        BIGINT NOT NULL,
  statistics       JSONB NOT NULL DEFAULT '[]'::jsonb,
  payload          JSONB,
  ingested_run     TEXT,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_fixture_player_statistics PRIMARY KEY (provider, fixture_id, team_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_fixture_player_statistics_fixture
  ON raw.fixture_player_statistics (fixture_id);

CREATE INDEX IF NOT EXISTS idx_fixture_player_statistics_team
  ON raw.fixture_player_statistics (team_id);

CREATE INDEX IF NOT EXISTS idx_fixture_player_statistics_player
  ON raw.fixture_player_statistics (player_id);

CREATE TABLE IF NOT EXISTS raw.player_season_statistics (
  provider         TEXT NOT NULL,
  player_id        BIGINT NOT NULL,
  season_id        BIGINT NOT NULL,
  team_id          BIGINT NOT NULL DEFAULT 0,
  league_id        BIGINT,
  season_name      TEXT,
  position_name    TEXT,
  statistics       JSONB NOT NULL DEFAULT '[]'::jsonb,
  payload          JSONB,
  ingested_run     TEXT,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_player_season_statistics PRIMARY KEY (provider, player_id, season_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_player_season_statistics_player
  ON raw.player_season_statistics (player_id);

CREATE INDEX IF NOT EXISTS idx_player_season_statistics_season
  ON raw.player_season_statistics (season_id);

CREATE INDEX IF NOT EXISTS idx_player_season_statistics_league
  ON raw.player_season_statistics (league_id);

CREATE TABLE IF NOT EXISTS raw.player_transfers (
  provider         TEXT NOT NULL,
  transfer_id      BIGINT NOT NULL,
  player_id        BIGINT NOT NULL,
  from_team_id     BIGINT,
  to_team_id       BIGINT,
  transfer_date    DATE,
  completed        BOOLEAN,
  career_ended     BOOLEAN,
  type_id          BIGINT,
  position_id      BIGINT,
  amount           TEXT,
  payload          JSONB,
  ingested_run     TEXT,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_player_transfers PRIMARY KEY (provider, transfer_id)
);

CREATE INDEX IF NOT EXISTS idx_player_transfers_player
  ON raw.player_transfers (player_id);

CREATE INDEX IF NOT EXISTS idx_player_transfers_date
  ON raw.player_transfers (transfer_date);

CREATE TABLE IF NOT EXISTS raw.team_sidelined (
  provider         TEXT NOT NULL,
  sidelined_id     BIGINT NOT NULL,
  team_id          BIGINT NOT NULL,
  player_id        BIGINT NOT NULL,
  season_id        BIGINT,
  category         TEXT,
  type_id          BIGINT,
  start_date       DATE,
  end_date         DATE,
  games_missed     INT,
  completed        BOOLEAN,
  payload          JSONB,
  ingested_run     TEXT,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_team_sidelined PRIMARY KEY (provider, sidelined_id)
);

CREATE INDEX IF NOT EXISTS idx_team_sidelined_team
  ON raw.team_sidelined (team_id);

CREATE INDEX IF NOT EXISTS idx_team_sidelined_player
  ON raw.team_sidelined (player_id);

CREATE INDEX IF NOT EXISTS idx_team_sidelined_season
  ON raw.team_sidelined (season_id);

CREATE TABLE IF NOT EXISTS raw.team_coaches (
  provider          TEXT NOT NULL,
  coach_tenure_id   BIGINT NOT NULL,
  team_id           BIGINT NOT NULL,
  coach_id          BIGINT NOT NULL,
  position_id       BIGINT,
  active            BOOLEAN,
  temporary         BOOLEAN,
  start_date        DATE,
  end_date          DATE,
  payload           JSONB,
  ingested_run      TEXT,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_team_coaches PRIMARY KEY (provider, coach_tenure_id)
);

CREATE INDEX IF NOT EXISTS idx_team_coaches_team
  ON raw.team_coaches (team_id);

CREATE INDEX IF NOT EXISTS idx_team_coaches_coach
  ON raw.team_coaches (coach_id);

CREATE TABLE IF NOT EXISTS raw.head_to_head_fixtures (
  provider           TEXT NOT NULL,
  pair_team_id       BIGINT NOT NULL,
  pair_opponent_id   BIGINT NOT NULL,
  fixture_id         BIGINT NOT NULL,
  league_id          BIGINT,
  season_id          BIGINT,
  match_date         TIMESTAMPTZ,
  home_team_id       BIGINT,
  away_team_id       BIGINT,
  home_goals         INT,
  away_goals         INT,
  payload            JSONB,
  ingested_run       TEXT,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_head_to_head_fixtures PRIMARY KEY (provider, pair_team_id, pair_opponent_id, fixture_id)
);

CREATE INDEX IF NOT EXISTS idx_head_to_head_fixture
  ON raw.head_to_head_fixtures (fixture_id);

CREATE INDEX IF NOT EXISTS idx_head_to_head_pair
  ON raw.head_to_head_fixtures (pair_team_id, pair_opponent_id);

-- migrate:down
SELECT 1;
