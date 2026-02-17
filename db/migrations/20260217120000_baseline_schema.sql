-- migrate:up
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS raw.fixtures (
  fixture_id     BIGINT PRIMARY KEY,
  date_utc       TIMESTAMPTZ,
  timestamp      BIGINT,
  timezone       TEXT,
  referee        TEXT,
  venue_id       BIGINT,
  venue_name     TEXT,
  venue_city     TEXT,
  status_short   TEXT,
  status_long    TEXT,
  league_id      BIGINT,
  league_name    TEXT,
  season         INT,
  round          TEXT,
  home_team_id   BIGINT,
  home_team_name TEXT,
  away_team_id   BIGINT,
  away_team_name TEXT,
  home_goals     INT,
  away_goals     INT,
  year           TEXT,
  month          TEXT,
  ingested_run   TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_fixtures_date ON raw.fixtures (date_utc);
CREATE INDEX IF NOT EXISTS idx_raw_fixtures_home ON raw.fixtures (home_team_id);
CREATE INDEX IF NOT EXISTS idx_raw_fixtures_away ON raw.fixtures (away_team_id);

CREATE TABLE IF NOT EXISTS raw.match_statistics (
  fixture_id         BIGINT NOT NULL,
  team_id            BIGINT NOT NULL,
  team_name          TEXT,
  shots_on_goal      INT,
  shots_off_goal     INT,
  total_shots        INT,
  blocked_shots      INT,
  shots_inside_box   INT,
  shots_outside_box  INT,
  fouls              INT,
  corner_kicks       INT,
  offsides           INT,
  ball_possession    INT,
  yellow_cards       INT,
  red_cards          INT,
  goalkeeper_saves   INT,
  total_passes       INT,
  passes_accurate    INT,
  passes_pct         NUMERIC(5,2),
  ingested_run       TEXT,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_match_statistics PRIMARY KEY (fixture_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_match_statistics_fixture
  ON raw.match_statistics (fixture_id);

CREATE INDEX IF NOT EXISTS idx_match_statistics_team
  ON raw.match_statistics (team_id);

DO $$
BEGIN
  IF to_regclass('raw.match_events') IS NULL THEN
    CREATE TABLE raw.match_events (
      event_id      TEXT NOT NULL,
      season        INT NOT NULL,
      fixture_id    BIGINT NOT NULL,
      time_elapsed  INT,
      time_extra    INT,
      team_id       BIGINT,
      team_name     TEXT,
      player_id     BIGINT,
      player_name   TEXT,
      assist_id     BIGINT,
      assist_name   TEXT,
      type          TEXT,
      detail        TEXT,
      comments      TEXT,
      ingested_run  TEXT,
      updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
      CONSTRAINT pk_match_events PRIMARY KEY (event_id, season),
      CONSTRAINT fk_match_events_fixture
        FOREIGN KEY (fixture_id) REFERENCES raw.fixtures (fixture_id)
    ) PARTITION BY LIST (season);
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS raw.match_events_2024
  PARTITION OF raw.match_events
  FOR VALUES IN (2024);

CREATE INDEX IF NOT EXISTS idx_raw_match_events_fixture_id
  ON raw.match_events (fixture_id);

CREATE INDEX IF NOT EXISTS idx_raw_match_events_team_id
  ON raw.match_events (team_id);

CREATE INDEX IF NOT EXISTS idx_raw_match_events_player_id
  ON raw.match_events (player_id);

CREATE TABLE IF NOT EXISTS mart.team_match_goals_monthly (
  season        INT NOT NULL,
  year          TEXT NOT NULL,
  month         TEXT NOT NULL,
  team_id       BIGINT,
  team_name     TEXT NOT NULL,
  goals_for     INT NOT NULL,
  goals_against INT NOT NULL,
  matches       INT NOT NULL,
  wins          INT NOT NULL,
  draws         INT NOT NULL,
  losses        INT NOT NULL,
  points        INT NOT NULL,
  goal_diff     INT NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_team_match_goals_monthly PRIMARY KEY (season, year, month, team_name)
);

CREATE INDEX IF NOT EXISTS idx_team_match_goals_monthly_period
  ON mart.team_match_goals_monthly (season, year, month);

CREATE INDEX IF NOT EXISTS idx_team_match_goals_monthly_team_name
  ON mart.team_match_goals_monthly (team_name);

CREATE TABLE IF NOT EXISTS mart.league_summary (
  league_id           BIGINT NOT NULL,
  league_name         TEXT NOT NULL,
  season              INT NOT NULL,
  total_matches       INT NOT NULL,
  total_goals         INT NOT NULL,
  avg_goals_per_match NUMERIC(10,4) NOT NULL,
  first_match_date    DATE,
  last_match_date     DATE,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_league_summary PRIMARY KEY (league_id, season)
);

CREATE INDEX IF NOT EXISTS idx_league_summary_season
  ON mart.league_summary (season);

CREATE TABLE IF NOT EXISTS mart.standings_evolution (
  season                    INT NOT NULL,
  round                     INT NOT NULL,
  team_id                   BIGINT NOT NULL,
  points_accumulated        INT NOT NULL,
  goals_for_accumulated     INT NOT NULL,
  goal_diff_accumulated     INT NOT NULL,
  position                  INT NOT NULL,
  updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_standings_evolution PRIMARY KEY (season, round, team_id)
);

CREATE INDEX IF NOT EXISTS idx_standings_evolution_season_round_position
  ON mart.standings_evolution (season, round, position);

CREATE INDEX IF NOT EXISTS idx_standings_evolution_team
  ON mart.standings_evolution (team_id);

CREATE TABLE IF NOT EXISTS mart.team_performance_monthly (
  season               INT NOT NULL,
  year                 TEXT NOT NULL,
  month                TEXT NOT NULL,
  team_id              BIGINT NOT NULL,
  avg_ball_possession  NUMERIC(6,2),
  total_shots          INT NOT NULL,
  shots_on_target      INT NOT NULL,
  conversion_rate      NUMERIC(8,2),
  pass_accuracy        NUMERIC(6,2),
  fouls_committed      INT NOT NULL,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_team_performance_monthly PRIMARY KEY (season, year, month, team_id)
);

CREATE INDEX IF NOT EXISTS idx_team_performance_monthly_period
  ON mart.team_performance_monthly (season, year, month);

CREATE INDEX IF NOT EXISTS idx_team_performance_monthly_team
  ON mart.team_performance_monthly (team_id);

CREATE TABLE IF NOT EXISTS gold.dim_team (
  team_id      BIGINT PRIMARY KEY,
  team_name    TEXT NOT NULL,
  logo_url     TEXT,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.dim_venue (
  venue_id     BIGINT PRIMARY KEY,
  venue_name   TEXT NOT NULL,
  venue_city   TEXT,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.dim_competition (
  league_id    BIGINT PRIMARY KEY,
  league_name  TEXT NOT NULL,
  country      TEXT,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.dim_player (
  player_id    BIGINT PRIMARY KEY,
  player_name  TEXT NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
  date_day          DATE PRIMARY KEY,
  year              INT NOT NULL,
  month             INT NOT NULL,
  day               INT NOT NULL,
  day_of_week_name  TEXT NOT NULL,
  is_weekend        BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.fact_matches (
  match_id               BIGINT PRIMARY KEY,
  league_id              BIGINT NOT NULL,
  season                 INT NOT NULL,
  date_day               DATE NOT NULL,
  home_team_id           BIGINT NOT NULL,
  away_team_id           BIGINT NOT NULL,
  venue_id               BIGINT,
  home_goals             INT,
  away_goals             INT,
  total_goals            INT,
  result                 TEXT,
  home_shots             INT,
  home_shots_on_target   INT,
  home_possession        INT,
  home_corners           INT,
  home_fouls             INT,
  away_shots             INT,
  away_shots_on_target   INT,
  away_possession        INT,
  away_corners           INT,
  away_fouls             INT,
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT fk_fact_matches_competition
    FOREIGN KEY (league_id) REFERENCES gold.dim_competition (league_id),
  CONSTRAINT fk_fact_matches_date
    FOREIGN KEY (date_day) REFERENCES gold.dim_date (date_day),
  CONSTRAINT fk_fact_matches_home_team
    FOREIGN KEY (home_team_id) REFERENCES gold.dim_team (team_id),
  CONSTRAINT fk_fact_matches_away_team
    FOREIGN KEY (away_team_id) REFERENCES gold.dim_team (team_id),
  CONSTRAINT fk_fact_matches_venue
    FOREIGN KEY (venue_id) REFERENCES gold.dim_venue (venue_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_matches_league_id
  ON gold.fact_matches (league_id);

CREATE INDEX IF NOT EXISTS idx_fact_matches_season
  ON gold.fact_matches (season);

CREATE INDEX IF NOT EXISTS idx_fact_matches_date_day
  ON gold.fact_matches (date_day);

CREATE INDEX IF NOT EXISTS idx_fact_matches_home_team_id
  ON gold.fact_matches (home_team_id);

CREATE INDEX IF NOT EXISTS idx_fact_matches_away_team_id
  ON gold.fact_matches (away_team_id);

CREATE INDEX IF NOT EXISTS idx_fact_matches_venue_id
  ON gold.fact_matches (venue_id);

CREATE TABLE IF NOT EXISTS gold.fact_match_events (
  event_id            TEXT PRIMARY KEY,
  match_id            BIGINT NOT NULL,
  team_id             BIGINT,
  player_id           BIGINT,
  assist_player_id    BIGINT,
  time_elapsed        INT,
  time_extra          INT,
  event_type          TEXT,
  event_detail        TEXT,
  is_goal             BOOLEAN NOT NULL DEFAULT false,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT fk_fact_match_events_match
    FOREIGN KEY (match_id) REFERENCES gold.fact_matches (match_id),
  CONSTRAINT fk_fact_match_events_team
    FOREIGN KEY (team_id) REFERENCES gold.dim_team (team_id),
  CONSTRAINT fk_fact_match_events_player
    FOREIGN KEY (player_id) REFERENCES gold.dim_player (player_id),
  CONSTRAINT fk_fact_match_events_assist_player
    FOREIGN KEY (assist_player_id) REFERENCES gold.dim_player (player_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_match_events_match_id
  ON gold.fact_match_events (match_id);

CREATE INDEX IF NOT EXISTS idx_fact_match_events_team_id
  ON gold.fact_match_events (team_id);

CREATE INDEX IF NOT EXISTS idx_fact_match_events_player_id
  ON gold.fact_match_events (player_id);

CREATE INDEX IF NOT EXISTS idx_fact_match_events_assist_player_id
  ON gold.fact_match_events (assist_player_id);

-- migrate:down
SELECT 1;
