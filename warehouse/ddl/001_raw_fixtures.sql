CREATE SCHEMA IF NOT EXISTS raw;

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

CREATE INDEX IF NOT EXISTS idx_raw_fixtures_date ON raw.fixtures(date_utc);
CREATE INDEX IF NOT EXISTS idx_raw_fixtures_home ON raw.fixtures(home_team_id);
CREATE INDEX IF NOT EXISTS idx_raw_fixtures_away ON raw.fixtures(away_team_id);
