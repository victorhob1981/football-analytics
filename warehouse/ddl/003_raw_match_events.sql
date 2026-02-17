CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.match_events CASCADE;

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

CREATE TABLE IF NOT EXISTS raw.match_events_2024
  PARTITION OF raw.match_events
  FOR VALUES IN (2024);

CREATE INDEX IF NOT EXISTS idx_raw_match_events_fixture_id
  ON raw.match_events (fixture_id);

CREATE INDEX IF NOT EXISTS idx_raw_match_events_team_id
  ON raw.match_events (team_id);

CREATE INDEX IF NOT EXISTS idx_raw_match_events_player_id
  ON raw.match_events (player_id);
