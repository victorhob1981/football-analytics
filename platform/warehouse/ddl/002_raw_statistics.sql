CREATE SCHEMA IF NOT EXISTS raw;

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
  CONSTRAINT pk_match_statistics PRIMARY KEY (fixture_id, team_id),
  CONSTRAINT fk_match_statistics_fixture
    FOREIGN KEY (fixture_id) REFERENCES raw.fixtures (fixture_id)
);

CREATE INDEX IF NOT EXISTS idx_match_statistics_fixture
  ON raw.match_statistics (fixture_id);

CREATE INDEX IF NOT EXISTS idx_match_statistics_team
  ON raw.match_statistics (team_id);
