-- migrate:up
-- Evita falha de insert em temporadas novas sem particao dedicada.
DO $$
BEGIN
  IF to_regclass('raw.match_events_default') IS NULL THEN
    CREATE TABLE raw.match_events_default
      PARTITION OF raw.match_events DEFAULT;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_raw_match_events_default_fixture_id
  ON raw.match_events_default (fixture_id);

CREATE INDEX IF NOT EXISTS idx_raw_match_events_default_team_id
  ON raw.match_events_default (team_id);

CREATE INDEX IF NOT EXISTS idx_raw_match_events_default_player_id
  ON raw.match_events_default (player_id);

-- migrate:down
DROP TABLE IF EXISTS raw.match_events_default;
