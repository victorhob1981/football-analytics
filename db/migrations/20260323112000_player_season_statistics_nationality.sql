-- migrate:up
ALTER TABLE IF EXISTS raw.player_season_statistics
  ADD COLUMN IF NOT EXISTS player_nationality TEXT;

-- migrate:down
-- no-op: player_nationality is intentionally additive and retained on rollback.
