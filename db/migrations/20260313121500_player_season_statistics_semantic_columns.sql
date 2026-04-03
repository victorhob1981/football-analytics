-- migrate:up
ALTER TABLE IF EXISTS raw.player_season_statistics
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

-- migrate:down
-- no-op: semantic columns are intentionally additive and retained on rollback.
