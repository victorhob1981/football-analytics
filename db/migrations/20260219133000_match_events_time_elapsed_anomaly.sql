-- migrate:up
ALTER TABLE raw.match_events
  ADD COLUMN IF NOT EXISTS is_time_elapsed_anomalous BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE raw.match_events
SET
  time_elapsed = NULL,
  is_time_elapsed_anomalous = TRUE
WHERE time_elapsed < 0;

-- migrate:down
ALTER TABLE raw.match_events
  DROP COLUMN IF EXISTS is_time_elapsed_anomalous;
