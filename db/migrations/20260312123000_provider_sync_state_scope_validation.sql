-- migrate:up
ALTER TABLE raw.provider_sync_state
  ADD COLUMN IF NOT EXISTS scope_validated BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS scope_validation_notes TEXT;

UPDATE raw.provider_sync_state
SET scope_validated = FALSE
WHERE scope_validated IS NULL;

-- migrate:down
SELECT 1;
