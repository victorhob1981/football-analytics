-- migrate:up
-- Copa do Mundo | Bloco 4
-- Extensao aditiva minima de raw.provider_entity_map para suportar o contrato
-- do bootstrap canonico inicial sem quebrar a base legada.

ALTER TABLE raw.provider_entity_map
  ADD COLUMN IF NOT EXISTS edition_key TEXT,
  ADD COLUMN IF NOT EXISTS source_version TEXT,
  ADD COLUMN IF NOT EXISTS mapping_confidence TEXT,
  ADD COLUMN IF NOT EXISTS resolution_method TEXT,
  ADD COLUMN IF NOT EXISTS needs_manual_review BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS review_reason TEXT,
  ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS team_type TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_provider_entity_map_mapping_confidence'
      AND conrelid = 'raw.provider_entity_map'::regclass
  ) THEN
    ALTER TABLE raw.provider_entity_map
      ADD CONSTRAINT chk_provider_entity_map_mapping_confidence
      CHECK (
        mapping_confidence IS NULL
        OR mapping_confidence IN ('exact', 'high', 'medium', 'low')
      );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_provider_entity_map_team_type'
      AND conrelid = 'raw.provider_entity_map'::regclass
  ) THEN
    ALTER TABLE raw.provider_entity_map
      ADD CONSTRAINT chk_provider_entity_map_team_type
      CHECK (
        team_type IS NULL
        OR team_type = 'national_team'
      );
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_provider_entity_map_review_reason'
      AND conrelid = 'raw.provider_entity_map'::regclass
  ) THEN
    ALTER TABLE raw.provider_entity_map
      ADD CONSTRAINT chk_provider_entity_map_review_reason
      CHECK (
        needs_manual_review = FALSE
        OR review_reason IS NOT NULL
      );
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_provider_entity_map_entity_edition
  ON raw.provider_entity_map (entity_type, edition_key);

CREATE INDEX IF NOT EXISTS idx_provider_entity_map_provider_entity_active
  ON raw.provider_entity_map (provider, entity_type, is_active);

CREATE INDEX IF NOT EXISTS idx_provider_entity_map_source_version
  ON raw.provider_entity_map (source_version);

-- migrate:down
SELECT 1;
