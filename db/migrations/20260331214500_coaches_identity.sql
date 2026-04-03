-- migrate:up
-- Materializa identidade canonica de tecnicos em tabela dedicada.

CREATE TABLE IF NOT EXISTS raw.coaches (
  provider         TEXT NOT NULL,
  coach_id         BIGINT NOT NULL,
  coach_name       TEXT,
  image_path       TEXT,
  payload          JSONB,
  ingested_run     TEXT,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_coaches PRIMARY KEY (provider, coach_id)
);

CREATE INDEX IF NOT EXISTS idx_coaches_name
  ON raw.coaches (coach_name);

-- migrate:down
SELECT 1;
