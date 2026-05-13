-- Raw statistics -> fixtures FK rollout (safe/idempotent)
-- Strategy:
-- 1) Keep check query for orphans
-- 2) Ensure index exists on fixture_id
-- 3) Add FK as NOT VALID to avoid long lock/validation on large tables
-- 4) Validate later via warehouse/ddl/005_validate_raw_statistics_fixture_fk.sql

CREATE INDEX IF NOT EXISTS idx_match_statistics_fixture
  ON raw.match_statistics (fixture_id);

-- Orphan check (should be 0 before validation):
-- SELECT s.fixture_id
-- FROM raw.match_statistics s
-- LEFT JOIN raw.fixtures f ON f.fixture_id = s.fixture_id
-- WHERE f.fixture_id IS NULL;

DO $$
DECLARE
  orphan_count BIGINT;
BEGIN
  SELECT COUNT(*)
    INTO orphan_count
  FROM raw.match_statistics s
  LEFT JOIN raw.fixtures f
    ON f.fixture_id = s.fixture_id
  WHERE f.fixture_id IS NULL;

  IF orphan_count > 0 THEN
    RAISE WARNING
      'Encontrados % orfaos em raw.match_statistics. FK sera criada como NOT VALID. Rode limpeza e depois 005_validate_raw_statistics_fixture_fk.sql.',
      orphan_count;
  ELSE
    RAISE NOTICE 'Nenhum orfao encontrado em raw.match_statistics.';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_match_statistics_fixture'
      AND conrelid = 'raw.match_statistics'::regclass
  ) THEN
    ALTER TABLE raw.match_statistics
      ADD CONSTRAINT fk_match_statistics_fixture
      FOREIGN KEY (fixture_id)
      REFERENCES raw.fixtures (fixture_id)
      NOT VALID;
  ELSE
    RAISE NOTICE 'Constraint fk_match_statistics_fixture ja existe.';
  END IF;
END $$;
