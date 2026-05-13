-- Validation step for raw.match_statistics.fixture_id -> raw.fixtures.fixture_id
-- Run this after cleaning orphan rows.

DO $$
DECLARE
  orphan_count BIGINT;
  constraint_exists BOOLEAN;
  already_validated BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_match_statistics_fixture'
      AND conrelid = 'raw.match_statistics'::regclass
  )
  INTO constraint_exists;

  IF NOT constraint_exists THEN
    RAISE EXCEPTION
      'Constraint fk_match_statistics_fixture nao existe. Rode warehouse/ddl/004_raw_statistics_fixture_fk.sql primeiro.';
  END IF;

  SELECT COUNT(*)
    INTO orphan_count
  FROM raw.match_statistics s
  LEFT JOIN raw.fixtures f
    ON f.fixture_id = s.fixture_id
  WHERE f.fixture_id IS NULL;

  IF orphan_count > 0 THEN
    RAISE EXCEPTION
      'Validacao bloqueada: % orfaos em raw.match_statistics. Limpe/corrija antes de validar FK.',
      orphan_count;
  END IF;

  SELECT convalidated
    INTO already_validated
  FROM pg_constraint
  WHERE conname = 'fk_match_statistics_fixture'
    AND conrelid = 'raw.match_statistics'::regclass;

  IF already_validated THEN
    RAISE NOTICE 'Constraint fk_match_statistics_fixture ja estava validada.';
  ELSE
    ALTER TABLE raw.match_statistics
      VALIDATE CONSTRAINT fk_match_statistics_fixture;
    RAISE NOTICE 'Constraint fk_match_statistics_fixture validada com sucesso.';
  END IF;
END $$;
