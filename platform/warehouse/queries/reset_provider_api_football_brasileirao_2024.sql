-- Limpeza de migracao de provider (api_football -> sportmonks)
-- Escopo: Brasileirao 2024, removendo dados antigos da liga 71 (API-Football)
-- para evitar duplicidade analitica ao reprocessar com SportMonks (liga 648).

BEGIN;

WITH old_fixtures AS (
    SELECT fixture_id
    FROM raw.fixtures
    WHERE league_id = 71
      AND season = 2024
)
DELETE FROM raw.match_events e
USING old_fixtures f
WHERE e.fixture_id = f.fixture_id;

WITH old_fixtures AS (
    SELECT fixture_id
    FROM raw.fixtures
    WHERE league_id = 71
      AND season = 2024
)
DELETE FROM raw.match_statistics s
USING old_fixtures f
WHERE s.fixture_id = f.fixture_id;

DELETE FROM raw.fixtures
WHERE league_id = 71
  AND season = 2024;

-- Reseta cursores de sync para forcar nova trilha limpa.
DELETE FROM raw.provider_sync_state
WHERE season = 2024
  AND (
      provider = 'api_football'
      OR (provider = 'sportmonks' AND league_id IN (71, 648))
  );

COMMIT;
