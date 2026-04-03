-- migrate:up
WITH competition_seed AS (
  SELECT
    'supercopa_do_brasil'::text AS competition_key,
    'Supercopa do Brasil'::text AS competition_name,
    'cup'::text AS competition_type,
    'Brazil'::text AS country_name,
    'CONMEBOL'::text AS confederation_name,
    NULL::smallint AS tier,
    TRUE AS is_active,
    95::integer AS display_priority
),
competition_upsert AS (
  INSERT INTO control.competitions (
    competition_key,
    competition_name,
    competition_type,
    country_name,
    confederation_name,
    tier,
    is_active,
    display_priority
  )
  SELECT
    competition_key,
    competition_name,
    competition_type,
    country_name,
    confederation_name,
    tier,
    is_active,
    display_priority
  FROM competition_seed
  ON CONFLICT (competition_key) DO UPDATE
  SET
    competition_name = EXCLUDED.competition_name,
    competition_type = EXCLUDED.competition_type,
    country_name = EXCLUDED.country_name,
    confederation_name = EXCLUDED.confederation_name,
    tier = EXCLUDED.tier,
    is_active = EXCLUDED.is_active,
    display_priority = EXCLUDED.display_priority,
    updated_at = now()
  WHERE
    control.competitions.competition_name IS DISTINCT FROM EXCLUDED.competition_name
    OR control.competitions.competition_type IS DISTINCT FROM EXCLUDED.competition_type
    OR control.competitions.country_name IS DISTINCT FROM EXCLUDED.country_name
    OR control.competitions.confederation_name IS DISTINCT FROM EXCLUDED.confederation_name
    OR control.competitions.tier IS DISTINCT FROM EXCLUDED.tier
    OR control.competitions.is_active IS DISTINCT FROM EXCLUDED.is_active
    OR control.competitions.display_priority IS DISTINCT FROM EXCLUDED.display_priority
  RETURNING 1
),
provider_seed AS (
  SELECT
    'supercopa_do_brasil'::text AS competition_key,
    'sportmonks'::text AS provider,
    1798::bigint AS provider_league_id,
    'Supercopa do Brasil'::text AS provider_name,
    TRUE AS is_active
),
provider_upsert AS (
  INSERT INTO control.competition_provider_map (
    competition_key,
    provider,
    provider_league_id,
    provider_name,
    is_active
  )
  SELECT
    competition_key,
    provider,
    provider_league_id,
    provider_name,
    is_active
  FROM provider_seed
  ON CONFLICT ON CONSTRAINT uq_control_competition_provider_map_provider_league DO UPDATE
  SET
    competition_key = EXCLUDED.competition_key,
    provider_name = EXCLUDED.provider_name,
    is_active = EXCLUDED.is_active,
    updated_at = now()
  WHERE
    control.competition_provider_map.competition_key IS DISTINCT FROM EXCLUDED.competition_key
    OR control.competition_provider_map.provider_name IS DISTINCT FROM EXCLUDED.provider_name
    OR control.competition_provider_map.is_active IS DISTINCT FROM EXCLUDED.is_active
  RETURNING 1
)
INSERT INTO control.season_catalog (
  competition_key,
  season_label,
  season_start_date,
  season_end_date,
  is_closed,
  provider,
  provider_season_id
)
SELECT
  seeded.competition_key,
  seeded.season_label,
  seeded.season_start_date,
  seeded.season_end_date,
  seeded.is_closed,
  seeded.provider,
  seeded.provider_season_id
FROM (
  VALUES
    ('supercopa_do_brasil', '2025', DATE '2025-02-02', DATE '2025-02-02', TRUE, 'sportmonks', 24772::bigint),
    ('supercopa_do_brasil', '2026', DATE '2026-02-01', DATE '2026-02-01', FALSE, 'sportmonks', 26826::bigint),
    ('supercopa_do_brasil', '2027', DATE '2027-02-01', DATE '2027-02-01', FALSE, 'sportmonks', 27800::bigint)
) AS seeded (
  competition_key,
  season_label,
  season_start_date,
  season_end_date,
  is_closed,
  provider,
  provider_season_id
)
ON CONFLICT (competition_key, season_label, provider) DO UPDATE
SET
  season_start_date = EXCLUDED.season_start_date,
  season_end_date = EXCLUDED.season_end_date,
  is_closed = EXCLUDED.is_closed,
  provider_season_id = EXCLUDED.provider_season_id,
  updated_at = now()
WHERE
  control.season_catalog.season_start_date IS DISTINCT FROM EXCLUDED.season_start_date
  OR control.season_catalog.season_end_date IS DISTINCT FROM EXCLUDED.season_end_date
  OR control.season_catalog.is_closed IS DISTINCT FROM EXCLUDED.is_closed
  OR control.season_catalog.provider_season_id IS DISTINCT FROM EXCLUDED.provider_season_id;

-- migrate:down
DELETE FROM control.season_catalog
WHERE competition_key = 'supercopa_do_brasil'
  AND provider = 'sportmonks';

DELETE FROM control.competition_provider_map
WHERE competition_key = 'supercopa_do_brasil'
  AND provider = 'sportmonks';

DELETE FROM control.competitions
WHERE competition_key = 'supercopa_do_brasil';
