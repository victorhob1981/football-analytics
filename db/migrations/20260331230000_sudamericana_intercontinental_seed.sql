-- migrate:up
WITH competition_seed AS (
  SELECT *
  FROM (
    VALUES
      ('sudamericana'::text, 'CONMEBOL Sudamericana'::text, 'continental_cup'::text, NULL::text, 'CONMEBOL'::text, NULL::smallint, TRUE, 75::integer),
      ('fifa_intercontinental_cup'::text, 'FIFA Intercontinental Cup'::text, 'cup'::text, NULL::text, 'FIFA'::text, NULL::smallint, TRUE, 65::integer)
  ) AS seeded (
    competition_key,
    competition_name,
    competition_type,
    country_name,
    confederation_name,
    tier,
    is_active,
    display_priority
  )
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
  SELECT *
  FROM (
    VALUES
      ('sudamericana'::text, 'sportmonks'::text, 1116::bigint, 'Copa Sudamericana'::text, TRUE),
      ('fifa_intercontinental_cup'::text, 'sportmonks'::text, 1452::bigint, 'FIFA Intercontinental Cup'::text, TRUE)
  ) AS seeded (
    competition_key,
    provider,
    provider_league_id,
    provider_name,
    is_active
  )
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
    ('sudamericana', '2024', DATE '2024-03-05', DATE '2024-11-23', TRUE, 'sportmonks', 22971::bigint),
    ('sudamericana', '2025', DATE '2025-03-05', DATE '2025-11-22', TRUE, 'sportmonks', 24955::bigint),
    ('fifa_intercontinental_cup', '2024', DATE '2024-09-22', DATE '2024-12-18', TRUE, 'sportmonks', 24583::bigint)
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
WHERE competition_key IN ('sudamericana', 'fifa_intercontinental_cup')
  AND provider = 'sportmonks';

DELETE FROM control.competition_provider_map
WHERE competition_key IN ('sudamericana', 'fifa_intercontinental_cup')
  AND provider = 'sportmonks';

DELETE FROM control.competitions
WHERE competition_key IN ('sudamericana', 'fifa_intercontinental_cup');
