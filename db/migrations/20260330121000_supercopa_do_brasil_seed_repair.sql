-- migrate:up
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
VALUES (
  'supercopa_do_brasil',
  'Supercopa do Brasil',
  'cup',
  'Brazil',
  'CONMEBOL',
  NULL,
  TRUE,
  95
)
ON CONFLICT (competition_key) DO UPDATE
SET
  competition_name = EXCLUDED.competition_name,
  competition_type = EXCLUDED.competition_type,
  country_name = EXCLUDED.country_name,
  confederation_name = EXCLUDED.confederation_name,
  tier = EXCLUDED.tier,
  is_active = EXCLUDED.is_active,
  display_priority = EXCLUDED.display_priority,
  updated_at = now();

INSERT INTO control.competition_provider_map (
  competition_key,
  provider,
  provider_league_id,
  provider_name,
  is_active
)
VALUES (
  'supercopa_do_brasil',
  'sportmonks',
  1798,
  'Supercopa do Brasil',
  TRUE
)
ON CONFLICT ON CONSTRAINT uq_control_competition_provider_map_provider_league DO UPDATE
SET
  competition_key = EXCLUDED.competition_key,
  provider_name = EXCLUDED.provider_name,
  is_active = EXCLUDED.is_active,
  updated_at = now();

INSERT INTO control.season_catalog (
  competition_key,
  season_label,
  season_start_date,
  season_end_date,
  is_closed,
  provider,
  provider_season_id
)
VALUES
  ('supercopa_do_brasil', '2025', DATE '2025-02-02', DATE '2025-02-02', TRUE, 'sportmonks', 24772),
  ('supercopa_do_brasil', '2026', DATE '2026-02-01', DATE '2026-02-01', FALSE, 'sportmonks', 26826),
  ('supercopa_do_brasil', '2027', DATE '2027-02-01', DATE '2027-02-01', FALSE, 'sportmonks', 27800)
ON CONFLICT (competition_key, season_label, provider) DO UPDATE
SET
  season_start_date = EXCLUDED.season_start_date,
  season_end_date = EXCLUDED.season_end_date,
  is_closed = EXCLUDED.is_closed,
  provider_season_id = EXCLUDED.provider_season_id,
  updated_at = now();

-- migrate:down
SELECT 1;
