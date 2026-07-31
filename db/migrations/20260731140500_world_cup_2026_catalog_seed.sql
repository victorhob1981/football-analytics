-- migrate:up
INSERT INTO control.competition_provider_map (
  competition_key,
  provider,
  provider_league_id,
  provider_name,
  is_active
) VALUES (
  'fifa_world_cup_mens',
  'mominullptr_wc2026',
  344223137057272147,
  'FIFA World Cup 2026 Dataset by mominullptr',
  TRUE
)
ON CONFLICT (competition_key, provider) DO UPDATE
SET
  provider_league_id = EXCLUDED.provider_league_id,
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
VALUES (
  'fifa_world_cup_mens',
  '2026',
  DATE '2026-06-11',
  DATE '2026-07-19',
  TRUE,
  'mominullptr_wc2026',
  9099783570221629243
)
ON CONFLICT (competition_key, season_label, provider) DO UPDATE
SET
  season_start_date = EXCLUDED.season_start_date,
  season_end_date = EXCLUDED.season_end_date,
  is_closed = EXCLUDED.is_closed,
  provider_season_id = EXCLUDED.provider_season_id,
  updated_at = now();

-- migrate:down
DELETE FROM control.season_catalog
WHERE competition_key = 'fifa_world_cup_mens'
  AND season_label = '2026'
  AND provider = 'mominullptr_wc2026';

DELETE FROM control.competition_provider_map
WHERE competition_key = 'fifa_world_cup_mens'
  AND provider = 'mominullptr_wc2026';
