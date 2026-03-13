-- migrate:up
INSERT INTO control.competition_provider_map (
  competition_key,
  provider,
  provider_league_id,
  provider_name,
  is_active
)
VALUES
  ('premier_league', 'sportmonks', 8, 'Premier League', TRUE),
  ('la_liga', 'sportmonks', 564, 'La Liga', TRUE),
  ('serie_a_it', 'sportmonks', 384, 'Serie A', TRUE),
  ('bundesliga', 'sportmonks', 82, 'Bundesliga', TRUE),
  ('ligue_1', 'sportmonks', 301, 'Ligue 1', TRUE),
  ('champions_league', 'sportmonks', 2, 'Champions League', TRUE),
  ('libertadores', 'sportmonks', 1122, 'Copa Libertadores', TRUE),
  ('brasileirao_a', 'sportmonks', 648, 'Serie A', TRUE),
  ('brasileirao_b', 'sportmonks', 651, 'Serie B', TRUE),
  ('copa_do_brasil', 'sportmonks', 654, 'Copa do Brasil', TRUE)
ON CONFLICT (competition_key, provider) DO UPDATE
SET
  provider_league_id = EXCLUDED.provider_league_id,
  provider_name = EXCLUDED.provider_name,
  is_active = EXCLUDED.is_active,
  updated_at = now();

-- migrate:down
DELETE FROM control.competition_provider_map
WHERE provider = 'sportmonks'
  AND competition_key IN (
    'premier_league',
    'la_liga',
    'serie_a_it',
    'bundesliga',
    'ligue_1',
    'champions_league',
    'libertadores',
    'brasileirao_a',
    'brasileirao_b',
    'copa_do_brasil'
  );
