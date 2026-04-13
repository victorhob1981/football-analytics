-- migrate:up
-- Official vocabulary for historical competition statistics.

INSERT INTO control.historical_stat_definitions (
  stat_code,
  stat_group,
  display_name,
  description,
  entity_type,
  value_unit,
  is_ranking,
  is_active
)
VALUES
  (
    'all_time_champions',
    'champions',
    'Maiores campeoes',
    'All-time competition title ranking.',
    'team',
    'titles',
    TRUE,
    TRUE
  ),
  (
    'all_time_top_scorers',
    'scorers',
    'Maiores artilheiros',
    'All-time competition top scorers ranking when a direct structured table is available.',
    'player',
    'goals',
    TRUE,
    TRUE
  ),
  (
    'most_points_single_season',
    'team_records',
    'Mais pontos em uma temporada',
    'Team record for most points in a single competition season.',
    'team',
    'points',
    FALSE,
    TRUE
  ),
  (
    'most_goals_single_season_team',
    'team_records',
    'Melhor ataque em uma temporada',
    'Team record for most goals scored in a single competition season.',
    'team',
    'goals',
    FALSE,
    TRUE
  ),
  (
    'fewest_goals_conceded_single_season',
    'team_records',
    'Melhor defesa em uma temporada',
    'Team record for fewest goals conceded in a single competition season.',
    'team',
    'goals_conceded',
    FALSE,
    TRUE
  ),
  (
    'longest_unbeaten_run',
    'team_records',
    'Maior sequencia invicta',
    'Team record for longest unbeaten run in the competition.',
    'team',
    'matches',
    FALSE,
    TRUE
  ),
  (
    'longest_winning_run',
    'team_records',
    'Maior sequencia de vitorias',
    'Team record for longest winning run in the competition.',
    'team',
    'matches',
    FALSE,
    TRUE
  ),
  (
    'biggest_win',
    'match_records',
    'Maior goleada',
    'Match record for biggest win in competition history.',
    'match',
    'goal_difference',
    FALSE,
    TRUE
  ),
  (
    'highest_scoring_match',
    'match_records',
    'Jogo com mais gols',
    'Match record for highest total goals in competition history.',
    'match',
    'goals',
    FALSE,
    TRUE
  ),
  (
    'player_most_goals_single_season',
    'player_records',
    'Mais gols em uma temporada',
    'Player record for most goals in a single competition season.',
    'player',
    'goals',
    FALSE,
    TRUE
  ),
  (
    'player_most_goals_single_match',
    'player_records',
    'Mais gols em uma partida',
    'Player record for most goals in one competition match.',
    'player',
    'goals',
    FALSE,
    TRUE
  ),
  (
    'player_most_titles',
    'player_records',
    'Jogador com mais titulos',
    'Player record for most competition titles.',
    'player',
    'titles',
    FALSE,
    TRUE
  ),
  (
    'player_most_appearances',
    'player_records',
    'Jogador com mais jogos',
    'Player record for most competition appearances.',
    'player',
    'appearances',
    FALSE,
    TRUE
  )
ON CONFLICT (stat_code) DO UPDATE
SET
  stat_group = EXCLUDED.stat_group,
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  entity_type = EXCLUDED.entity_type,
  value_unit = EXCLUDED.value_unit,
  is_ranking = EXCLUDED.is_ranking,
  is_active = EXCLUDED.is_active,
  updated_at = now()
WHERE
  control.historical_stat_definitions.stat_group IS DISTINCT FROM EXCLUDED.stat_group
  OR control.historical_stat_definitions.display_name IS DISTINCT FROM EXCLUDED.display_name
  OR control.historical_stat_definitions.description IS DISTINCT FROM EXCLUDED.description
  OR control.historical_stat_definitions.entity_type IS DISTINCT FROM EXCLUDED.entity_type
  OR control.historical_stat_definitions.value_unit IS DISTINCT FROM EXCLUDED.value_unit
  OR control.historical_stat_definitions.is_ranking IS DISTINCT FROM EXCLUDED.is_ranking
  OR control.historical_stat_definitions.is_active IS DISTINCT FROM EXCLUDED.is_active;

-- migrate:down
DELETE FROM control.historical_stat_definitions
WHERE stat_code IN (
  'all_time_champions',
  'all_time_top_scorers',
  'most_points_single_season',
  'most_goals_single_season_team',
  'fewest_goals_conceded_single_season',
  'longest_unbeaten_run',
  'longest_winning_run',
  'biggest_win',
  'highest_scoring_match',
  'player_most_goals_single_season',
  'player_most_goals_single_match',
  'player_most_titles',
  'player_most_appearances'
);
