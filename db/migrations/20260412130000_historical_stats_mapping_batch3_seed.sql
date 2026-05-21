-- migrate:up
-- Final expansion batch for historical stats mappings.
-- Selection criteria: competitions with manually reviewed Wikipedia tables that
-- can be parsed opportunistically by the current champions/scorers/records pipeline.

INSERT INTO control.competition_wiki_mapping (
  competition_key,
  competition_id,
  competition_name,
  wiki_main_url,
  wiki_scorers_url,
  wiki_records_url,
  is_active,
  notes
)
VALUES
  (
    'brasileirao_b',
    651,
    'Campeonato Brasileiro Serie B',
    'https://en.wikipedia.org/wiki/Campeonato_Brasileiro_S%C3%A9rie_B',
    NULL,
    NULL,
    TRUE,
    'Batch 3 mapping. Champions table reviewed manually. Current scorer/record coverage is opportunistic and not forced.'
  ),
  (
    'sudamericana',
    1116,
    'CONMEBOL Sudamericana',
    'https://en.wikipedia.org/wiki/Copa_Sudamericana',
    NULL,
    NULL,
    TRUE,
    'Batch 3 mapping. Champions table reviewed manually. Dedicated scorer page is seasonal only, so current scorer coverage is intentionally skipped.'
  ),
  (
    'supercopa_do_brasil',
    1798,
    'Supercopa do Brasil',
    'https://en.wikipedia.org/wiki/Supercopa_do_Brasil',
    'https://en.wikipedia.org/wiki/Supercopa_do_Brasil',
    NULL,
    TRUE,
    'Batch 3 mapping. Main page contains direct champions and scorer tables reviewed manually before seed.'
  ),
  (
    'fifa_intercontinental_cup',
    1452,
    'FIFA Intercontinental Cup',
    'https://en.wikipedia.org/wiki/FIFA_Intercontinental_Cup',
    NULL,
    NULL,
    TRUE,
    'Batch 3 mapping. Finals table reviewed manually. Current scorer/record coverage is opportunistic and not forced.'
  ),
  (
    'primeira_liga',
    462,
    'Liga Portugal',
    'https://en.wikipedia.org/wiki/Primeira_Liga',
    'https://en.wikipedia.org/wiki/List_of_Primeira_Liga_top_scorers',
    'https://en.wikipedia.org/wiki/Primeira_Liga',
    TRUE,
    'Batch 3 mapping. Main page contains champions and appearances tables; dedicated scorer page reviewed manually before seed.'
  ),
  (
    'serie_a_it',
    384,
    'Serie A',
    'https://en.wikipedia.org/wiki/List_of_Italian_football_champions',
    'https://en.wikipedia.org/wiki/List_of_Serie_A_players_with_100_or_more_goals',
    'https://en.wikipedia.org/wiki/Serie_A_records_and_statistics',
    TRUE,
    'Batch 3 mapping. Champions, scorer and records tables reviewed manually before seed. Frontend alias serie_a_italy must resolve to this key.'
  )
ON CONFLICT (competition_key) DO UPDATE
SET
  competition_id = EXCLUDED.competition_id,
  competition_name = EXCLUDED.competition_name,
  wiki_main_url = EXCLUDED.wiki_main_url,
  wiki_scorers_url = EXCLUDED.wiki_scorers_url,
  wiki_records_url = EXCLUDED.wiki_records_url,
  is_active = EXCLUDED.is_active,
  notes = EXCLUDED.notes,
  updated_at = now()
WHERE
  control.competition_wiki_mapping.competition_id IS DISTINCT FROM EXCLUDED.competition_id
  OR control.competition_wiki_mapping.competition_name IS DISTINCT FROM EXCLUDED.competition_name
  OR control.competition_wiki_mapping.wiki_main_url IS DISTINCT FROM EXCLUDED.wiki_main_url
  OR control.competition_wiki_mapping.wiki_scorers_url IS DISTINCT FROM EXCLUDED.wiki_scorers_url
  OR control.competition_wiki_mapping.wiki_records_url IS DISTINCT FROM EXCLUDED.wiki_records_url
  OR control.competition_wiki_mapping.is_active IS DISTINCT FROM EXCLUDED.is_active
  OR control.competition_wiki_mapping.notes IS DISTINCT FROM EXCLUDED.notes;

-- migrate:down
DELETE FROM control.competition_wiki_mapping
WHERE competition_key IN (
  'brasileirao_b',
  'sudamericana',
  'supercopa_do_brasil',
  'fifa_intercontinental_cup',
  'primeira_liga',
  'serie_a_it'
);
