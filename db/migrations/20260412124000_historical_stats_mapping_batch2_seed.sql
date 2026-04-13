-- migrate:up
-- Expansion batch 2 for historical stats mapping.
-- Selection criteria: competitions with manually reviewed Wikipedia pages containing
-- structured champions tables and direct scorer/records pages when available.

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
    'brasileirao_a',
    71,
    'Campeonato Brasileiro Serie A',
    'https://en.wikipedia.org/wiki/List_of_Brazilian_football_champions',
    'https://en.wikipedia.org/wiki/List_of_Campeonato_Brasileiro_S%C3%A9rie_A_top_scorers',
    NULL,
    TRUE,
    'Batch 2 mapping. Champions and scorer tables reviewed manually before seed.'
  ),
  (
    'champions_league',
    2,
    'UEFA Champions League',
    'https://en.wikipedia.org/wiki/List_of_European_Cup_and_UEFA_Champions_League_finals',
    'https://en.wikipedia.org/wiki/List_of_UEFA_Champions_League_top_scorers',
    'https://en.wikipedia.org/wiki/European_Cup_and_UEFA_Champions_League_records_and_statistics',
    TRUE,
    'Batch 2 mapping. Finals, scorer and records tables reviewed manually before seed.'
  ),
  (
    'la_liga',
    564,
    'La Liga',
    'https://en.wikipedia.org/wiki/List_of_Spanish_football_champions',
    'https://en.wikipedia.org/wiki/List_of_La_Liga_top_scorers',
    'https://en.wikipedia.org/wiki/La_Liga_records_and_statistics',
    TRUE,
    'Batch 2 mapping. Champions, scorer and records tables reviewed manually before seed.'
  ),
  (
    'ligue_1',
    301,
    'Ligue 1',
    'https://en.wikipedia.org/wiki/List_of_French_football_champions',
    'https://en.wikipedia.org/wiki/List_of_Ligue_1_top_scorers',
    'https://en.wikipedia.org/wiki/List_of_Ligue_1_records_and_statistics',
    TRUE,
    'Batch 2 mapping. Champions, scorer and records tables reviewed manually before seed.'
  ),
  (
    'bundesliga',
    82,
    'Bundesliga',
    'https://en.wikipedia.org/wiki/List_of_German_football_champions',
    'https://en.wikipedia.org/wiki/List_of_Bundesliga_top_scorers',
    'https://en.wikipedia.org/wiki/Bundesliga_records_and_statistics',
    TRUE,
    'Batch 2 mapping. Champions, scorer and records tables reviewed manually before seed.'
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
  'brasileirao_a',
  'champions_league',
  'la_liga',
  'ligue_1',
  'bundesliga'
);
