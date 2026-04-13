-- migrate:up
-- Pilot Wikipedia mapping for structured champion extraction.

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
    'premier_league',
    8,
    'Premier League',
    'https://en.wikipedia.org/wiki/List_of_Premier_League_seasons',
    'https://en.wikipedia.org/wiki/List_of_Premier_League_top_scorers',
    NULL,
    TRUE,
    'Pilot league mapping. Champion table validated by pandas.read_html before seed.'
  ),
  (
    'libertadores',
    390,
    'CONMEBOL Libertadores',
    'https://en.wikipedia.org/wiki/Copa_Libertadores',
    NULL,
    'https://en.wikipedia.org/wiki/Copa_Libertadores_records_and_statistics',
    TRUE,
    'Pilot continental cup mapping. Winner table validated by pandas.read_html before seed.'
  ),
  (
    'copa_do_brasil',
    732,
    'Copa do Brasil',
    'https://en.wikipedia.org/wiki/Copa_do_Brasil',
    NULL,
    NULL,
    TRUE,
    'Pilot domestic cup mapping. Winner table validated by pandas.read_html before seed.'
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
WHERE competition_key IN ('premier_league', 'libertadores', 'copa_do_brasil');
