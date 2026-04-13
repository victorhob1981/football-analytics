-- migrate:up
-- Fix Bundesliga champions source to use the Bundesliga page, avoiding pre-Bundesliga German titles.

UPDATE control.competition_wiki_mapping
SET
  wiki_main_url = 'https://en.wikipedia.org/wiki/Bundesliga',
  notes = 'Batch 2 mapping. Bundesliga main page and scorer/records tables reviewed manually before seed.',
  updated_at = now()
WHERE competition_key = 'bundesliga';

-- migrate:down
UPDATE control.competition_wiki_mapping
SET
  wiki_main_url = 'https://en.wikipedia.org/wiki/List_of_German_football_champions',
  notes = 'Batch 2 mapping. Champions, scorer and records tables reviewed manually before seed.',
  updated_at = now()
WHERE competition_key = 'bundesliga';
