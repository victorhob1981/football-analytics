-- migrate:up
-- Enable Sudamericana seasonal scorer and records pages for deeper historical coverage.

UPDATE control.competition_wiki_mapping
SET
  wiki_scorers_url = 'https://en.wikipedia.org/wiki/List_of_Copa_Sudamericana_top_scorers',
  wiki_records_url = 'https://en.wikipedia.org/wiki/Copa_Sudamericana_records_and_statistics',
  notes = 'Batch 3 mapping expanded. Champions, seasonal scorer and records tables reviewed manually before seed.',
  updated_at = now()
WHERE competition_key = 'sudamericana';

-- migrate:down
UPDATE control.competition_wiki_mapping
SET
  wiki_scorers_url = NULL,
  wiki_records_url = NULL,
  notes = 'Batch 3 mapping. Champions table reviewed manually. Dedicated scorer page is seasonal only, so current scorer coverage is intentionally skipped.',
  updated_at = now()
WHERE competition_key = 'sudamericana';
