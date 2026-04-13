-- migrate:up
-- Pilot records URL for structured team/match record extraction.

UPDATE control.competition_wiki_mapping
SET
  wiki_records_url = 'https://en.wikipedia.org/wiki/Premier_League_records_and_statistics',
  notes = 'Pilot league mapping. Champion and records tables validated by pandas.read_html before seed.',
  updated_at = now()
WHERE competition_key = 'premier_league';

-- migrate:down
UPDATE control.competition_wiki_mapping
SET
  wiki_records_url = NULL,
  notes = 'Pilot league mapping. Champion table validated by pandas.read_html before seed.',
  updated_at = now()
WHERE competition_key = 'premier_league';
