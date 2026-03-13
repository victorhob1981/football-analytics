-- migrate:up
UPDATE raw.match_events me
SET provider = f.provider
FROM raw.fixtures f
WHERE me.fixture_id = f.fixture_id
  AND me.provider IS NULL;

ALTER TABLE raw.match_events
  ALTER COLUMN provider SET NOT NULL;

ALTER TABLE raw.match_events
  DROP CONSTRAINT IF EXISTS pk_raw_match_events;

ALTER TABLE raw.match_events
  DROP CONSTRAINT IF EXISTS pk_match_events;

ALTER TABLE raw.match_events
  ADD CONSTRAINT pk_match_events PRIMARY KEY (provider, season, fixture_id, event_id);

-- migrate:down
ALTER TABLE raw.match_events
  DROP CONSTRAINT IF EXISTS pk_match_events;

ALTER TABLE raw.match_events
  DROP CONSTRAINT IF EXISTS pk_raw_match_events;

ALTER TABLE raw.match_events
  ADD CONSTRAINT pk_match_events PRIMARY KEY (event_id, season);

ALTER TABLE raw.match_events
  ALTER COLUMN provider DROP NOT NULL;
