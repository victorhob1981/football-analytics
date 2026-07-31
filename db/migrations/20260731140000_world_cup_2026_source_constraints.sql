-- migrate:up
ALTER TABLE raw.wc_goals
  DROP CONSTRAINT IF EXISTS chk_wc_goals_source_name_raw;

ALTER TABLE raw.wc_goals
  ADD CONSTRAINT chk_wc_goals_source_name_raw
  CHECK (source_name IN ('fjelstul_worldcup', 'mominullptr_wc2026'));

ALTER TABLE raw.wc_squads
  DROP CONSTRAINT IF EXISTS chk_wc_squads_source_name_raw;

ALTER TABLE raw.wc_squads
  ADD CONSTRAINT chk_wc_squads_source_name_raw
  CHECK (source_name IN ('fjelstul_worldcup', 'mominullptr_wc2026'));

ALTER TABLE raw.wc_match_events
  DROP CONSTRAINT IF EXISTS chk_wc_match_events_source_name;

ALTER TABLE raw.wc_match_events
  ADD CONSTRAINT chk_wc_match_events_source_name
  CHECK (
    source_name IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more',
      'mominullptr_wc2026'
    )
  );

ALTER TABLE raw.wc_bookings
  DROP CONSTRAINT IF EXISTS chk_wc_bookings_source_name_raw;

ALTER TABLE raw.wc_bookings
  ADD CONSTRAINT chk_wc_bookings_source_name_raw
  CHECK (source_name IN ('fjelstul_worldcup', 'mominullptr_wc2026'));

ALTER TABLE raw.wc_substitutions
  DROP CONSTRAINT IF EXISTS chk_wc_substitutions_source_name_raw;

ALTER TABLE raw.wc_substitutions
  ADD CONSTRAINT chk_wc_substitutions_source_name_raw
  CHECK (source_name IN ('fjelstul_worldcup', 'mominullptr_wc2026'));

ALTER TABLE control.wc_source_snapshots
  DROP CONSTRAINT IF EXISTS chk_wc_source_snapshots_source_name;

ALTER TABLE control.wc_source_snapshots
  ADD CONSTRAINT chk_wc_source_snapshots_source_name
  CHECK (
    source_name IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more',
      'mominullptr_wc2026'
    )
  );

ALTER TABLE control.wc_entity_match_review_queue
  DROP CONSTRAINT IF EXISTS chk_wc_entity_match_review_source_name;

ALTER TABLE control.wc_entity_match_review_queue
  ADD CONSTRAINT chk_wc_entity_match_review_source_name
  CHECK (
    source_name IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more',
      'mominullptr_wc2026'
    )
  );

-- migrate:down
ALTER TABLE raw.wc_goals
  DROP CONSTRAINT IF EXISTS chk_wc_goals_source_name_raw;

ALTER TABLE raw.wc_goals
  ADD CONSTRAINT chk_wc_goals_source_name_raw
  CHECK (source_name = 'fjelstul_worldcup');

ALTER TABLE raw.wc_squads
  DROP CONSTRAINT IF EXISTS chk_wc_squads_source_name_raw;

ALTER TABLE raw.wc_squads
  ADD CONSTRAINT chk_wc_squads_source_name_raw
  CHECK (source_name = 'fjelstul_worldcup');

ALTER TABLE raw.wc_match_events
  DROP CONSTRAINT IF EXISTS chk_wc_match_events_source_name;

ALTER TABLE raw.wc_match_events
  ADD CONSTRAINT chk_wc_match_events_source_name
  CHECK (
    source_name IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more'
    )
  );

ALTER TABLE raw.wc_bookings
  DROP CONSTRAINT IF EXISTS chk_wc_bookings_source_name_raw;

ALTER TABLE raw.wc_bookings
  ADD CONSTRAINT chk_wc_bookings_source_name_raw
  CHECK (source_name = 'fjelstul_worldcup');

ALTER TABLE raw.wc_substitutions
  DROP CONSTRAINT IF EXISTS chk_wc_substitutions_source_name_raw;

ALTER TABLE raw.wc_substitutions
  ADD CONSTRAINT chk_wc_substitutions_source_name_raw
  CHECK (source_name = 'fjelstul_worldcup');

ALTER TABLE control.wc_source_snapshots
  DROP CONSTRAINT IF EXISTS chk_wc_source_snapshots_source_name;

ALTER TABLE control.wc_source_snapshots
  ADD CONSTRAINT chk_wc_source_snapshots_source_name
  CHECK (
    source_name IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more'
    )
  );

ALTER TABLE control.wc_entity_match_review_queue
  DROP CONSTRAINT IF EXISTS chk_wc_entity_match_review_source_name;

ALTER TABLE control.wc_entity_match_review_queue
  ADD CONSTRAINT chk_wc_entity_match_review_source_name
  CHECK (
    source_name IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more'
    )
  );
