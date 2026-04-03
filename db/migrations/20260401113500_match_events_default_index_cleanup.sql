-- migrate:up
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '15min';

LOCK TABLE raw.match_events_default IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
  v_default_total bigint;
BEGIN
  SELECT count(*)
  INTO v_default_total
  FROM raw.match_events_default;

  IF v_default_total <> 0 THEN
    RAISE EXCEPTION 'raw.match_events_default still contains % rows; duplicate index cleanup is not safe', v_default_total;
  END IF;
END
$$;

DROP INDEX IF EXISTS raw.idx_raw_match_events_default_fixture_id;
DROP INDEX IF EXISTS raw.idx_raw_match_events_default_player_id;
DROP INDEX IF EXISTS raw.idx_raw_match_events_default_team_id;

DO $$
DECLARE
  v_index_count integer;
  v_duplicate_count integer;
BEGIN
  SELECT count(*)
  INTO v_index_count
  FROM pg_indexes
  WHERE schemaname = 'raw'
    AND tablename = 'match_events_default';

  SELECT count(*)
  INTO v_duplicate_count
  FROM pg_indexes
  WHERE schemaname = 'raw'
    AND tablename = 'match_events_default'
    AND indexname IN (
      'idx_raw_match_events_default_fixture_id',
      'idx_raw_match_events_default_player_id',
      'idx_raw_match_events_default_team_id'
    );

  IF v_index_count <> 7 THEN
    RAISE EXCEPTION 'raw.match_events_default: expected 7 indexes after cleanup, found %', v_index_count;
  END IF;

  IF v_duplicate_count <> 0 THEN
    RAISE EXCEPTION 'raw.match_events_default: duplicate local indexes still present (% found)', v_duplicate_count;
  END IF;

  RAISE NOTICE 'raw.match_events_default cleanup complete: % indexes remain', v_index_count;
END
$$;

-- migrate:down
SELECT 1;
