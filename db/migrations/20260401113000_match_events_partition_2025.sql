-- migrate:up
SET LOCAL lock_timeout = '15s';
SET LOCAL statement_timeout = '15min';

LOCK TABLE raw.match_events IN ACCESS EXCLUSIVE MODE;
LOCK TABLE raw.match_events_default IN ACCESS EXCLUSIVE MODE;

CREATE TEMP TABLE tmp_match_events_stage_2025 AS
SELECT *
FROM raw.match_events_default
WITH NO DATA;

INSERT INTO tmp_match_events_stage_2025
SELECT *
FROM raw.match_events_default
WHERE season = 2025;

CREATE TEMP TABLE tmp_match_events_meta_2025 AS
SELECT
  (SELECT count(*) FROM raw.match_events) AS total_before,
  (SELECT count(*) FROM raw.match_events_default WHERE season = 2025) AS default_before,
  (SELECT count(*) FROM tmp_match_events_stage_2025) AS stage_count,
  (SELECT count(DISTINCT (provider, season, fixture_id, event_id)) FROM tmp_match_events_stage_2025) AS stage_distinct_pk,
  (SELECT to_regclass('raw.match_events_2025') IS NOT NULL) AS partition_exists_before;

DO $$
DECLARE
  m record;
BEGIN
  SELECT *
  INTO m
  FROM tmp_match_events_meta_2025;

  IF m.default_before <> m.stage_count THEN
    RAISE EXCEPTION 'season 2025: default_before % differs from staged rows %', m.default_before, m.stage_count;
  END IF;

  IF m.stage_count <> m.stage_distinct_pk THEN
    RAISE EXCEPTION 'season 2025: staged rows % differ from staged distinct PK %', m.stage_count, m.stage_distinct_pk;
  END IF;

  IF m.partition_exists_before AND m.default_before > 0 THEN
    RAISE EXCEPTION 'season 2025: partition already exists while default still contains % rows', m.default_before;
  END IF;
END
$$;

DELETE FROM raw.match_events_default
WHERE season = 2025;

DO $$
DECLARE
  v_default_after bigint;
BEGIN
  SELECT count(*)
  INTO v_default_after
  FROM raw.match_events_default
  WHERE season = 2025;

  IF v_default_after <> 0 THEN
    RAISE EXCEPTION 'season 2025: default still contains % rows after delete', v_default_after;
  END IF;
END
$$;

DO $$
BEGIN
  IF to_regclass('raw.match_events_2025') IS NULL THEN
    EXECUTE 'CREATE TABLE raw.match_events_2025 PARTITION OF raw.match_events FOR VALUES IN (2025)';
  END IF;
END
$$;

INSERT INTO raw.match_events
SELECT *
FROM tmp_match_events_stage_2025;

DO $$
DECLARE
  m record;
  v_partition_after bigint;
  v_total_after bigint;
  v_distinct_pk bigint;
  v_orphans bigint;
  v_index_count integer;
  v_partition_attached boolean;
BEGIN
  SELECT *
  INTO m
  FROM tmp_match_events_meta_2025;

  SELECT count(*)
  INTO v_partition_after
  FROM raw.match_events_2025;

  SELECT count(*)
  INTO v_total_after
  FROM raw.match_events;

  SELECT count(DISTINCT (provider, season, fixture_id, event_id))
  INTO v_distinct_pk
  FROM raw.match_events;

  SELECT count(*)
  INTO v_orphans
  FROM raw.match_events_2025 me
  LEFT JOIN raw.fixtures f
    ON f.fixture_id = me.fixture_id
  WHERE f.fixture_id IS NULL;

  SELECT count(*)
  INTO v_index_count
  FROM pg_indexes
  WHERE schemaname = 'raw'
    AND tablename = 'match_events_2025';

  SELECT EXISTS (
    SELECT 1
    FROM pg_inherits i
    JOIN pg_class c
      ON c.oid = i.inhrelid
    JOIN pg_class p
      ON p.oid = i.inhparent
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    JOIN pg_namespace pn
      ON pn.oid = p.relnamespace
    WHERE pn.nspname = 'raw'
      AND p.relname = 'match_events'
      AND n.nspname = 'raw'
      AND c.relname = 'match_events_2025'
      AND pg_get_expr(c.relpartbound, c.oid) = 'FOR VALUES IN (2025)'
  )
  INTO v_partition_attached;

  IF v_partition_after <> m.stage_count THEN
    RAISE EXCEPTION 'season 2025: partition row count % differs from staged rows %', v_partition_after, m.stage_count;
  END IF;

  IF v_total_after <> m.total_before THEN
    RAISE EXCEPTION 'season 2025: total row count changed from % to %', m.total_before, v_total_after;
  END IF;

  IF v_total_after <> v_distinct_pk THEN
    RAISE EXCEPTION 'season 2025: PK uniqueness violated, total % distinct %', v_total_after, v_distinct_pk;
  END IF;

  IF v_orphans <> 0 THEN
    RAISE EXCEPTION 'season 2025: fixture FK orphan count is %', v_orphans;
  END IF;

  IF v_index_count <> 7 THEN
    RAISE EXCEPTION 'season 2025: expected 7 indexes on partition, found %', v_index_count;
  END IF;

  IF NOT v_partition_attached THEN
    RAISE EXCEPTION 'season 2025: partition attachment validation failed';
  END IF;

  RAISE NOTICE 'season 2025 migrated: staged %, partition %, total %',
    m.stage_count,
    v_partition_after,
    v_total_after;
END
$$;

-- migrate:down
SELECT 1;
