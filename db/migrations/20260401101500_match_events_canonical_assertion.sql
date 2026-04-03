-- migrate:up
-- Canonical assertion for raw.match_events after repository officialization.
-- This migration is intentionally non-destructive: it validates the runtime
-- contract that the repository now declares, without repartitioning or moving rows.

DO $$
DECLARE
  expected record;
  actual_type text;
  actual_not_null boolean;
  actual_column_count integer;
  actual_pk_name text;
  actual_pk_columns text;
BEGIN
  IF to_regclass('raw.match_events') IS NULL THEN
    RAISE EXCEPTION 'raw.match_events is missing';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE n.nspname = 'raw'
      AND c.relname = 'match_events'
      AND c.relkind = 'p'
  ) THEN
    RAISE EXCEPTION 'raw.match_events is not a partitioned table';
  END IF;

  IF pg_get_partkeydef('raw.match_events'::regclass) IS DISTINCT FROM 'LIST (season)' THEN
    RAISE EXCEPTION 'raw.match_events partition key is not LIST (season)';
  END IF;

  SELECT COUNT(*)
  INTO actual_column_count
  FROM pg_attribute a
  WHERE a.attrelid = 'raw.match_events'::regclass
    AND a.attnum > 0
    AND NOT a.attisdropped;

  IF actual_column_count <> 25 THEN
    RAISE EXCEPTION 'raw.match_events column count mismatch: expected 25, got %', actual_column_count;
  END IF;

  FOR expected IN
    SELECT *
    FROM (
      VALUES
        ('event_id', 'text', true),
        ('season', 'integer', true),
        ('fixture_id', 'bigint', true),
        ('time_elapsed', 'integer', false),
        ('time_extra', 'integer', false),
        ('team_id', 'bigint', false),
        ('team_name', 'text', false),
        ('player_id', 'bigint', false),
        ('player_name', 'text', false),
        ('assist_id', 'bigint', false),
        ('assist_name', 'text', false),
        ('type', 'text', false),
        ('detail', 'text', false),
        ('comments', 'text', false),
        ('ingested_run', 'text', false),
        ('updated_at', 'timestamp with time zone', true),
        ('is_time_elapsed_anomalous', 'boolean', true),
        ('provider', 'text', true),
        ('provider_league_id', 'bigint', false),
        ('competition_key', 'text', false),
        ('season_label', 'text', false),
        ('provider_season_id', 'bigint', false),
        ('provider_event_id', 'text', false),
        ('ingested_at', 'timestamp with time zone', false),
        ('source_run_id', 'text', false)
    ) AS t(column_name, data_type, is_not_null)
  LOOP
    SELECT
      format_type(a.atttypid, a.atttypmod),
      a.attnotnull
    INTO
      actual_type,
      actual_not_null
    FROM pg_attribute a
    WHERE a.attrelid = 'raw.match_events'::regclass
      AND a.attname = expected.column_name
      AND a.attnum > 0
      AND NOT a.attisdropped;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'raw.match_events is missing canonical column %', expected.column_name;
    END IF;

    IF actual_type IS DISTINCT FROM expected.data_type THEN
      RAISE EXCEPTION
        'raw.match_events column % type mismatch: expected %, got %',
        expected.column_name,
        expected.data_type,
        actual_type;
    END IF;

    IF actual_not_null IS DISTINCT FROM expected.is_not_null THEN
      RAISE EXCEPTION
        'raw.match_events column % not-null mismatch: expected %, got %',
        expected.column_name,
        expected.is_not_null,
        actual_not_null;
    END IF;
  END LOOP;

  SELECT
    con.conname,
    string_agg(att.attname, ', ' ORDER BY cols.ordinality)
  INTO
    actual_pk_name,
    actual_pk_columns
  FROM pg_constraint con
  JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS cols(attnum, ordinality)
    ON TRUE
  JOIN pg_attribute att
    ON att.attrelid = con.conrelid
   AND att.attnum = cols.attnum
  WHERE con.conrelid = 'raw.match_events'::regclass
    AND con.contype = 'p'
  GROUP BY con.conname;

  IF actual_pk_name IS DISTINCT FROM 'pk_match_events' THEN
    RAISE EXCEPTION 'raw.match_events primary key name mismatch: expected pk_match_events, got %', actual_pk_name;
  END IF;

  IF actual_pk_columns IS DISTINCT FROM 'provider, season, fixture_id, event_id' THEN
    RAISE EXCEPTION
      'raw.match_events primary key columns mismatch: expected provider, season, fixture_id, event_id, got %',
      actual_pk_columns;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint con
    JOIN pg_attribute att
      ON att.attrelid = con.conrelid
     AND att.attnum = ANY (con.conkey)
    WHERE con.conrelid = 'raw.match_events'::regclass
      AND con.contype = 'f'
      AND con.conname = 'fk_match_events_fixture'
      AND con.confrelid = 'raw.fixtures'::regclass
      AND att.attname = 'fixture_id'
  ) THEN
    RAISE EXCEPTION 'raw.match_events canonical FK fk_match_events_fixture is missing';
  END IF;

  IF to_regclass('raw.match_events_2024') IS NULL THEN
    RAISE EXCEPTION 'raw.match_events_2024 partition is missing';
  END IF;

  IF to_regclass('raw.match_events_default') IS NULL THEN
    RAISE EXCEPTION 'raw.match_events_default partition is missing';
  END IF;

  IF (
    SELECT pg_get_expr(c.relpartbound, c.oid)
    FROM pg_class c
    WHERE c.oid = 'raw.match_events_2024'::regclass
  ) IS DISTINCT FROM 'FOR VALUES IN (2024)' THEN
    RAISE EXCEPTION 'raw.match_events_2024 partition bound mismatch';
  END IF;

  IF (
    SELECT pg_get_expr(c.relpartbound, c.oid)
    FROM pg_class c
    WHERE c.oid = 'raw.match_events_default'::regclass
  ) IS DISTINCT FROM 'DEFAULT' THEN
    RAISE EXCEPTION 'raw.match_events_default partition bound mismatch';
  END IF;

  IF to_regclass('raw.idx_raw_match_events_assist_id') IS NULL THEN
    RAISE EXCEPTION 'missing canonical index raw.idx_raw_match_events_assist_id';
  END IF;

  IF to_regclass('raw.idx_raw_match_events_competition_season_label') IS NULL THEN
    RAISE EXCEPTION 'missing canonical index raw.idx_raw_match_events_competition_season_label';
  END IF;

  IF to_regclass('raw.idx_raw_match_events_fixture_id') IS NULL THEN
    RAISE EXCEPTION 'missing canonical index raw.idx_raw_match_events_fixture_id';
  END IF;

  IF to_regclass('raw.idx_raw_match_events_fixture_type') IS NULL THEN
    RAISE EXCEPTION 'missing canonical index raw.idx_raw_match_events_fixture_type';
  END IF;

  IF to_regclass('raw.idx_raw_match_events_player_id') IS NULL THEN
    RAISE EXCEPTION 'missing canonical index raw.idx_raw_match_events_player_id';
  END IF;

  IF to_regclass('raw.idx_raw_match_events_team_id') IS NULL THEN
    RAISE EXCEPTION 'missing canonical index raw.idx_raw_match_events_team_id';
  END IF;
END $$;

-- migrate:down
SELECT 1;
