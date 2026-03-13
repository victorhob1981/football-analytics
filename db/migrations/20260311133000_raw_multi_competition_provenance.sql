-- migrate:up
ALTER TABLE raw.fixtures
  ADD COLUMN IF NOT EXISTS provider TEXT,
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS competition_type TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS season_name TEXT,
  ADD COLUMN IF NOT EXISTS season_start_date DATE,
  ADD COLUMN IF NOT EXISTS season_end_date DATE,
  ADD COLUMN IF NOT EXISTS stage_name TEXT,
  ADD COLUMN IF NOT EXISTS round_name TEXT,
  ADD COLUMN IF NOT EXISTS group_name TEXT,
  ADD COLUMN IF NOT EXISTS leg INT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE raw.match_statistics
  ADD COLUMN IF NOT EXISTS provider TEXT,
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE raw.match_events
  ADD COLUMN IF NOT EXISTS provider TEXT,
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS provider_event_id TEXT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE raw.competition_leagues
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS competition_type TEXT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE raw.competition_seasons
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE raw.competition_stages
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE raw.competition_rounds
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE raw.standings_snapshots
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE raw.fixture_lineups
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE raw.fixture_player_statistics
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE raw.player_season_statistics
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE raw.head_to_head_fixtures
  ADD COLUMN IF NOT EXISTS provider_league_id BIGINT,
  ADD COLUMN IF NOT EXISTS competition_key TEXT,
  ADD COLUMN IF NOT EXISTS season_label TEXT,
  ADD COLUMN IF NOT EXISTS provider_season_id BIGINT,
  ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS source_run_id TEXT;

UPDATE raw.fixtures f
SET
  provider = COALESCE(f.provider, f.source_provider, 'legacy'),
  provider_league_id = COALESCE(f.provider_league_id, f.league_id),
  season_label = COALESCE(f.season_label, f.season::text),
  round_name = COALESCE(f.round_name, f.round),
  source_run_id = COALESCE(f.source_run_id, f.ingested_run),
  ingested_at = COALESCE(f.ingested_at, now())
WHERE
  f.provider IS NULL
  OR f.provider_league_id IS NULL
  OR f.season_label IS NULL
  OR f.round_name IS NULL
  OR f.source_run_id IS NULL
  OR f.ingested_at IS NULL;

UPDATE raw.fixtures f
SET
  competition_key = m.competition_key,
  competition_type = c.competition_type
FROM control.competition_provider_map m
JOIN control.competitions c
  ON c.competition_key = m.competition_key
WHERE m.provider = f.provider
  AND m.provider_league_id = f.provider_league_id
  AND (f.competition_key IS NULL OR f.competition_type IS NULL);

UPDATE raw.competition_leagues cl
SET
  provider_league_id = COALESCE(cl.provider_league_id, cl.league_id),
  source_run_id = COALESCE(cl.source_run_id, cl.ingested_run),
  ingested_at = COALESCE(cl.ingested_at, now())
WHERE cl.provider_league_id IS NULL
   OR cl.source_run_id IS NULL
   OR cl.ingested_at IS NULL;

UPDATE raw.competition_leagues cl
SET
  competition_key = m.competition_key,
  competition_type = c.competition_type
FROM control.competition_provider_map m
JOIN control.competitions c
  ON c.competition_key = m.competition_key
WHERE m.provider = cl.provider
  AND m.provider_league_id = COALESCE(cl.provider_league_id, cl.league_id)
  AND (cl.competition_key IS NULL OR cl.competition_type IS NULL);

UPDATE raw.competition_seasons cs
SET
  provider_league_id = COALESCE(cs.provider_league_id, cs.league_id),
  provider_season_id = COALESCE(cs.provider_season_id, cs.season_id),
  season_label = COALESCE(
    cs.season_label,
    CASE
      WHEN cs.starting_at IS NOT NULL
       AND cs.ending_at IS NOT NULL
       AND EXTRACT(YEAR FROM cs.starting_at) <> EXTRACT(YEAR FROM cs.ending_at)
        THEN format('%s_%s', EXTRACT(YEAR FROM cs.starting_at)::INT, to_char(cs.ending_at, 'YY'))
      WHEN cs.season_name ~ '^[0-9]{4}/[0-9]{2,4}$'
        THEN regexp_replace(cs.season_name, '^([0-9]{4})/([0-9]{2}).*$', '\1_\2')
      ELSE COALESCE(cs.season_year::TEXT, cs.season_name, cs.season_id::TEXT)
    END
  ),
  source_run_id = COALESCE(cs.source_run_id, cs.ingested_run),
  ingested_at = COALESCE(cs.ingested_at, now())
WHERE cs.provider_league_id IS NULL
   OR cs.provider_season_id IS NULL
   OR cs.season_label IS NULL
   OR cs.source_run_id IS NULL
   OR cs.ingested_at IS NULL;

UPDATE raw.competition_seasons cs
SET competition_key = m.competition_key
FROM control.competition_provider_map m
WHERE m.provider = cs.provider
  AND m.provider_league_id = COALESCE(cs.provider_league_id, cs.league_id)
  AND cs.competition_key IS NULL;

UPDATE raw.competition_stages s
SET
  provider_league_id = COALESCE(s.provider_league_id, s.league_id),
  provider_season_id = COALESCE(s.provider_season_id, s.season_id),
  source_run_id = COALESCE(s.source_run_id, s.ingested_run),
  ingested_at = COALESCE(s.ingested_at, now())
WHERE s.provider_league_id IS NULL
   OR s.provider_season_id IS NULL
   OR s.source_run_id IS NULL
   OR s.ingested_at IS NULL;

UPDATE raw.competition_stages s
SET
  competition_key = cs.competition_key,
  season_label = COALESCE(s.season_label, cs.season_label)
FROM raw.competition_seasons cs
WHERE cs.provider = s.provider
  AND cs.season_id = s.season_id
  AND (s.competition_key IS NULL OR s.season_label IS NULL);

UPDATE raw.competition_rounds r
SET
  provider_league_id = COALESCE(r.provider_league_id, r.league_id),
  provider_season_id = COALESCE(r.provider_season_id, r.season_id),
  source_run_id = COALESCE(r.source_run_id, r.ingested_run),
  ingested_at = COALESCE(r.ingested_at, now())
WHERE r.provider_league_id IS NULL
   OR r.provider_season_id IS NULL
   OR r.source_run_id IS NULL
   OR r.ingested_at IS NULL;

UPDATE raw.competition_rounds r
SET
  competition_key = cs.competition_key,
  season_label = COALESCE(r.season_label, cs.season_label)
FROM raw.competition_seasons cs
WHERE cs.provider = r.provider
  AND cs.season_id = r.season_id
  AND (r.competition_key IS NULL OR r.season_label IS NULL);

UPDATE raw.standings_snapshots s
SET
  provider_league_id = COALESCE(s.provider_league_id, s.league_id),
  provider_season_id = COALESCE(s.provider_season_id, s.season_id),
  source_run_id = COALESCE(s.source_run_id, s.ingested_run),
  ingested_at = COALESCE(s.ingested_at, now())
WHERE s.provider_league_id IS NULL
   OR s.provider_season_id IS NULL
   OR s.source_run_id IS NULL
   OR s.ingested_at IS NULL;

UPDATE raw.standings_snapshots s
SET
  competition_key = cs.competition_key,
  season_label = COALESCE(s.season_label, cs.season_label)
FROM raw.competition_seasons cs
WHERE cs.provider = s.provider
  AND cs.season_id = s.season_id
  AND (s.competition_key IS NULL OR s.season_label IS NULL);

UPDATE raw.match_statistics ms
SET
  provider = COALESCE(ms.provider, f.provider),
  provider_league_id = COALESCE(ms.provider_league_id, f.provider_league_id),
  competition_key = COALESCE(ms.competition_key, f.competition_key),
  season_label = COALESCE(ms.season_label, f.season_label),
  provider_season_id = COALESCE(ms.provider_season_id, f.provider_season_id),
  source_run_id = COALESCE(ms.source_run_id, ms.ingested_run, f.source_run_id),
  ingested_at = COALESCE(ms.ingested_at, now())
FROM raw.fixtures f
WHERE f.fixture_id = ms.fixture_id
  AND (
    ms.provider IS NULL
    OR ms.provider_league_id IS NULL
    OR ms.competition_key IS NULL
    OR ms.season_label IS NULL
    OR ms.provider_season_id IS NULL
    OR ms.source_run_id IS NULL
    OR ms.ingested_at IS NULL
  );

UPDATE raw.match_events e
SET
  provider = COALESCE(e.provider, f.provider),
  provider_league_id = COALESCE(e.provider_league_id, f.provider_league_id),
  competition_key = COALESCE(e.competition_key, f.competition_key),
  season_label = COALESCE(e.season_label, f.season_label),
  provider_season_id = COALESCE(e.provider_season_id, f.provider_season_id),
  source_run_id = COALESCE(e.source_run_id, e.ingested_run, f.source_run_id),
  ingested_at = COALESCE(e.ingested_at, now())
FROM raw.fixtures f
WHERE f.fixture_id = e.fixture_id
  AND (
    e.provider IS NULL
    OR e.provider_league_id IS NULL
    OR e.competition_key IS NULL
    OR e.season_label IS NULL
    OR e.provider_season_id IS NULL
    OR e.source_run_id IS NULL
    OR e.ingested_at IS NULL
  );

UPDATE raw.fixture_lineups fl
SET
  provider_league_id = COALESCE(fl.provider_league_id, f.provider_league_id),
  competition_key = COALESCE(fl.competition_key, f.competition_key),
  season_label = COALESCE(fl.season_label, f.season_label),
  provider_season_id = COALESCE(fl.provider_season_id, f.provider_season_id),
  source_run_id = COALESCE(fl.source_run_id, fl.ingested_run, f.source_run_id),
  ingested_at = COALESCE(fl.ingested_at, now())
FROM raw.fixtures f
WHERE f.fixture_id = fl.fixture_id
  AND (
    fl.provider_league_id IS NULL
    OR fl.competition_key IS NULL
    OR fl.season_label IS NULL
    OR fl.provider_season_id IS NULL
    OR fl.source_run_id IS NULL
    OR fl.ingested_at IS NULL
  );

UPDATE raw.fixture_player_statistics fps
SET
  provider_league_id = COALESCE(fps.provider_league_id, f.provider_league_id),
  competition_key = COALESCE(fps.competition_key, f.competition_key),
  season_label = COALESCE(fps.season_label, f.season_label),
  provider_season_id = COALESCE(fps.provider_season_id, f.provider_season_id),
  source_run_id = COALESCE(fps.source_run_id, fps.ingested_run, f.source_run_id),
  ingested_at = COALESCE(fps.ingested_at, now())
FROM raw.fixtures f
WHERE f.fixture_id = fps.fixture_id
  AND (
    fps.provider_league_id IS NULL
    OR fps.competition_key IS NULL
    OR fps.season_label IS NULL
    OR fps.provider_season_id IS NULL
    OR fps.source_run_id IS NULL
    OR fps.ingested_at IS NULL
  );

UPDATE raw.player_season_statistics pss
SET
  provider_league_id = COALESCE(pss.provider_league_id, pss.league_id),
  provider_season_id = COALESCE(pss.provider_season_id, pss.season_id),
  source_run_id = COALESCE(pss.source_run_id, pss.ingested_run),
  ingested_at = COALESCE(pss.ingested_at, now())
WHERE pss.provider_league_id IS NULL
   OR pss.provider_season_id IS NULL
   OR pss.source_run_id IS NULL
   OR pss.ingested_at IS NULL;

UPDATE raw.player_season_statistics pss
SET
  competition_key = COALESCE(pss.competition_key, cs.competition_key),
  season_label = COALESCE(pss.season_label, cs.season_label)
FROM raw.competition_seasons cs
WHERE cs.provider = pss.provider
  AND cs.season_id = pss.season_id
  AND (pss.competition_key IS NULL OR pss.season_label IS NULL);

UPDATE raw.head_to_head_fixtures h
SET
  provider_league_id = COALESCE(h.provider_league_id, h.league_id),
  provider_season_id = COALESCE(h.provider_season_id, h.season_id),
  source_run_id = COALESCE(h.source_run_id, h.ingested_run),
  ingested_at = COALESCE(h.ingested_at, now())
WHERE h.provider_league_id IS NULL
   OR h.provider_season_id IS NULL
   OR h.source_run_id IS NULL
   OR h.ingested_at IS NULL;

UPDATE raw.head_to_head_fixtures h
SET
  competition_key = COALESCE(h.competition_key, cs.competition_key),
  season_label = COALESCE(h.season_label, cs.season_label)
FROM raw.competition_seasons cs
WHERE cs.provider = h.provider
  AND cs.season_id = h.season_id
  AND (h.competition_key IS NULL OR h.season_label IS NULL);

CREATE INDEX IF NOT EXISTS idx_raw_fixtures_competition_season_label
  ON raw.fixtures (competition_key, season_label);

CREATE INDEX IF NOT EXISTS idx_raw_match_statistics_competition_season_label
  ON raw.match_statistics (competition_key, season_label);

CREATE INDEX IF NOT EXISTS idx_raw_match_events_competition_season_label
  ON raw.match_events (competition_key, season_label);

CREATE INDEX IF NOT EXISTS idx_raw_competition_seasons_competition_season_label
  ON raw.competition_seasons (competition_key, season_label);

CREATE INDEX IF NOT EXISTS idx_raw_standings_snapshots_competition_season_label
  ON raw.standings_snapshots (competition_key, season_label);

CREATE INDEX IF NOT EXISTS idx_raw_head_to_head_competition_season_label
  ON raw.head_to_head_fixtures (competition_key, season_label);

-- migrate:down
DROP INDEX IF EXISTS idx_raw_head_to_head_competition_season_label;
DROP INDEX IF EXISTS idx_raw_standings_snapshots_competition_season_label;
DROP INDEX IF EXISTS idx_raw_competition_seasons_competition_season_label;
DROP INDEX IF EXISTS idx_raw_match_events_competition_season_label;
DROP INDEX IF EXISTS idx_raw_match_statistics_competition_season_label;
DROP INDEX IF EXISTS idx_raw_fixtures_competition_season_label;

ALTER TABLE raw.head_to_head_fixtures
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS provider_season_id,
  DROP COLUMN IF EXISTS season_label,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id;

ALTER TABLE raw.player_season_statistics
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS provider_season_id,
  DROP COLUMN IF EXISTS season_label,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id;

ALTER TABLE raw.fixture_player_statistics
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS provider_season_id,
  DROP COLUMN IF EXISTS season_label,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id;

ALTER TABLE raw.fixture_lineups
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS provider_season_id,
  DROP COLUMN IF EXISTS season_label,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id;

ALTER TABLE raw.standings_snapshots
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS provider_season_id,
  DROP COLUMN IF EXISTS season_label,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id;

ALTER TABLE raw.competition_rounds
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS provider_season_id,
  DROP COLUMN IF EXISTS season_label,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id;

ALTER TABLE raw.competition_stages
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS provider_season_id,
  DROP COLUMN IF EXISTS season_label,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id;

ALTER TABLE raw.competition_seasons
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS provider_season_id,
  DROP COLUMN IF EXISTS season_label,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id;

ALTER TABLE raw.competition_leagues
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS competition_type,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id;

ALTER TABLE raw.match_events
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS provider_event_id,
  DROP COLUMN IF EXISTS provider_season_id,
  DROP COLUMN IF EXISTS season_label,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id,
  DROP COLUMN IF EXISTS provider;

ALTER TABLE raw.match_statistics
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS provider_season_id,
  DROP COLUMN IF EXISTS season_label,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id,
  DROP COLUMN IF EXISTS provider;

ALTER TABLE raw.fixtures
  DROP COLUMN IF EXISTS source_run_id,
  DROP COLUMN IF EXISTS ingested_at,
  DROP COLUMN IF EXISTS leg,
  DROP COLUMN IF EXISTS group_name,
  DROP COLUMN IF EXISTS round_name,
  DROP COLUMN IF EXISTS stage_name,
  DROP COLUMN IF EXISTS season_end_date,
  DROP COLUMN IF EXISTS season_start_date,
  DROP COLUMN IF EXISTS season_name,
  DROP COLUMN IF EXISTS provider_season_id,
  DROP COLUMN IF EXISTS season_label,
  DROP COLUMN IF EXISTS competition_type,
  DROP COLUMN IF EXISTS competition_key,
  DROP COLUMN IF EXISTS provider_league_id,
  DROP COLUMN IF EXISTS provider;
