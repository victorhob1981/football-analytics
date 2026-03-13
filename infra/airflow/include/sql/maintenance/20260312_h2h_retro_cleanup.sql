-- Head-to-head retro cleanup
-- Batch: 20260312_h2h_retro_cleanup
-- Order:
-- 1. Run pre-cleanup diagnostics
-- 2. Create persistent snapshots (orphan + semantic scope)
-- 3. Review snapshot counts
-- 4. Delete using snapshot tables only
-- 5. Run post-cleanup verification
--
-- Safety:
-- - Snapshot tables are persistent in schema control
-- - Deletes are driven by snapshot tables, never by ad-hoc WHERE clauses
-- - If you need to rerun with a new snapshot, copy this file and change the batch/table suffix

\echo 'STEP 1 - pre-cleanup diagnostics'

SELECT COUNT(*) AS total_h2h_rows
FROM raw.head_to_head_fixtures;

SELECT COUNT(*) AS orphan_rows
FROM raw.head_to_head_fixtures h2h
WHERE NOT EXISTS (
    SELECT 1
    FROM raw.fixtures f
    WHERE f.fixture_id = h2h.fixture_id
);

SELECT COUNT(*) AS scope_cleanup_rows
FROM raw.head_to_head_fixtures h2h
JOIN raw.fixtures f
  ON f.fixture_id = h2h.fixture_id
WHERE h2h.competition_key IS NULL
   OR h2h.season_label IS NULL
   OR NOT EXISTS (
        SELECT 1
        FROM control.season_catalog sc
        WHERE sc.provider = h2h.provider
          AND sc.competition_key = h2h.competition_key
          AND sc.season_label = h2h.season_label
    )
   OR h2h.provider IS DISTINCT FROM f.provider
   OR h2h.league_id IS DISTINCT FROM f.league_id
   OR h2h.competition_key IS DISTINCT FROM f.competition_key
   OR h2h.season_label IS DISTINCT FROM f.season_label
   OR h2h.provider_season_id IS DISTINCT FROM f.provider_season_id
   OR h2h.season_id IS DISTINCT FROM f.provider_season_id;

WITH orphan_keys AS (
    SELECT provider, pair_team_id, pair_opponent_id, fixture_id
    FROM raw.head_to_head_fixtures h2h
    WHERE NOT EXISTS (
        SELECT 1
        FROM raw.fixtures f
        WHERE f.fixture_id = h2h.fixture_id
    )
),
scope_keys AS (
    SELECT h2h.provider, h2h.pair_team_id, h2h.pair_opponent_id, h2h.fixture_id
    FROM raw.head_to_head_fixtures h2h
    JOIN raw.fixtures f
      ON f.fixture_id = h2h.fixture_id
    WHERE h2h.competition_key IS NULL
       OR h2h.season_label IS NULL
       OR NOT EXISTS (
            SELECT 1
            FROM control.season_catalog sc
            WHERE sc.provider = h2h.provider
              AND sc.competition_key = h2h.competition_key
              AND sc.season_label = h2h.season_label
        )
       OR h2h.provider IS DISTINCT FROM f.provider
       OR h2h.league_id IS DISTINCT FROM f.league_id
       OR h2h.competition_key IS DISTINCT FROM f.competition_key
       OR h2h.season_label IS DISTINCT FROM f.season_label
       OR h2h.provider_season_id IS DISTINCT FROM f.provider_season_id
       OR h2h.season_id IS DISTINCT FROM f.provider_season_id
)
SELECT
    (SELECT COUNT(*) FROM orphan_keys) AS orphan_rows,
    (SELECT COUNT(*) FROM scope_keys) AS scope_rows,
    (SELECT COUNT(*) FROM (
        SELECT * FROM orphan_keys
        UNION
        SELECT * FROM scope_keys
    ) u) AS total_distinct_candidates;

\echo 'STEP 2 - persistent snapshots'

CREATE TABLE control.h2h_cleanup_20260312_orphan_snapshot AS
SELECT
    h2h.*,
    now() AS snapshot_at,
    '20260312_h2h_retro_cleanup'::text AS cleanup_batch,
    'orphan_fixture_id'::text AS snapshot_reason
FROM raw.head_to_head_fixtures h2h
WHERE NOT EXISTS (
    SELECT 1
    FROM raw.fixtures f
    WHERE f.fixture_id = h2h.fixture_id
);

CREATE INDEX idx_h2h_cleanup_20260312_orphan_snapshot_pk
  ON control.h2h_cleanup_20260312_orphan_snapshot (provider, pair_team_id, pair_opponent_id, fixture_id);

CREATE TABLE control.h2h_cleanup_20260312_scope_snapshot AS
SELECT
    h2h.*,
    f.provider AS fixture_provider,
    f.league_id AS fixture_league_id,
    f.competition_key AS fixture_competition_key,
    f.season AS fixture_season,
    f.season_label AS fixture_season_label,
    f.provider_season_id AS fixture_provider_season_id,
    (
        EXISTS (
            SELECT 1
            FROM control.season_catalog sc
            WHERE sc.provider = h2h.provider
              AND sc.competition_key = h2h.competition_key
              AND sc.season_label = h2h.season_label
        )
    ) AS matches_catalog,
    (h2h.competition_key IS NULL) AS is_null_competition_key,
    (h2h.season_label IS NULL) AS is_null_season_label,
    (NOT EXISTS (
        SELECT 1
        FROM control.season_catalog sc
        WHERE sc.provider = h2h.provider
          AND sc.competition_key = h2h.competition_key
          AND sc.season_label = h2h.season_label
    )) AS is_outside_catalog,
    (h2h.provider IS DISTINCT FROM f.provider) AS is_provider_mismatch,
    (h2h.league_id IS DISTINCT FROM f.league_id) AS is_league_mismatch,
    (h2h.competition_key IS DISTINCT FROM f.competition_key) AS is_competition_mismatch,
    (h2h.season_label IS DISTINCT FROM f.season_label) AS is_season_label_mismatch,
    (h2h.provider_season_id IS DISTINCT FROM f.provider_season_id) AS is_provider_season_mismatch,
    (h2h.season_id IS DISTINCT FROM f.provider_season_id) AS is_season_id_mismatch,
    CASE
        WHEN h2h.competition_key IS NULL THEN 'null_competition_key'
        WHEN h2h.season_label IS NULL THEN 'null_season_label'
        WHEN NOT EXISTS (
            SELECT 1
            FROM control.season_catalog sc
            WHERE sc.provider = h2h.provider
              AND sc.competition_key = h2h.competition_key
              AND sc.season_label = h2h.season_label
        ) THEN 'outside_catalog'
        WHEN h2h.provider IS DISTINCT FROM f.provider THEN 'provider_mismatch'
        WHEN h2h.league_id IS DISTINCT FROM f.league_id THEN 'league_mismatch'
        WHEN h2h.competition_key IS DISTINCT FROM f.competition_key THEN 'competition_mismatch'
        WHEN h2h.season_label IS DISTINCT FROM f.season_label THEN 'season_label_mismatch'
        WHEN h2h.provider_season_id IS DISTINCT FROM f.provider_season_id THEN 'provider_season_mismatch'
        WHEN h2h.season_id IS DISTINCT FROM f.provider_season_id THEN 'season_id_mismatch'
        ELSE 'unknown_scope_issue'
    END AS snapshot_reason,
    now() AS snapshot_at,
    '20260312_h2h_retro_cleanup'::text AS cleanup_batch
FROM raw.head_to_head_fixtures h2h
JOIN raw.fixtures f
  ON f.fixture_id = h2h.fixture_id
WHERE h2h.competition_key IS NULL
   OR h2h.season_label IS NULL
   OR NOT EXISTS (
        SELECT 1
        FROM control.season_catalog sc
        WHERE sc.provider = h2h.provider
          AND sc.competition_key = h2h.competition_key
          AND sc.season_label = h2h.season_label
    )
   OR h2h.provider IS DISTINCT FROM f.provider
   OR h2h.league_id IS DISTINCT FROM f.league_id
   OR h2h.competition_key IS DISTINCT FROM f.competition_key
   OR h2h.season_label IS DISTINCT FROM f.season_label
   OR h2h.provider_season_id IS DISTINCT FROM f.provider_season_id
   OR h2h.season_id IS DISTINCT FROM f.provider_season_id;

CREATE INDEX idx_h2h_cleanup_20260312_scope_snapshot_pk
  ON control.h2h_cleanup_20260312_scope_snapshot (provider, pair_team_id, pair_opponent_id, fixture_id);

\echo 'STEP 3 - snapshot review'

SELECT 'orphan_snapshot' AS snapshot_name, COUNT(*) AS rows_count
FROM control.h2h_cleanup_20260312_orphan_snapshot
UNION ALL
SELECT 'scope_snapshot' AS snapshot_name, COUNT(*) AS rows_count
FROM control.h2h_cleanup_20260312_scope_snapshot
UNION ALL
SELECT 'distinct_total_candidates' AS snapshot_name, COUNT(*)
FROM (
    SELECT provider, pair_team_id, pair_opponent_id, fixture_id
    FROM control.h2h_cleanup_20260312_orphan_snapshot
    UNION
    SELECT provider, pair_team_id, pair_opponent_id, fixture_id
    FROM control.h2h_cleanup_20260312_scope_snapshot
) u;

SELECT snapshot_reason, COUNT(*) AS rows_count
FROM control.h2h_cleanup_20260312_scope_snapshot
GROUP BY snapshot_reason
ORDER BY rows_count DESC, snapshot_reason;

\echo 'STEP 4 - delete using snapshots only'

WITH deleted AS (
    DELETE FROM raw.head_to_head_fixtures h2h
    USING control.h2h_cleanup_20260312_orphan_snapshot s
    WHERE h2h.provider = s.provider
      AND h2h.pair_team_id = s.pair_team_id
      AND h2h.pair_opponent_id = s.pair_opponent_id
      AND h2h.fixture_id = s.fixture_id
    RETURNING 1
)
SELECT COUNT(*) AS orphan_rows_deleted
FROM deleted;

WITH deleted AS (
    DELETE FROM raw.head_to_head_fixtures h2h
    USING control.h2h_cleanup_20260312_scope_snapshot s
    WHERE h2h.provider = s.provider
      AND h2h.pair_team_id = s.pair_team_id
      AND h2h.pair_opponent_id = s.pair_opponent_id
      AND h2h.fixture_id = s.fixture_id
    RETURNING 1
)
SELECT COUNT(*) AS scope_rows_deleted
FROM deleted;

\echo 'STEP 5 - post-cleanup verification'

SELECT COUNT(*) AS total_h2h_rows_after_cleanup
FROM raw.head_to_head_fixtures;

SELECT COUNT(*) AS orphan_rows_remaining
FROM raw.head_to_head_fixtures h2h
WHERE NOT EXISTS (
    SELECT 1
    FROM raw.fixtures f
    WHERE f.fixture_id = h2h.fixture_id
);

SELECT COUNT(*) AS scope_rows_remaining
FROM raw.head_to_head_fixtures h2h
JOIN raw.fixtures f
  ON f.fixture_id = h2h.fixture_id
WHERE h2h.competition_key IS NULL
   OR h2h.season_label IS NULL
   OR NOT EXISTS (
        SELECT 1
        FROM control.season_catalog sc
        WHERE sc.provider = h2h.provider
          AND sc.competition_key = h2h.competition_key
          AND sc.season_label = h2h.season_label
    )
   OR h2h.provider IS DISTINCT FROM f.provider
   OR h2h.league_id IS DISTINCT FROM f.league_id
   OR h2h.competition_key IS DISTINCT FROM f.competition_key
   OR h2h.season_label IS DISTINCT FROM f.season_label
   OR h2h.provider_season_id IS DISTINCT FROM f.provider_season_id
   OR h2h.season_id IS DISTINCT FROM f.provider_season_id;

/*
Optional restore from snapshot tables:

INSERT INTO raw.head_to_head_fixtures (
    provider,
    pair_team_id,
    pair_opponent_id,
    fixture_id,
    league_id,
    provider_league_id,
    competition_key,
    season_label,
    season_id,
    provider_season_id,
    match_date,
    home_team_id,
    away_team_id,
    home_goals,
    away_goals,
    payload,
    ingested_at,
    source_run_id,
    ingested_run,
    updated_at
)
SELECT
    provider,
    pair_team_id,
    pair_opponent_id,
    fixture_id,
    league_id,
    provider_league_id,
    competition_key,
    season_label,
    season_id,
    provider_season_id,
    match_date,
    home_team_id,
    away_team_id,
    home_goals,
    away_goals,
    payload,
    ingested_at,
    source_run_id,
    ingested_run,
    updated_at
FROM control.h2h_cleanup_20260312_orphan_snapshot
UNION ALL
SELECT
    provider,
    pair_team_id,
    pair_opponent_id,
    fixture_id,
    league_id,
    provider_league_id,
    competition_key,
    season_label,
    season_id,
    provider_season_id,
    match_date,
    home_team_id,
    away_team_id,
    home_goals,
    away_goals,
    payload,
    ingested_at,
    source_run_id,
    ingested_run,
    updated_at
FROM control.h2h_cleanup_20260312_scope_snapshot;
*/
