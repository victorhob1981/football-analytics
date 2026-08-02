\set ON_ERROR_STOP on
\if :{?rebuild_run_key}
\else
  \set rebuild_run_key 'mart-v2-local-current'
\endif

SELECT rebuild_run_id
FROM control.rebuild_run
WHERE run_key = :'rebuild_run_key'\gset rebuild_

DROP TABLE IF EXISTS tmp_match_duplicate_map;
CREATE TEMP TABLE tmp_match_duplicate_map ON COMMIT PRESERVE ROWS AS
WITH ranked AS (
  SELECT
    f.match_id,
    f.edition_key,
    f.match_date,
    f.home_team_id,
    f.away_team_id,
    row_number() OVER (
      PARTITION BY f.edition_key, f.match_date, f.home_team_id, f.away_team_id
      ORDER BY CASE f.source_system
        WHEN 'sportmonks' THEN 10
        WHEN 'world_cup_2022' THEN 10
        WHEN 'dataset_brasileirao' THEN 20
        WHEN 'statsbomb_open_data' THEN 30
        WHEN 'transfermarkt' THEN 40
        WHEN 'eloratings' THEN 50
        ELSE 90
      END,
      f.match_id
    ) AS duplicate_rank
  FROM mart_v2.fact_match f
  WHERE f.publication_state = 'published'
    AND f.edition_key IS NOT NULL
    AND f.match_date IS NOT NULL
    AND f.home_team_id IS NOT NULL
    AND f.away_team_id IS NOT NULL
)
SELECT
  loser.match_id AS duplicate_match_id,
  winner.match_id AS canonical_match_id,
  loser.edition_key,
  loser.match_date,
  loser.home_team_id,
  loser.away_team_id
FROM ranked loser
JOIN ranked winner
  ON winner.edition_key = loser.edition_key
 AND winner.match_date = loser.match_date
 AND winner.home_team_id = loser.home_team_id
 AND winner.away_team_id = loser.away_team_id
 AND winner.duplicate_rank = 1
WHERE loser.duplicate_rank > 1;

UPDATE mart_v2.fact_match f
SET publication_state = 'quarantined',
    publication_reason = 'duplicate_of:' || d.canonical_match_id::text,
    metadata = f.metadata || jsonb_build_object('canonical_match_id', d.canonical_match_id, 'dedup_rule', 'edition_date_home_away_priority'),
    rebuild_run_id = :rebuild_rebuild_run_id
FROM tmp_match_duplicate_map d
WHERE f.match_id = d.duplicate_match_id;

UPDATE mart_v2.match_source s
SET canonical_match_id = d.canonical_match_id,
    reconciliation_state = 'approved',
    method = coalesce(s.method, 'duplicate_semantic_reconciliation') || ':duplicate_semantic_reconciliation',
    evidence = s.evidence || jsonb_build_object('duplicate_match_id', d.duplicate_match_id, 'canonical_match_id', d.canonical_match_id),
    rebuild_run_id = :rebuild_rebuild_run_id
FROM tmp_match_duplicate_map d
WHERE s.canonical_match_id = d.duplicate_match_id;

INSERT INTO control.source_system (source_system, source_kind, priority, description)
SELECT DISTINCT source_system, 'derived', 100, 'Source observed during v2 match reconciliation'
FROM mart_v2.match_source
ON CONFLICT (source_system) DO NOTHING;

INSERT INTO control.match_reconciliation (
  source_system, source_match_id, canonical_match_id, competition_key, edition_key,
  reconciliation_state, method, confidence, source_date, evidence, rebuild_run_id
)
SELECT
  s.source_system,
  s.source_match_id,
  s.canonical_match_id,
  f.competition_key,
  f.edition_key,
  s.reconciliation_state,
  s.method,
  s.confidence,
  s.source_date,
  s.evidence,
  :rebuild_rebuild_run_id
FROM mart_v2.match_source s
LEFT JOIN mart_v2.fact_match f ON f.match_id = s.canonical_match_id
ON CONFLICT (source_system, source_match_id) DO UPDATE
SET canonical_match_id = EXCLUDED.canonical_match_id,
    competition_key = EXCLUDED.competition_key,
    edition_key = EXCLUDED.edition_key,
    reconciliation_state = EXCLUDED.reconciliation_state,
    method = EXCLUDED.method,
    confidence = EXCLUDED.confidence,
    source_date = EXCLUDED.source_date,
    evidence = EXCLUDED.evidence,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

INSERT INTO control.quality_issue (
  rebuild_run_id, entity_type, entity_key, rule_code, severity, disposition, evidence
)
SELECT
  :rebuild_rebuild_run_id,
  'match',
  d.duplicate_match_id::text,
  'duplicate_match_semantic_key',
  'warning',
  'quarantined',
  jsonb_build_object(
    'canonical_match_id', d.canonical_match_id,
    'edition_key', d.edition_key,
    'match_date', d.match_date,
    'home_team_id', d.home_team_id,
    'away_team_id', d.away_team_id
  )
FROM tmp_match_duplicate_map d
ON CONFLICT (rebuild_run_id, entity_type, entity_key, rule_code) DO UPDATE
SET evidence = EXCLUDED.evidence;

INSERT INTO control.publication_decision (
  entity_type, entity_key, publication_state, reason_code, rebuild_run_id, evidence
)
SELECT
  'match',
  f.match_id::text,
  f.publication_state,
  f.publication_reason,
  :rebuild_rebuild_run_id,
  jsonb_build_object('edition_key', f.edition_key, 'source_system', f.source_system)
FROM mart_v2.fact_match f
ON CONFLICT (entity_type, entity_key) DO UPDATE
SET publication_state = EXCLUDED.publication_state,
    reason_code = EXCLUDED.reason_code,
    rebuild_run_id = EXCLUDED.rebuild_run_id,
    evidence = EXCLUDED.evidence,
    decided_at = now();

UPDATE mart_v2.dim_edition e
SET published_match_count = coalesce(x.published_match_count, 0),
    publication_state = CASE WHEN coalesce(x.published_match_count, 0) > 0 THEN 'published' ELSE 'pending' END,
    first_match_date = x.first_match_date,
    last_match_date = x.last_match_date
FROM (
  SELECT edition_key,
         count(*) FILTER (WHERE publication_state = 'published') AS published_match_count,
         min(match_date) FILTER (WHERE publication_state = 'published') AS first_match_date,
         max(match_date) FILTER (WHERE publication_state = 'published') AS last_match_date
  FROM mart_v2.fact_match
  WHERE edition_key IS NOT NULL
  GROUP BY edition_key
) x
WHERE e.edition_key = x.edition_key;

UPDATE control.rebuild_run
SET phase = 'match_reconciliation', status = 'succeeded', finished_at = now()
WHERE rebuild_run_id = :rebuild_rebuild_run_id;
