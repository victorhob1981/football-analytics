\set ON_ERROR_STOP on
\if :{?rebuild_run_key}
\else
  \set rebuild_run_key 'mart-v2-local-current'
\endif

SELECT rebuild_run_id
FROM control.rebuild_run
WHERE run_key = :'rebuild_run_key'\gset rebuild_

DELETE FROM control.coverage_reconciliation
WHERE rebuild_run_id = :rebuild_rebuild_run_id;

INSERT INTO control.coverage_reconciliation (
  rebuild_run_id, scope_name, source_system, reference_rows, observed_rows,
  published_rows, quarantined_rows, pending_rows, disposition, evidence
)
SELECT
  :rebuild_rebuild_run_id,
  'legacy_shadow_match_reference',
  'legacy_reference',
  (SELECT count(*) FROM raw_reference.fact_matches),
  (SELECT count(*) FROM mart_v2.fact_match),
  (SELECT count(*) FROM mart_v2.fact_match WHERE publication_state = 'published'),
  (SELECT count(*) FROM mart_v2.fact_match WHERE publication_state = 'quarantined'),
  (SELECT count(*) FROM mart_v2.fact_match WHERE publication_state = 'pending'),
  CASE WHEN (SELECT count(*) FROM mart_v2.match_source WHERE reconciliation_state = 'pending') > 0 THEN 'pending' ELSE 'explained' END,
  jsonb_build_object(
    'comparison_rule', 'reference is legacy shadow scope; v2 is multi-source canonical scope',
    'reference_table', 'shadow_dbt_20260716.fact_matches',
    'v2_difference_requires_breakdown', true
  );

INSERT INTO control.coverage_reconciliation (
  rebuild_run_id, scope_name, source_system, competition_key, edition_key,
  observed_rows, published_rows, quarantined_rows, pending_rows, disposition, evidence
)
SELECT
  :rebuild_rebuild_run_id,
  'source_match',
  s.source_system,
  coalesce(f.competition_key, ''),
  coalesce(f.edition_key, ''),
  count(*)::bigint,
  count(*) FILTER (WHERE f.publication_state = 'published')::bigint,
  count(*) FILTER (WHERE f.publication_state = 'quarantined')::bigint,
  count(*) FILTER (WHERE s.reconciliation_state = 'pending')::bigint,
  CASE
    WHEN count(*) FILTER (WHERE s.reconciliation_state = 'pending') > 0 THEN 'pending'
    WHEN count(*) FILTER (WHERE f.publication_state = 'quarantined') > 0 THEN 'quarantined'
    ELSE 'complete'
  END,
  jsonb_build_object('source_table', 'mart_v2.match_source')
FROM mart_v2.match_source s
LEFT JOIN mart_v2.fact_match f ON f.match_id = s.canonical_match_id
GROUP BY s.source_system, coalesce(f.competition_key, ''), coalesce(f.edition_key, '')
ON CONFLICT DO NOTHING;

INSERT INTO control.coverage_snapshot (
  rebuild_run_id, entity_type, source_system, competition_key, edition_key,
  observed_rows, accepted_rows, quarantined_rows, first_date, last_date, metadata
)
SELECT
  :rebuild_rebuild_run_id,
  'match',
  s.source_system,
  coalesce(f.competition_key, ''),
  coalesce(f.edition_key, ''),
  count(*)::bigint,
  count(*) FILTER (WHERE s.reconciliation_state = 'approved')::bigint,
  count(*) FILTER (WHERE s.reconciliation_state IN ('quarantined', 'rejected', 'ambiguous'))::bigint,
  min(s.source_date),
  max(s.source_date),
  jsonb_build_object('publication_states', jsonb_build_object(
    'published', count(*) FILTER (WHERE f.publication_state = 'published'),
    'quarantined', count(*) FILTER (WHERE f.publication_state = 'quarantined')
  ))
FROM mart_v2.match_source s
LEFT JOIN mart_v2.fact_match f ON f.match_id = s.canonical_match_id
GROUP BY s.source_system, coalesce(f.competition_key, ''), coalesce(f.edition_key, '')
ON CONFLICT (rebuild_run_id, entity_type, source_system, competition_key, edition_key) DO UPDATE
SET observed_rows = EXCLUDED.observed_rows,
    accepted_rows = EXCLUDED.accepted_rows,
    quarantined_rows = EXCLUDED.quarantined_rows,
    first_date = EXCLUDED.first_date,
    last_date = EXCLUDED.last_date,
    metadata = EXCLUDED.metadata;

INSERT INTO control.rebuild_fingerprint (rebuild_run_id, object_name, row_count, fingerprint, metadata)
SELECT :rebuild_rebuild_run_id, 'mart_v2.fact_match', q.row_count,
       md5(format('%s|%s|%s|%s', q.row_count, q.hash_xor, q.hash_sum, q.hash_minmax)),
       jsonb_build_object('columns', 'match_id,match_key,source_system,source_match_id,competition_key,edition_key,match_date,teams,goals,publication_state,publication_reason')
FROM (
  SELECT count(*)::bigint AS row_count,
         coalesce(bit_xor(hashtextextended(concat_ws('|', match_id, match_key, source_system, source_match_id, competition_key, edition_key, match_date, home_team_id, away_team_id, home_goals, away_goals, publication_state, publication_reason), 0)), 0) AS hash_xor,
         coalesce(sum(hashtextextended(concat_ws('|', match_id, match_key, source_system, source_match_id, competition_key, edition_key, match_date, home_team_id, away_team_id, home_goals, away_goals, publication_state, publication_reason), 0)), 0) AS hash_sum,
         concat_ws(':', min(hashtextextended(concat_ws('|', match_id, match_key, source_system, source_match_id), 0)), max(hashtextextended(concat_ws('|', match_id, match_key, source_system, source_match_id), 0))) AS hash_minmax
  FROM mart_v2.fact_match
) q
ON CONFLICT (rebuild_run_id, object_name) DO UPDATE
SET row_count = EXCLUDED.row_count, fingerprint = EXCLUDED.fingerprint, metadata = EXCLUDED.metadata;

INSERT INTO control.rebuild_fingerprint (rebuild_run_id, object_name, row_count, fingerprint, metadata)
SELECT :rebuild_rebuild_run_id, 'mart_v2.match_source', q.row_count,
       md5(format('%s|%s|%s|%s', q.row_count, q.hash_xor, q.hash_sum, q.hash_minmax)),
       jsonb_build_object('columns', 'source_system,source_match_id,canonical_match_id,reconciliation_state,method,confidence')
FROM (
  SELECT count(*)::bigint AS row_count,
         coalesce(bit_xor(hashtextextended(concat_ws('|', source_system, source_match_id, canonical_match_id, reconciliation_state, method, confidence), 0)), 0) AS hash_xor,
         coalesce(sum(hashtextextended(concat_ws('|', source_system, source_match_id, canonical_match_id, reconciliation_state, method, confidence), 0)), 0) AS hash_sum,
         concat_ws(':', min(hashtextextended(concat_ws('|', source_system, source_match_id), 0)), max(hashtextextended(concat_ws('|', source_system, source_match_id), 0))) AS hash_minmax
  FROM mart_v2.match_source
) q
ON CONFLICT (rebuild_run_id, object_name) DO UPDATE
SET row_count = EXCLUDED.row_count, fingerprint = EXCLUDED.fingerprint, metadata = EXCLUDED.metadata;

INSERT INTO control.rebuild_fingerprint (rebuild_run_id, object_name, row_count, fingerprint, metadata)
SELECT :rebuild_rebuild_run_id, 'serving_v2.search_document', q.row_count,
       md5(format('%s|%s|%s|%s', q.row_count, q.hash_xor, q.hash_sum, q.hash_minmax)),
       jsonb_build_object('columns', 'entity_type,entity_id,label,publication_state,search_text')
FROM (
  SELECT count(*)::bigint AS row_count,
         coalesce(bit_xor(hashtextextended(concat_ws('|', entity_type, entity_id, label, publication_state, search_text), 0)), 0) AS hash_xor,
         coalesce(sum(hashtextextended(concat_ws('|', entity_type, entity_id, label, publication_state, search_text), 0)), 0) AS hash_sum,
         concat_ws(':', min(hashtextextended(concat_ws('|', entity_type, entity_id), 0)), max(hashtextextended(concat_ws('|', entity_type, entity_id), 0))) AS hash_minmax
  FROM serving_v2.search_document
) q
ON CONFLICT (rebuild_run_id, object_name) DO UPDATE
SET row_count = EXCLUDED.row_count, fingerprint = EXCLUDED.fingerprint, metadata = EXCLUDED.metadata;

INSERT INTO control.rebuild_fingerprint (rebuild_run_id, object_name, row_count, fingerprint, metadata)
SELECT :rebuild_rebuild_run_id, 'mart_v2.fact_match_elo_team_stats', q.row_count,
       md5(format('%s|%s|%s|%s', q.row_count, q.hash_xor, q.hash_sum, q.hash_minmax)),
       jsonb_build_object('columns', 'match_id,team_id,side,elo_rating,form3,form5,stats,source_record_key')
FROM (
  SELECT count(*)::bigint AS row_count,
         coalesce(bit_xor(hashtextextended(concat_ws('|', match_id, team_id, side, elo_rating, form3, form5, shots, shots_on_target, fouls, corners, yellow_cards, red_cards, half_time_goals, full_time_goals, ft_result, ht_result, source_record_key), 0)), 0) AS hash_xor,
         coalesce(sum(hashtextextended(concat_ws('|', match_id, team_id, side, elo_rating, form3, form5, shots, shots_on_target, fouls, corners, yellow_cards, red_cards, half_time_goals, full_time_goals, ft_result, ht_result, source_record_key), 0)), 0) AS hash_sum,
         concat_ws(':', min(hashtextextended(concat_ws('|', match_id, source_record_key), 0)), max(hashtextextended(concat_ws('|', match_id, source_record_key), 0))) AS hash_minmax
  FROM mart_v2.fact_match_elo_team_stats
) q
ON CONFLICT (rebuild_run_id, object_name) DO UPDATE
SET row_count = EXCLUDED.row_count, fingerprint = EXCLUDED.fingerprint, metadata = EXCLUDED.metadata;

INSERT INTO control.rebuild_fingerprint (rebuild_run_id, object_name, row_count, fingerprint, metadata)
SELECT :rebuild_rebuild_run_id, 'mart_v2.fact_match_tie', q.row_count,
       md5(format('%s|%s|%s|%s', q.row_count, q.hash_xor, q.hash_sum, q.hash_minmax)),
       jsonb_build_object('columns', 'tie_key,match_id,leg_number,source_system,source_record_key')
FROM (
  SELECT count(*)::bigint AS row_count,
         coalesce(bit_xor(hashtextextended(concat_ws('|', tie_key, match_id, leg_number, source_system, source_record_key), 0)), 0) AS hash_xor,
         coalesce(sum(hashtextextended(concat_ws('|', tie_key, match_id, leg_number, source_system, source_record_key), 0)), 0) AS hash_sum,
         concat_ws(':', min(hashtextextended(concat_ws('|', tie_key, match_id), 0)), max(hashtextextended(concat_ws('|', tie_key, match_id), 0))) AS hash_minmax
  FROM mart_v2.fact_match_tie
) q
ON CONFLICT (rebuild_run_id, object_name) DO UPDATE
SET row_count = EXCLUDED.row_count, fingerprint = EXCLUDED.fingerprint, metadata = EXCLUDED.metadata;

DO $$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT n.nspname, c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'mart_v2'
      AND c.relkind = 'r'
  LOOP
    EXECUTE format(
      'ALTER TABLE %I.%I SET (autovacuum_enabled = true)',
      r.nspname, r.relname
    );
    EXECUTE format('ANALYZE %I.%I', r.nspname, r.relname);
  END LOOP;
END $$;

ALTER TABLE control.match_reconciliation SET (autovacuum_enabled = true);
ALTER TABLE control.publication_decision SET (autovacuum_enabled = true);
ANALYZE control.match_reconciliation;
ANALYZE control.publication_decision;

DO $$
DECLARE
  pending_count bigint;
  broken_count bigint;
  duplicate_count bigint;
  decision_count bigint;
  tie_reference_count bigint;
  tie_link_count bigint;
  wc_count bigint;
BEGIN
  SELECT count(*) INTO pending_count
  FROM mart_v2.match_source
  WHERE reconciliation_state = 'pending';
  IF pending_count > 0 THEN
    RAISE EXCEPTION 'validation failed: % match_source rows remain pending', pending_count;
  END IF;

  SELECT count(*) INTO broken_count
  FROM mart_v2.fact_match f
  LEFT JOIN mart_v2.dim_competition c ON c.competition_key = f.competition_key
  LEFT JOIN mart_v2.dim_edition e ON e.edition_key = f.edition_key
  WHERE f.publication_state = 'published'
    AND (c.competition_key IS NULL OR e.edition_key IS NULL OR f.home_team_id IS NULL OR f.away_team_id IS NULL);
  IF broken_count > 0 THEN
    RAISE EXCEPTION 'validation failed: % published matches violate canonical references', broken_count;
  END IF;

  SELECT count(*) INTO decision_count
  FROM mart_v2.fact_match f
  LEFT JOIN control.publication_decision d
    ON d.entity_type = 'match' AND d.entity_key = f.match_id::text
  WHERE f.publication_state = 'published' AND d.entity_key IS NULL;
  IF decision_count > 0 THEN
    RAISE EXCEPTION 'validation failed: % published matches lack publication decisions', decision_count;
  END IF;

  SELECT count(*) INTO duplicate_count
  FROM (
    SELECT f.edition_key, least(f.home_team_id, f.away_team_id), greatest(f.home_team_id, f.away_team_id), f.match_date,
           CASE WHEN f.home_team_id < f.away_team_id THEN f.home_goals ELSE f.away_goals END,
           CASE WHEN f.home_team_id < f.away_team_id THEN f.away_goals ELSE f.home_goals END
    FROM mart_v2.fact_match f
    WHERE f.publication_state = 'published'
    GROUP BY f.edition_key, least(f.home_team_id, f.away_team_id), greatest(f.home_team_id, f.away_team_id), f.match_date,
             CASE WHEN f.home_team_id < f.away_team_id THEN f.home_goals ELSE f.away_goals END,
             CASE WHEN f.home_team_id < f.away_team_id THEN f.away_goals ELSE f.home_goals END
    HAVING count(*) > 1
  ) duplicates;
  IF duplicate_count > 0 THEN
    RAISE EXCEPTION 'validation failed: % exact semantic duplicate groups remain', duplicate_count;
  END IF;

  SELECT count(*) INTO tie_reference_count
  FROM raw_reference.fact_matches
  WHERE NULLIF(trim(tie_id), '') IS NOT NULL;
  SELECT count(*) INTO tie_link_count FROM mart_v2.fact_match_tie;
  IF tie_link_count <> tie_reference_count THEN
    RAISE EXCEPTION 'validation failed: match tie coverage is % of %', tie_link_count, tie_reference_count;
  END IF;

  SELECT count(*) INTO wc_count FROM mart_v2.fact_match WHERE publication_state = 'published' AND edition_key = 'fifa_world_cup_mens:2006';
  IF wc_count <> 64 THEN RAISE EXCEPTION 'validation failed: World Cup 2006 has % published matches', wc_count; END IF;
  SELECT count(*) INTO wc_count FROM mart_v2.fact_match WHERE publication_state = 'published' AND edition_key = 'fifa_world_cup_mens:2014';
  IF wc_count <> 64 THEN RAISE EXCEPTION 'validation failed: World Cup 2014 has % published matches', wc_count; END IF;

END $$;

UPDATE control.rebuild_run
SET phase = 'validation', status = 'succeeded', finished_at = now(),
    metadata = metadata || jsonb_build_object('validation_status', 'green')
WHERE rebuild_run_id = :rebuild_rebuild_run_id;
