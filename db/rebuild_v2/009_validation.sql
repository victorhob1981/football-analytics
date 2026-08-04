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

DELETE FROM control.coverage_delta_reason
WHERE rebuild_run_id = :rebuild_rebuild_run_id;

DROP TABLE IF EXISTS _match_coverage_reconciliation;
CREATE TEMP TABLE _match_coverage_reconciliation AS
WITH edition_map AS (
  SELECT competition_key, season_label, min(edition_key) AS edition_key
  FROM mart_v2.dim_edition
  GROUP BY competition_key, season_label
),
reference_scope AS (
  SELECT
    r.provider AS source_system,
    coalesce(r.competition_key, '') AS competition_key,
    coalesce(
      e.edition_key,
      concat_ws(':', coalesce(r.competition_key, ''), coalesce(r.season_label, r.season::text, ''))
    ) AS edition_key,
    count(*)::bigint AS reference_rows
  FROM raw_reference.fact_matches r
  LEFT JOIN edition_map e
    ON e.competition_key = r.competition_key
   AND e.season_label = coalesce(r.season_label, r.season::text)
  GROUP BY r.provider, coalesce(r.competition_key, ''),
    coalesce(
      e.edition_key,
      concat_ws(':', coalesce(r.competition_key, ''), coalesce(r.season_label, r.season::text, ''))
    )
),
approved_scope AS (
  SELECT
    s.source_system,
    coalesce(f.competition_key, '') AS competition_key,
    coalesce(f.edition_key, '') AS edition_key,
    count(*)::bigint AS approved_source_rows,
    count(*) FILTER (
      WHERE f.publication_state = 'published'
        AND f.source_system <> s.source_system
    )::bigint AS reattributed_rows,
    count(*) FILTER (WHERE f.publication_state <> 'published')::bigint AS nonpublished_rows,
    count(*) FILTER (
      WHERE f.publication_state = 'published'
        AND f.source_system = s.source_system
    )::bigint AS owned_published_source_rows
  FROM mart_v2.match_source s
  JOIN mart_v2.fact_match f ON f.match_id = s.canonical_match_id
  WHERE s.reconciliation_state = 'approved'
  GROUP BY s.source_system, coalesce(f.competition_key, ''), coalesce(f.edition_key, '')
),
published_scope AS (
  SELECT
    source_system,
    coalesce(competition_key, '') AS competition_key,
    coalesce(edition_key, '') AS edition_key,
    count(*)::bigint AS published_rows
  FROM mart_v2.fact_match
  WHERE publication_state = 'published'
  GROUP BY source_system, coalesce(competition_key, ''), coalesce(edition_key, '')
),
quarantined_scope AS (
  SELECT
    source_system,
    coalesce(competition_key, '') AS competition_key,
    coalesce(edition_key, '') AS edition_key,
    count(*)::bigint AS quarantined_rows
  FROM mart_v2.fact_match
  WHERE publication_state = 'quarantined'
  GROUP BY source_system, coalesce(competition_key, ''), coalesce(edition_key, '')
),
pending_scope AS (
  SELECT
    s.source_system,
    coalesce(f.competition_key, '') AS competition_key,
    coalesce(f.edition_key, '') AS edition_key,
    count(*)::bigint AS pending_rows
  FROM mart_v2.match_source s
  LEFT JOIN mart_v2.fact_match f ON f.match_id = s.canonical_match_id
  WHERE s.reconciliation_state = 'pending'
  GROUP BY s.source_system, coalesce(f.competition_key, ''), coalesce(f.edition_key, '')
),
scope_keys AS (
  SELECT source_system, competition_key, edition_key FROM reference_scope
  UNION
  SELECT source_system, competition_key, edition_key FROM approved_scope
  UNION
  SELECT source_system, competition_key, edition_key FROM published_scope
  UNION
  SELECT source_system, competition_key, edition_key FROM quarantined_scope
  UNION
  SELECT source_system, competition_key, edition_key FROM pending_scope
),
scope_counts AS (
  SELECT
    k.source_system,
    k.competition_key,
    k.edition_key,
    coalesce(r.reference_rows, 0)::bigint AS reference_rows,
    coalesce(a.approved_source_rows, 0)::bigint AS approved_source_rows,
    coalesce(p.published_rows, 0)::bigint AS published_rows,
    coalesce(q.quarantined_rows, 0)::bigint AS quarantined_rows,
    coalesce(n.pending_rows, 0)::bigint AS pending_rows,
    coalesce(a.reattributed_rows, 0)::bigint AS reattributed_rows,
    coalesce(a.nonpublished_rows, 0)::bigint AS nonpublished_rows,
    coalesce(a.owned_published_source_rows, 0)::bigint AS owned_published_source_rows
  FROM scope_keys k
  LEFT JOIN reference_scope r USING (source_system, competition_key, edition_key)
  LEFT JOIN approved_scope a USING (source_system, competition_key, edition_key)
  LEFT JOIN published_scope p USING (source_system, competition_key, edition_key)
  LEFT JOIN quarantined_scope q USING (source_system, competition_key, edition_key)
  LEFT JOIN pending_scope n USING (source_system, competition_key, edition_key)
)
SELECT
  s.*,
  (s.owned_published_source_rows - s.published_rows)::bigint AS same_source_duplicate_rows,
  (s.published_rows - s.reference_rows)::bigint AS published_delta,
  (
    (s.approved_source_rows - s.reference_rows)
    - s.reattributed_rows
    - s.nonpublished_rows
    - (s.owned_published_source_rows - s.published_rows)
  )::bigint AS classified_delta
FROM scope_counts s;

INSERT INTO control.coverage_delta_reason (
  rebuild_run_id, scope_name, source_system, competition_key, edition_key,
  publication_state, reason_code, direction, reference_rows, candidate_rows,
  delta_rows, evidence
)
SELECT
  :rebuild_rebuild_run_id,
  'provider_competition_edition',
  source_system,
  competition_key,
  edition_key,
  'published',
  'approved_source_scope_change',
  CASE
    WHEN approved_source_rows > reference_rows THEN 'increase'
    WHEN approved_source_rows < reference_rows THEN 'decrease'
    ELSE 'neutral'
  END,
  reference_rows,
  approved_source_rows,
  approved_source_rows - reference_rows,
  jsonb_build_object(
    'reference_table', 'shadow_dbt_20260716.fact_matches',
    'candidate_table', 'mart_v2.match_source',
    'candidate_filter', 'reconciliation_state=approved',
    'meaning', 'approved source coverage after canonical competition and edition assignment'
  )
FROM _match_coverage_reconciliation;

INSERT INTO control.coverage_delta_reason (
  rebuild_run_id, scope_name, source_system, competition_key, edition_key,
  publication_state, reason_code, direction, candidate_rows, delta_rows, evidence
)
SELECT
  :rebuild_rebuild_run_id,
  'provider_competition_edition',
  source_system,
  competition_key,
  edition_key,
  'published',
  'cross_source_canonical_dedup',
  'decrease',
  reattributed_rows,
  -reattributed_rows,
  jsonb_build_object(
    'rule', 'approved source row points to a published canonical match owned by another source',
    'candidate_table', 'mart_v2.match_source'
  )
FROM _match_coverage_reconciliation
WHERE reattributed_rows > 0;

INSERT INTO control.coverage_delta_reason (
  rebuild_run_id, scope_name, source_system, competition_key, edition_key,
  publication_state, reason_code, direction, candidate_rows, delta_rows, evidence
)
SELECT
  :rebuild_rebuild_run_id,
  'provider_competition_edition',
  source_system,
  competition_key,
  edition_key,
  'published',
  'same_source_canonical_dedup',
  CASE WHEN same_source_duplicate_rows > 0 THEN 'decrease' ELSE 'increase' END,
  abs(same_source_duplicate_rows),
  -same_source_duplicate_rows,
  jsonb_build_object(
    'rule', 'multiple approved source rows resolve to one canonical match owned by the same source',
    'candidate_table', 'mart_v2.match_source'
  )
FROM _match_coverage_reconciliation
WHERE same_source_duplicate_rows <> 0;

INSERT INTO control.coverage_delta_reason (
  rebuild_run_id, scope_name, source_system, competition_key, edition_key,
  publication_state, reason_code, direction, candidate_rows, delta_rows, evidence
)
SELECT
  :rebuild_rebuild_run_id,
  'provider_competition_edition',
  source_system,
  competition_key,
  edition_key,
  'published',
  'approved_source_nonpublished',
  'decrease',
  nonpublished_rows,
  -nonpublished_rows,
  jsonb_build_object(
    'rule', 'approved source row is attached to a canonical match that is not published',
    'candidate_table', 'mart_v2.match_source'
  )
FROM _match_coverage_reconciliation
WHERE nonpublished_rows > 0;

INSERT INTO control.coverage_delta_reason (
  rebuild_run_id, scope_name, source_system, competition_key, edition_key,
  publication_state, reason_code, direction, candidate_rows, delta_rows, evidence
)
SELECT
  :rebuild_rebuild_run_id,
  'provider_competition_edition',
  source_system,
  coalesce(competition_key, ''),
  coalesce(edition_key, ''),
  'quarantined',
  CASE
    WHEN publication_reason LIKE 'duplicate_of:%' THEN 'duplicate_semantic_candidate'
    ELSE coalesce(NULLIF(publication_reason, ''), 'quarantine_reason_missing')
  END,
  'increase',
  count(*)::bigint,
  count(*)::bigint,
  jsonb_build_object(
    'candidate_table', 'mart_v2.fact_match',
    'candidate_filter', 'publication_state=quarantined',
    'raw_reason', jsonb_agg(publication_reason)
  )
FROM mart_v2.fact_match
WHERE publication_state = 'quarantined'
GROUP BY source_system, coalesce(competition_key, ''), coalesce(edition_key, ''),
  CASE
    WHEN publication_reason LIKE 'duplicate_of:%' THEN 'duplicate_semantic_candidate'
    ELSE coalesce(NULLIF(publication_reason, ''), 'quarantine_reason_missing')
  END;

INSERT INTO control.coverage_reconciliation (
  rebuild_run_id, scope_name, source_system, competition_key, edition_key,
  reference_rows, observed_rows, published_rows, quarantined_rows, pending_rows,
  disposition, evidence
)
SELECT
  :rebuild_rebuild_run_id,
  'provider_competition_edition',
  source_system,
  competition_key,
  edition_key,
  reference_rows,
  published_rows + quarantined_rows,
  published_rows,
  quarantined_rows,
  pending_rows,
  CASE
    WHEN pending_rows > 0 OR classified_delta <> published_delta THEN 'pending'
    WHEN published_delta <> 0 THEN 'explained'
    WHEN quarantined_rows > 0 THEN 'quarantined'
    ELSE 'complete'
  END,
  jsonb_build_object(
    'approved_source_rows', approved_source_rows,
    'reattributed_rows', reattributed_rows,
    'same_source_duplicate_rows', same_source_duplicate_rows,
    'approved_source_nonpublished_rows', nonpublished_rows,
    'published_delta', published_delta,
    'classified_delta', classified_delta
  )
FROM _match_coverage_reconciliation;

INSERT INTO control.coverage_reconciliation (
  rebuild_run_id, scope_name, source_system, reference_rows, observed_rows,
  published_rows, quarantined_rows, pending_rows, disposition, evidence
)
SELECT
  :rebuild_rebuild_run_id,
  'legacy_shadow_match_reference',
  'all_sources',
  sum(reference_rows),
  (SELECT count(*) FROM mart_v2.fact_match),
  sum(published_rows),
  sum(quarantined_rows),
  sum(pending_rows),
  CASE
    WHEN sum(pending_rows) > 0 OR sum(classified_delta) <> sum(published_delta) THEN 'pending'
    WHEN sum(published_delta) <> 0 THEN 'explained'
    WHEN sum(quarantined_rows) > 0 THEN 'quarantined'
    ELSE 'complete'
  END,
  jsonb_build_object(
    'comparison_rule', 'reference is legacy shadow scope; v2 is multi-source canonical scope',
    'reference_table', 'shadow_dbt_20260716.fact_matches',
    'published_delta', sum(published_delta),
    'classified_delta', sum(classified_delta),
    'reason_matrix', 'control.coverage_delta_reason'
  )
FROM _match_coverage_reconciliation;

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
  unexplained_count bigint;
  invalid_reason_count bigint;
BEGIN
  SELECT count(*) INTO pending_count
  FROM mart_v2.match_source
  WHERE reconciliation_state = 'pending';
  IF pending_count > 0 THEN
    RAISE EXCEPTION 'validation failed: % match_source rows remain pending', pending_count;
  END IF;

  SELECT count(*) INTO unexplained_count
  FROM _match_coverage_reconciliation
  WHERE published_delta <> classified_delta;
  IF unexplained_count > 0 THEN
    RAISE EXCEPTION 'validation failed: % unexplained published match delta scopes remain', unexplained_count;
  END IF;

  SELECT count(*) INTO invalid_reason_count
  FROM _match_coverage_reconciliation
  WHERE same_source_duplicate_rows < 0;
  IF invalid_reason_count > 0 THEN
    RAISE EXCEPTION 'validation failed: % published scopes lack approved source lineage', invalid_reason_count;
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
