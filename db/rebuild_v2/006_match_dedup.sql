\set ON_ERROR_STOP on
\if :{?rebuild_run_key}
\else
  \set rebuild_run_key 'mart-v2-local-current'
\endif

SELECT rebuild_run_id
FROM control.rebuild_run
WHERE run_key = :'rebuild_run_key'\gset rebuild_

-- Elo is a complete provider source, not a pending side table. Approved team
-- crosswalks supply the identity evidence; exact date/team/score matching links
-- existing matches and the remaining resolved rows become stable new coverage.
DROP TABLE IF EXISTS tmp_elo_prepared;
CREATE TEMP TABLE tmp_elo_prepared ON COMMIT PRESERVE ROWS AS
WITH team_map AS (
  SELECT source_team_key, canonical_id::bigint AS canonical_id
  FROM raw_src.provider_entity_map
  WHERE provider = 'eloratings'
    AND entity_type = 'team'
    AND mapping_state = 'approved'
    AND canonical_id ~ '^[0-9]+$'
), mapped AS (
  SELECT
    e.record_hash,
    NULLIF(trim(e.match_date_raw), '')::date AS match_date,
    cpm.competition_key,
    cpm.competition_key || ':' || extract(year FROM NULLIF(trim(e.match_date_raw), '')::date)::integer::text AS edition_key,
    e.division,
    e.home_team_name,
    e.away_team_name,
    CASE WHEN NULLIF(trim(e.ft_home_raw), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN NULLIF(trim(e.ft_home_raw), '')::numeric::integer END AS home_goals,
    CASE WHEN NULLIF(trim(e.ft_away_raw), '') ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN NULLIF(trim(e.ft_away_raw), '')::numeric::integer END AS away_goals,
    e.ht_home_raw,
    e.ht_away_raw,
    e.home_elo_raw,
    e.away_elo_raw,
    e.form3_home_raw,
    e.form5_home_raw,
    e.form3_away_raw,
    e.form5_away_raw,
    e.home_shots_raw,
    e.away_shots_raw,
    e.home_target_raw,
    e.away_target_raw,
    e.home_fouls_raw,
    e.away_fouls_raw,
    e.home_corners_raw,
    e.away_corners_raw,
    e.home_yellow_raw,
    e.away_yellow_raw,
    e.home_red_raw,
    e.away_red_raw,
    e.odd_home_raw,
    e.odd_draw_raw,
    e.odd_away_raw,
    e.max_home_raw,
    e.max_draw_raw,
    e.max_away_raw,
    e.over25_raw,
    e.under25_raw,
    e.max_over25_raw,
    e.max_under25_raw,
    e.handi_size_raw,
    e.handi_home_raw,
    e.handi_away_raw,
    e.ft_result,
    e.ht_result,
    e.source_file,
    e.ingested_at,
    home_map.canonical_id AS home_team_id,
    away_map.canonical_id AS away_team_id,
    700000000000000000 + mod(abs(hashtextextended('eloratings:' || e.record_hash, 0)), 100000000000000000) AS generated_match_id
  FROM raw_src.elo_matches e
  JOIN control.competition_provider_map cpm
    ON cpm.provider = 'eloratings'
   AND cpm.provider_league_code = e.division
  LEFT JOIN team_map home_map
    ON home_map.source_team_key = 'eloratings:' || cpm.competition_key || ':' || e.home_team_name
  LEFT JOIN team_map away_map
    ON away_map.source_team_key = 'eloratings:' || cpm.competition_key || ':' || e.away_team_name
)
SELECT * FROM mapped;

DO $$
BEGIN
  IF EXISTS (
    SELECT generated_match_id
    FROM tmp_elo_prepared
    GROUP BY generated_match_id
    HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION 'stable Elo match id collision';
  END IF;
END $$;

-- Elo covers historical editions that are absent from the operational season
-- catalog. Materialize those approved provider contexts before the fact FK is
-- exercised; the source rows remain in raw and only their canonical context is
-- added here.
INSERT INTO mart_v2.dim_competition (
  competition_key, competition_name, competition_type, country_name,
  confederation_name, tier, is_active, source_count, metadata, rebuild_run_id
)
SELECT
  c.competition_key,
  c.competition_name,
  c.competition_type,
  c.country_name,
  c.confederation_name,
  c.tier,
  c.is_active,
  count(*)::integer,
  jsonb_build_object('catalog_source', 'raw.elo_matches', 'provider', 'eloratings'),
  :rebuild_rebuild_run_id
FROM control.competitions c
JOIN tmp_elo_prepared e
  ON e.competition_key = c.competition_key
GROUP BY c.competition_key, c.competition_name, c.competition_type,
         c.country_name, c.confederation_name, c.tier, c.is_active
ON CONFLICT (competition_key) DO UPDATE
SET metadata = mart_v2.dim_competition.metadata || EXCLUDED.metadata,
    source_count = greatest(mart_v2.dim_competition.source_count, EXCLUDED.source_count),
    rebuild_run_id = EXCLUDED.rebuild_run_id;

INSERT INTO mart_v2.dim_edition (
  edition_key, competition_key, season_label, season_start_date,
  season_end_date, is_closed, observed_source_count, first_match_date,
  last_match_date, metadata, rebuild_run_id
)
SELECT
  e.edition_key,
  e.competition_key,
  split_part(e.edition_key, ':', 2),
  min(e.match_date),
  max(e.match_date),
  max(e.match_date) < current_date,
  count(*)::bigint,
  min(e.match_date),
  max(e.match_date),
  jsonb_build_object('catalog_source', 'raw.elo_matches', 'provider', 'eloratings'),
  :rebuild_rebuild_run_id
FROM tmp_elo_prepared e
WHERE e.edition_key IS NOT NULL
GROUP BY e.edition_key, e.competition_key
ON CONFLICT (edition_key) DO UPDATE
SET season_start_date = coalesce(mart_v2.dim_edition.season_start_date, EXCLUDED.season_start_date),
    season_end_date = greatest(mart_v2.dim_edition.season_end_date, EXCLUDED.season_end_date),
    is_closed = coalesce(EXCLUDED.is_closed, mart_v2.dim_edition.is_closed),
    observed_source_count = greatest(mart_v2.dim_edition.observed_source_count, EXCLUDED.observed_source_count),
    first_match_date = least(mart_v2.dim_edition.first_match_date, EXCLUDED.first_match_date),
    last_match_date = greatest(mart_v2.dim_edition.last_match_date, EXCLUDED.last_match_date),
    metadata = mart_v2.dim_edition.metadata || EXCLUDED.metadata,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

DROP TABLE IF EXISTS tmp_elo_resolved;
CREATE TEMP TABLE tmp_elo_resolved ON COMMIT PRESERVE ROWS AS
SELECT
  e.*,
  ms.canonical_match_id AS source_canonical_match_id,
  candidate.match_id AS candidate_match_id,
  coalesce(ms.canonical_match_id, candidate.match_id) AS resolved_match_id,
  CASE
    WHEN ms.canonical_match_id IS NOT NULL THEN 'approved_existing_match_source'
    WHEN candidate.match_id IS NOT NULL THEN 'exact_or_near_date_team_score_match'
    WHEN e.home_team_id IS NOT NULL AND e.away_team_id IS NOT NULL THEN 'approved_team_crosswalk_new_coverage'
    ELSE 'unresolved_team_identity'
  END AS resolution_method
FROM tmp_elo_prepared e
LEFT JOIN mart_v2.match_source ms
  ON ms.source_system = 'eloratings'
 AND ms.source_match_id = e.record_hash
 AND ms.reconciliation_state = 'approved'
LEFT JOIN LATERAL (
  SELECT f.match_id
  FROM mart_v2.fact_match f
  WHERE f.publication_state = 'published'
    AND f.competition_key = e.competition_key
    AND f.home_team_id = e.home_team_id
    AND f.away_team_id = e.away_team_id
    AND f.match_date BETWEEN e.match_date - 1 AND e.match_date + 1
    AND (e.home_goals IS NULL OR (f.home_goals = e.home_goals AND f.away_goals = e.away_goals))
  ORDER BY abs(f.match_date - e.match_date), f.match_id
  LIMIT 1
) candidate ON true;

INSERT INTO mart_v2.fact_match (
  match_id, match_key, source_system, source_match_id, competition_key,
  edition_key, match_date, date_utc, status_short, status_long,
  home_team_id, away_team_id, home_team_name_raw, away_team_name_raw,
  home_goals, away_goals, publication_state, publication_reason,
  source_run_id, ingested_at, metadata, rebuild_run_id
)
SELECT
  e.generated_match_id,
  'eloratings:' || e.record_hash,
  'eloratings',
  e.record_hash,
  e.competition_key,
  e.edition_key,
  e.match_date,
  e.match_date::timestamp AT TIME ZONE 'UTC',
  'FT',
  'Finished',
  e.home_team_id,
  e.away_team_id,
  e.home_team_name,
  e.away_team_name,
  e.home_goals,
  e.away_goals,
  CASE WHEN e.match_date IS NOT NULL AND e.home_team_id IS NOT NULL AND e.away_team_id IS NOT NULL
       THEN 'published' ELSE 'quarantined' END,
  CASE WHEN e.home_team_id IS NULL OR e.away_team_id IS NULL THEN 'unresolved_team_identity'
       WHEN e.match_date IS NULL THEN 'missing_match_date'
       ELSE NULL END,
  'eloratings',
  e.ingested_at,
  jsonb_build_object(
    'division', e.division,
    'source_file', e.source_file,
    'resolution_method', e.resolution_method,
    'source_record_key', e.record_hash,
    'identity_evidence', 'approved_provider_entity_map'
  ),
  :rebuild_rebuild_run_id
FROM tmp_elo_resolved e
WHERE e.resolved_match_id IS NULL
ON CONFLICT (match_id) DO NOTHING;

UPDATE mart_v2.match_source s
SET canonical_match_id = coalesce(e.resolved_match_id, e.generated_match_id),
    source_table = 'raw.elo_matches',
    source_date = e.match_date,
    source_home_team_id = e.home_team_id::text,
    source_away_team_id = e.away_team_id::text,
    reconciliation_state = CASE
      WHEN e.match_date IS NOT NULL AND e.home_team_id IS NOT NULL AND e.away_team_id IS NOT NULL THEN 'approved'
      ELSE 'quarantined'
    END,
    method = e.resolution_method,
    confidence = CASE
      WHEN e.home_team_id IS NOT NULL AND e.away_team_id IS NOT NULL AND e.resolved_match_id IS NOT NULL THEN 1.0
      WHEN e.home_team_id IS NOT NULL AND e.away_team_id IS NOT NULL THEN 0.95
      ELSE 0.0
    END,
    evidence = jsonb_build_object(
      'competition_key', e.competition_key,
      'edition_key', e.edition_key,
      'home_team_name', e.home_team_name,
      'away_team_name', e.away_team_name,
      'home_team_crosswalk', e.home_team_id,
      'away_team_crosswalk', e.away_team_id,
      'source_record_key', e.record_hash,
      'resolution_method', e.resolution_method
    ),
    rebuild_run_id = :rebuild_rebuild_run_id
FROM tmp_elo_resolved e
WHERE s.source_system = 'eloratings'
  AND s.source_match_id = e.record_hash;

INSERT INTO control.quality_issue (
  rebuild_run_id, entity_type, entity_key, rule_code, severity, disposition, evidence
)
SELECT
  :rebuild_rebuild_run_id,
  'match',
  e.record_hash,
  'elo_team_identity_unresolved',
  'warning',
  'quarantined',
  jsonb_build_object(
    'competition_key', e.competition_key,
    'home_team_name', e.home_team_name,
    'away_team_name', e.away_team_name,
    'home_team_id', e.home_team_id,
    'away_team_id', e.away_team_id
  )
FROM tmp_elo_resolved e
WHERE e.home_team_id IS NULL OR e.away_team_id IS NULL
ON CONFLICT (rebuild_run_id, entity_type, entity_key, rule_code) DO UPDATE
SET evidence = EXCLUDED.evidence,
    disposition = EXCLUDED.disposition;

-- Cross-provider semantic deduplication is order-insensitive and tolerates a
-- one-day provider date drift. Scores are descriptive rather than identity:
-- providers can disagree on whether shootout goals belong in the final score.
DROP TABLE IF EXISTS tmp_match_duplicate_map;
CREATE TEMP TABLE tmp_match_duplicate_map ON COMMIT PRESERVE ROWS AS
WITH eligible AS (
  SELECT
    f.*,
    CASE
      WHEN f.source_system = 'sportmonks' THEN 10
      WHEN f.source_system LIKE 'world_cup_%' OR f.source_system = 'fjelstul_worldcup' THEN 15
      WHEN f.source_system = 'dataset_brasileirao' THEN 20
      WHEN f.source_system = 'statsbomb_open_data' THEN 30
      WHEN f.source_system = 'transfermarkt' THEN 40
      WHEN f.source_system = 'eloratings' THEN 50
      ELSE 90
    END AS source_priority
  FROM mart_v2.fact_match f
  WHERE f.publication_state = 'published'
    AND f.edition_key IS NOT NULL
    AND f.match_date IS NOT NULL
    AND f.home_team_id IS NOT NULL
    AND f.away_team_id IS NOT NULL
), pairs AS (
  SELECT DISTINCT ON (loser.match_id)
    loser.match_id AS duplicate_match_id,
    winner.match_id AS canonical_match_id,
    loser.edition_key,
    loser.match_date,
    loser.home_team_id,
    loser.away_team_id,
    loser.source_system AS duplicate_source_system,
    winner.source_system AS canonical_source_system,
    abs(loser.match_date - winner.match_date) AS day_delta,
    NOT (
      CASE WHEN winner.home_team_id < winner.away_team_id THEN winner.home_goals ELSE winner.away_goals END
        IS NOT DISTINCT FROM
      CASE WHEN loser.home_team_id < loser.away_team_id THEN loser.home_goals ELSE loser.away_goals END
      AND
      CASE WHEN winner.home_team_id < winner.away_team_id THEN winner.away_goals ELSE winner.home_goals END
        IS NOT DISTINCT FROM
      CASE WHEN loser.home_team_id < loser.away_team_id THEN loser.away_goals ELSE loser.home_goals END
    ) AS score_conflict
  FROM eligible loser
  JOIN eligible winner
    ON winner.match_id <> loser.match_id
   AND winner.edition_key = loser.edition_key
   AND winner.source_system <> loser.source_system
   AND abs(winner.match_date - loser.match_date) <= 1
   AND least(winner.home_team_id, winner.away_team_id) = least(loser.home_team_id, loser.away_team_id)
   AND greatest(winner.home_team_id, winner.away_team_id) = greatest(loser.home_team_id, loser.away_team_id)
  WHERE (winner.source_priority, winner.match_id) < (loser.source_priority, loser.match_id)
  ORDER BY loser.match_id, winner.source_priority, abs(loser.match_date - winner.match_date), winner.match_id
)
SELECT * FROM pairs;

UPDATE mart_v2.fact_match f
SET publication_state = 'quarantined',
    publication_reason = 'duplicate_of:' || d.canonical_match_id::text,
    metadata = f.metadata || jsonb_build_object(
      'canonical_match_id', d.canonical_match_id,
      'dedup_rule', 'cross_provider_unordered_teams_date_window',
      'score_conflict', d.score_conflict
    ),
    rebuild_run_id = :rebuild_rebuild_run_id
FROM tmp_match_duplicate_map d
WHERE f.match_id = d.duplicate_match_id;

UPDATE mart_v2.match_source s
SET canonical_match_id = d.canonical_match_id,
    reconciliation_state = 'approved',
    method = coalesce(s.method, 'duplicate_semantic_reconciliation') || ':duplicate_semantic_reconciliation',
    evidence = s.evidence || jsonb_build_object(
      'duplicate_match_id', d.duplicate_match_id,
      'canonical_match_id', d.canonical_match_id,
      'day_delta', d.day_delta,
      'score_conflict', d.score_conflict,
      'canonical_source_system', d.canonical_source_system
    ),
    rebuild_run_id = :rebuild_rebuild_run_id
FROM tmp_match_duplicate_map d
WHERE s.canonical_match_id = d.duplicate_match_id;

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
    'day_delta', d.day_delta,
    'score_conflict', d.score_conflict,
    'duplicate_source_system', d.duplicate_source_system,
    'canonical_source_system', d.canonical_source_system
  )
FROM tmp_match_duplicate_map d
ON CONFLICT (rebuild_run_id, entity_type, entity_key, rule_code) DO UPDATE
SET evidence = EXCLUDED.evidence;

INSERT INTO control.source_system (source_system, source_kind, priority, description)
SELECT DISTINCT source_system, 'derived', 100, 'Source observed during v2 match reconciliation'
FROM mart_v2.match_source
ON CONFLICT (source_system) DO NOTHING;

INSERT INTO control.match_reconciliation (
  source_system, source_match_id, canonical_match_id, competition_key, edition_key,
  reconciliation_state, method, confidence, source_date, home_source_team_id,
  away_source_team_id, evidence, rebuild_run_id
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
  s.source_home_team_id,
  s.source_away_team_id,
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
    home_source_team_id = EXCLUDED.home_source_team_id,
    away_source_team_id = EXCLUDED.away_source_team_id,
    evidence = EXCLUDED.evidence,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

INSERT INTO mart_v2.dim_tie (
  tie_key, edition_key, stage_key, tie_order, home_team_id, away_team_id,
  winner_team_id, match_count, first_leg_at, last_leg_at, home_side_goals,
  away_side_goals, resolution_type, has_extra_time_match,
  has_penalties_match, next_stage_name, is_inferred, source_system,
  source_record_key, metadata, rebuild_run_id
)
SELECT
  t.competition_key || ':' || t.season_label || ':' || t.tie_id,
  t.competition_key || ':' || t.season_label,
  ds.stage_key,
  t.tie_order,
  ht.canonical_entity_id::bigint,
  at.canonical_entity_id::bigint,
  wt.canonical_entity_id::bigint,
  t.match_count,
  t.first_leg_at,
  t.last_leg_at,
  t.home_side_goals,
  t.away_side_goals,
  t.resolution_type,
  t.has_extra_time_match,
  t.has_penalties_match,
  t.next_stage_name,
  coalesce(t.is_inferred, false),
  'legacy_mart_reference',
  t.tie_id,
  jsonb_build_object('stage_name', t.stage_name, 'stage_format', t.stage_format),
  :rebuild_rebuild_run_id
FROM raw_reference.fact_tie_results t
JOIN mart_v2.dim_edition e
  ON e.edition_key = t.competition_key || ':' || t.season_label
LEFT JOIN mart_v2.dim_stage ds
  ON ds.edition_key = e.edition_key
 AND (ds.stage_id = t.stage_id OR ds.stage_name = t.stage_name)
LEFT JOIN control.entity_source_identity ht
  ON ht.source_system = t.provider
 AND ht.entity_type = 'team'
 AND ht.source_entity_id = t.home_side_team_id::text
 AND ht.mapping_state = 'approved'
LEFT JOIN control.entity_source_identity at
  ON at.source_system = t.provider
 AND at.entity_type = 'team'
 AND at.source_entity_id = t.away_side_team_id::text
 AND at.mapping_state = 'approved'
LEFT JOIN control.entity_source_identity wt
  ON wt.source_system = t.provider
 AND wt.entity_type = 'team'
 AND wt.source_entity_id = t.winner_team_id::text
 AND wt.mapping_state = 'approved'
WHERE NULLIF(trim(t.tie_id), '') IS NOT NULL
ON CONFLICT (tie_key) DO UPDATE
SET stage_key = EXCLUDED.stage_key,
    tie_order = EXCLUDED.tie_order,
    home_team_id = EXCLUDED.home_team_id,
    away_team_id = EXCLUDED.away_team_id,
    winner_team_id = EXCLUDED.winner_team_id,
    match_count = EXCLUDED.match_count,
    first_leg_at = EXCLUDED.first_leg_at,
    last_leg_at = EXCLUDED.last_leg_at,
    home_side_goals = EXCLUDED.home_side_goals,
    away_side_goals = EXCLUDED.away_side_goals,
    resolution_type = EXCLUDED.resolution_type,
    metadata = EXCLUDED.metadata,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

INSERT INTO mart_v2.fact_match_tie (
  tie_key, match_id, leg_number, source_system, source_record_key, rebuild_run_id
)
SELECT
  dt.tie_key,
  f.match_id,
  t.leg_number::integer,
  'legacy_mart_reference',
  t.match_id::text,
  :rebuild_rebuild_run_id
FROM raw_reference.fact_matches t
JOIN mart_v2.fact_match f
  ON f.edition_key = t.competition_key || ':' || t.season_label
 AND f.match_date = t.date_day
 AND least(f.home_team_id, f.away_team_id) = least(t.home_team_id, t.away_team_id)
 AND greatest(f.home_team_id, f.away_team_id) = greatest(t.home_team_id, t.away_team_id)
 AND f.publication_state = 'published'
JOIN mart_v2.dim_tie dt
  ON dt.edition_key = f.edition_key
 AND dt.stage_key IS NOT DISTINCT FROM f.stage_key
 AND least(dt.home_team_id, dt.away_team_id) = least(f.home_team_id, f.away_team_id)
 AND greatest(dt.home_team_id, dt.away_team_id) = greatest(f.home_team_id, f.away_team_id)
 AND f.match_date BETWEEN dt.first_leg_at::date AND dt.last_leg_at::date
WHERE NULLIF(trim(t.tie_id), '') IS NOT NULL
ON CONFLICT DO NOTHING;

WITH tie_progression AS (
  SELECT
    competition_key || ':' || season_label AS edition_key,
    count(*)::integer AS tie_count,
    jsonb_agg(DISTINCT jsonb_build_object(
      'stage_name', coalesce(stage_name, ''),
      'stage_format', coalesce(stage_format, ''),
      'next_stage_name', coalesce(next_stage_name, '')
    )) AS progression
  FROM raw_reference.fact_tie_results
  GROUP BY competition_key || ':' || season_label
), stage_counts AS (
  SELECT edition_key, count(*)::integer AS stage_count
  FROM mart_v2.dim_stage
  GROUP BY edition_key
), group_counts AS (
  SELECT edition_key, count(*)::integer AS group_count
  FROM mart_v2.dim_group
  GROUP BY edition_key
)
INSERT INTO mart_v2.dim_edition_format (
  edition_key, format_code, format_name, stage_count, group_count, tie_count,
  tie_rule_code, progression, is_inferred, source_system, source_record_key,
  rebuild_run_id
)
SELECT
  e.edition_key,
  CASE WHEN coalesce(tp.tie_count, 0) > 0 THEN 'group_or_league_plus_knockout' ELSE 'group_or_league' END,
  CASE WHEN coalesce(tp.tie_count, 0) > 0 THEN 'Observed group/league and knockout progression'
       ELSE 'Observed group or league format without tie records' END,
  coalesce(sc.stage_count, 0),
  coalesce(gc.group_count, 0),
  coalesce(tp.tie_count, 0),
  CASE WHEN coalesce(tp.tie_count, 0) > 0 THEN 'provider_aggregated_tie' END,
  jsonb_build_object('stages', coalesce(tp.progression, '[]'::jsonb)),
  coalesce(tp.tie_count, 0) = 0,
  CASE WHEN coalesce(tp.tie_count, 0) > 0 THEN 'legacy_mart_reference' ELSE 'v2_observed_structure' END,
  'edition:' || e.edition_key,
  :rebuild_rebuild_run_id
FROM mart_v2.dim_edition e
LEFT JOIN tie_progression tp ON tp.edition_key = e.edition_key
LEFT JOIN stage_counts sc ON sc.edition_key = e.edition_key
LEFT JOIN group_counts gc ON gc.edition_key = e.edition_key
ON CONFLICT (edition_key) DO UPDATE
SET format_code = EXCLUDED.format_code,
    format_name = EXCLUDED.format_name,
    stage_count = EXCLUDED.stage_count,
    group_count = EXCLUDED.group_count,
    tie_count = EXCLUDED.tie_count,
    tie_rule_code = EXCLUDED.tie_rule_code,
    progression = EXCLUDED.progression,
    is_inferred = EXCLUDED.is_inferred,
    source_system = EXCLUDED.source_system,
    source_record_key = EXCLUDED.source_record_key,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

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
