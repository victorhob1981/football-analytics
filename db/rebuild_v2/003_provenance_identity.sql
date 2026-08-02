\set ON_ERROR_STOP on
\if :{?rebuild_run_key}
\else
  \set rebuild_run_key 'mart-v2-local-current'
\endif
\if :{?source_snapshot_ref}
\else
  \set source_snapshot_ref 'source:football_dw:raw'
\endif

INSERT INTO control.rebuild_run (run_key, phase, status, source_database, source_snapshot_ref, metadata)
VALUES (:'rebuild_run_key', 'provenance_identity', 'running', 'football_dw', :'source_snapshot_ref', '{"scope":"local","raw_mode":"foreign_read_only"}'::jsonb)
ON CONFLICT (run_key) DO UPDATE
SET phase = EXCLUDED.phase,
    status = EXCLUDED.status,
    source_database = EXCLUDED.source_database,
    source_snapshot_ref = EXCLUDED.source_snapshot_ref,
    started_at = now(),
    finished_at = NULL,
    metadata = EXCLUDED.metadata
RETURNING rebuild_run_id \gset rebuild_

INSERT INTO control.source_record (
  source_system, entity_type, source_record_key, source_table, source_run_id,
  source_version, first_seen_at, last_seen_at, metadata
)
SELECT DISTINCT ON (CASE WHEN provider = 'legacy_dim_team' THEN 'legacy_control' ELSE provider END, entity_type, source_id)
  CASE WHEN provider = 'legacy_dim_team' THEN 'legacy_control' ELSE provider END,
  entity_type,
  source_id,
  'raw.provider_entity_map',
  NULL,
  source_version,
  first_seen_at,
  updated_at,
  jsonb_build_object(
    'edition_key', edition_key,
    'source_team_key', source_team_key,
    'mapping_state', mapping_state,
    'needs_manual_review', needs_manual_review,
    'review_reason', review_reason
  )
FROM raw_src.provider_entity_map
WHERE CASE WHEN provider = 'legacy_dim_team' THEN 'legacy_control' ELSE provider END IN (
  SELECT source_system FROM control.source_system
)
ORDER BY CASE WHEN provider = 'legacy_dim_team' THEN 'legacy_control' ELSE provider END, entity_type, source_id, updated_at DESC NULLS LAST
ON CONFLICT (source_system, entity_type, source_record_key) DO UPDATE
SET source_version = EXCLUDED.source_version,
    last_seen_at = EXCLUDED.last_seen_at,
    metadata = EXCLUDED.metadata;

INSERT INTO control.entity_source_identity (
  entity_type, source_system, source_entity_id, canonical_entity_id, context_key,
  mapping_state, confidence, resolution_method, valid_from, valid_to, evidence
)
SELECT DISTINCT ON (entity_type, CASE WHEN provider = 'legacy_dim_team' THEN 'legacy_control' ELSE provider END, source_id, COALESCE(edition_key, ''))
  entity_type,
  CASE WHEN provider = 'legacy_dim_team' THEN 'legacy_control' ELSE provider END,
  source_id,
  canonical_id,
  COALESCE(edition_key, ''),
  CASE
    WHEN needs_manual_review THEN 'pending'
    WHEN lower(COALESCE(mapping_state, '')) IN ('approved', 'pending', 'ambiguous', 'quarantined', 'rejected')
      THEN lower(mapping_state)
    ELSE 'pending'
  END,
  CASE
    WHEN mapping_confidence ~ '^[0-9]+(\\.[0-9]+)?$' THEN mapping_confidence::numeric
    WHEN lower(mapping_confidence) IN ('high', 'very_high') THEN 0.95
    WHEN lower(mapping_confidence) = 'medium' THEN 0.75
    WHEN lower(mapping_confidence) = 'low' THEN 0.50
    ELSE NULL
  END,
  resolution_method,
  valid_from,
  valid_to,
  COALESCE(evidence, '{}'::jsonb) || jsonb_build_object(
    'provider', provider,
    'review_reason', review_reason,
    'team_type', team_type
  )
FROM raw_src.provider_entity_map
WHERE CASE WHEN provider = 'legacy_dim_team' THEN 'legacy_control' ELSE provider END IN (
  SELECT source_system FROM control.source_system
)
ORDER BY entity_type, CASE WHEN provider = 'legacy_dim_team' THEN 'legacy_control' ELSE provider END, source_id, COALESCE(edition_key, ''), updated_at DESC NULLS LAST
ON CONFLICT (entity_type, source_system, source_entity_id, context_key) DO UPDATE
SET canonical_entity_id = EXCLUDED.canonical_entity_id,
    mapping_state = EXCLUDED.mapping_state,
    confidence = EXCLUDED.confidence,
    resolution_method = EXCLUDED.resolution_method,
    valid_from = EXCLUDED.valid_from,
    valid_to = EXCLUDED.valid_to,
    evidence = EXCLUDED.evidence,
    updated_at = now();

INSERT INTO control.source_record (
  source_system, entity_type, source_record_key, source_table, metadata
)
SELECT
  'legacy_control',
  'team',
  canonical_team_id::text,
  'control.team_identity',
  jsonb_build_object('identity_state', identity_state, 'team_type', team_type)
FROM control.team_identity
ON CONFLICT (source_system, entity_type, source_record_key) DO NOTHING;

INSERT INTO control.entity_source_identity (
  entity_type, source_system, source_entity_id, canonical_entity_id,
  mapping_state, confidence, resolution_method, evidence
)
SELECT
  'team',
  'legacy_control',
  canonical_team_id::text,
  canonical_team_id::text,
  CASE identity_state
    WHEN 'active' THEN 'approved'
    WHEN 'merged' THEN 'approved'
    ELSE 'pending'
  END,
  decision_confidence,
  decision_method,
  COALESCE(decision_evidence, '{}'::jsonb) || jsonb_build_object(
    'team_name', team_name,
    'country_or_territory', country_or_territory,
    'team_type', team_type,
    'identity_state', identity_state,
    'merged_into_team_id', merged_into_team_id
  )
FROM control.team_identity
ON CONFLICT (entity_type, source_system, source_entity_id, context_key) DO UPDATE
SET canonical_entity_id = EXCLUDED.canonical_entity_id,
    mapping_state = EXCLUDED.mapping_state,
    confidence = EXCLUDED.confidence,
    resolution_method = EXCLUDED.resolution_method,
    evidence = EXCLUDED.evidence,
    updated_at = now();

INSERT INTO control.identity_decision (
  entity_type, source_system, source_entity_id, canonical_entity_id,
  decision_state, method, confidence, evidence, rebuild_run_id
)
SELECT DISTINCT ON (entity_type, source_system, source_entity_id)
  entity_type,
  source_system,
  source_entity_id,
  canonical_entity_id,
  mapping_state,
  resolution_method,
  confidence,
  evidence,
  :rebuild_rebuild_run_id
FROM control.entity_source_identity
ORDER BY entity_type, source_system, source_entity_id,
         (mapping_state = 'approved') DESC,
         confidence DESC NULLS LAST,
         context_key
ON CONFLICT (entity_type, source_system, source_entity_id) DO UPDATE
SET canonical_entity_id = EXCLUDED.canonical_entity_id,
    decision_state = EXCLUDED.decision_state,
    method = EXCLUDED.method,
    confidence = EXCLUDED.confidence,
    evidence = EXCLUDED.evidence,
    rebuild_run_id = EXCLUDED.rebuild_run_id,
    decided_at = now();

UPDATE control.rebuild_run
SET status = 'succeeded', finished_at = now()
WHERE rebuild_run_id = :rebuild_rebuild_run_id;
