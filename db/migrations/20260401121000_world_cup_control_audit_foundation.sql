-- migrate:up
-- Copa do Mundo | Bloco 2
-- Estruturas mínimas de controle e auditoria, sem tocar em ingestão/publicação.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.wc_coverage_manifest (
  edition_key           TEXT NOT NULL,
  domain_name           TEXT NOT NULL,
  source_name           TEXT NOT NULL,
  coverage_status       TEXT NOT NULL,
  expected_match_count  INTEGER,
  actual_match_count    INTEGER,
  expected_row_count    INTEGER,
  actual_row_count      INTEGER,
  notes                 TEXT,
  computed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_wc_coverage_manifest
    PRIMARY KEY (edition_key, domain_name, source_name),
  CONSTRAINT chk_wc_coverage_manifest_source_name
    CHECK (source_name IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more'
    )),
  CONSTRAINT chk_wc_coverage_manifest_status
    CHECK (coverage_status IN (
      'FULL_TOURNAMENT',
      'PARTIAL_MATCH_SAMPLE',
      'PARTIAL_DOMAIN',
      'PROVIDER_COVERAGE_GAP',
      'NOT_IN_SCOPE_YET'
    ))
);

CREATE INDEX IF NOT EXISTS idx_wc_coverage_manifest_status
  ON silver.wc_coverage_manifest (coverage_status);

CREATE INDEX IF NOT EXISTS idx_wc_coverage_manifest_edition
  ON silver.wc_coverage_manifest (edition_key);

CREATE TABLE IF NOT EXISTS silver.wc_source_divergences (
  divergence_pk      BIGSERIAL PRIMARY KEY,
  edition_key        TEXT NOT NULL,
  entity_type        TEXT NOT NULL,
  internal_id        TEXT,
  source_left        TEXT NOT NULL,
  source_right       TEXT NOT NULL,
  divergence_type    TEXT NOT NULL,
  field_name         TEXT NOT NULL,
  left_value         JSONB,
  right_value        JSONB,
  severity           TEXT NOT NULL,
  resolution_status  TEXT NOT NULL DEFAULT 'open',
  detected_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_wc_source_divergences_source_left
    CHECK (source_left IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more'
    )),
  CONSTRAINT chk_wc_source_divergences_source_right
    CHECK (source_right IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more'
    )),
  CONSTRAINT chk_wc_source_divergences_severity
    CHECK (severity IN ('info', 'warning', 'blocking')),
  CONSTRAINT chk_wc_source_divergences_resolution_status
    CHECK (resolution_status IN ('open', 'accepted', 'resolved', 'ignored'))
);

CREATE INDEX IF NOT EXISTS idx_wc_source_divergences_edition_severity
  ON silver.wc_source_divergences (edition_key, severity);

CREATE INDEX IF NOT EXISTS idx_wc_source_divergences_internal_id
  ON silver.wc_source_divergences (internal_id);

CREATE INDEX IF NOT EXISTS idx_wc_source_divergences_resolution_status
  ON silver.wc_source_divergences (resolution_status);

CREATE TABLE IF NOT EXISTS control.wc_entity_match_review_queue (
  review_pk              BIGSERIAL PRIMARY KEY,
  entity_type            TEXT NOT NULL,
  edition_key            TEXT,
  source_name            TEXT NOT NULL,
  source_external_id     TEXT NOT NULL,
  candidate_internal_id  TEXT,
  confidence_level       TEXT NOT NULL,
  review_reason          TEXT NOT NULL,
  candidate_payload      JSONB NOT NULL,
  review_status          TEXT NOT NULL DEFAULT 'pending',
  reviewer_name          TEXT,
  reviewed_at            TIMESTAMPTZ,
  resolved_internal_id   TEXT,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_wc_entity_match_review_source_name
    CHECK (source_name IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more'
    )),
  CONSTRAINT chk_wc_entity_match_review_confidence
    CHECK (confidence_level IN ('exact', 'high', 'medium', 'low')),
  CONSTRAINT chk_wc_entity_match_review_status
    CHECK (review_status IN ('pending', 'approved', 'rejected', 'deferred'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_wc_entity_match_review_source_entity_scope
  ON control.wc_entity_match_review_queue (
    entity_type,
    source_name,
    source_external_id,
    COALESCE(edition_key, 'GLOBAL')
  );

CREATE INDEX IF NOT EXISTS idx_wc_entity_match_review_status
  ON control.wc_entity_match_review_queue (review_status);

CREATE INDEX IF NOT EXISTS idx_wc_entity_match_review_entity_edition
  ON control.wc_entity_match_review_queue (entity_type, edition_key);

CREATE INDEX IF NOT EXISTS idx_wc_entity_match_review_source_name
  ON control.wc_entity_match_review_queue (source_name);

-- migrate:down
SELECT 1;
