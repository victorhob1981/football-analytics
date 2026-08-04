\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.rebuild_run (
  rebuild_run_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  run_key              text NOT NULL UNIQUE,
  phase                text NOT NULL,
  status               text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
  source_database      text NOT NULL,
  source_snapshot_ref  text,
  started_at           timestamptz NOT NULL DEFAULT now(),
  finished_at          timestamptz,
  metadata             jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS control.source_system (
  source_system       text PRIMARY KEY,
  source_kind         text NOT NULL,
  immutable           boolean NOT NULL DEFAULT true,
  priority            integer NOT NULL DEFAULT 100,
  description         text,
  created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS control.source_record (
  source_record_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_system       text NOT NULL REFERENCES control.source_system(source_system),
  entity_type         text NOT NULL,
  source_record_key   text NOT NULL,
  source_table        text NOT NULL,
  source_run_id       text,
  source_version      text,
  source_hash         text,
  record_status       text NOT NULL DEFAULT 'observed'
                      CHECK (record_status IN ('observed', 'accepted', 'quarantined', 'rejected')),
  first_seen_at       timestamptz,
  last_seen_at        timestamptz,
  metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (source_system, entity_type, source_record_key)
);

CREATE TABLE IF NOT EXISTS control.entity_source_identity (
  entity_type         text NOT NULL,
  source_system       text NOT NULL REFERENCES control.source_system(source_system),
  source_entity_id    text NOT NULL,
  canonical_entity_id text NOT NULL,
  context_key         text NOT NULL DEFAULT '',
  mapping_state       text NOT NULL DEFAULT 'pending'
                      CHECK (mapping_state IN ('approved', 'pending', 'ambiguous', 'quarantined', 'rejected')),
  confidence          numeric(6,5),
  resolution_method   text,
  valid_from          date,
  valid_to            date,
  evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (entity_type, source_system, source_entity_id, context_key)
);

CREATE TABLE IF NOT EXISTS control.identity_decision (
  identity_decision_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  entity_type          text NOT NULL,
  source_system        text NOT NULL,
  source_entity_id     text NOT NULL,
  canonical_entity_id  text,
  decision_state        text NOT NULL CHECK (decision_state IN ('approved', 'pending', 'ambiguous', 'quarantined', 'rejected')),
  method               text,
  confidence           numeric(6,5),
  evidence             jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id       bigint REFERENCES control.rebuild_run(rebuild_run_id),
  decided_at           timestamptz NOT NULL DEFAULT now(),
  UNIQUE (entity_type, source_system, source_entity_id)
);

CREATE TABLE IF NOT EXISTS control.match_reconciliation (
  match_reconciliation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source_system           text NOT NULL REFERENCES control.source_system(source_system),
  source_match_id         text NOT NULL,
  canonical_match_id      bigint,
  competition_key         text,
  edition_key             text,
  reconciliation_state    text NOT NULL CHECK (reconciliation_state IN ('approved', 'pending', 'ambiguous', 'quarantined', 'rejected')),
  method                  text,
  confidence              numeric(6,5),
  source_date             date,
  home_source_team_id     text,
  away_source_team_id     text,
  evidence                jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id          bigint REFERENCES control.rebuild_run(rebuild_run_id),
  UNIQUE (source_system, source_match_id)
);

CREATE TABLE IF NOT EXISTS control.quality_issue (
  quality_issue_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  rebuild_run_id      bigint REFERENCES control.rebuild_run(rebuild_run_id),
  entity_type         text NOT NULL,
  entity_key          text NOT NULL,
  rule_code           text NOT NULL,
  severity            text NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'blocker')),
  disposition         text NOT NULL CHECK (disposition IN ('accepted', 'quarantined', 'rejected', 'needs_review')),
  evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (rebuild_run_id, entity_type, entity_key, rule_code)
);

CREATE TABLE IF NOT EXISTS control.publication_decision (
  publication_decision_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  entity_type             text NOT NULL,
  entity_key              text NOT NULL,
  publication_state       text NOT NULL CHECK (publication_state IN ('published', 'pending', 'quarantined', 'rejected')),
  reason_code             text,
  rebuild_run_id          bigint REFERENCES control.rebuild_run(rebuild_run_id),
  evidence                jsonb NOT NULL DEFAULT '{}'::jsonb,
  decided_at              timestamptz NOT NULL DEFAULT now(),
  UNIQUE (entity_type, entity_key)
);

ALTER TABLE control.match_reconciliation SET (autovacuum_enabled = false);
ALTER TABLE control.publication_decision SET (autovacuum_enabled = false);

CREATE TABLE IF NOT EXISTS control.coverage_snapshot (
  coverage_snapshot_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  rebuild_run_id       bigint REFERENCES control.rebuild_run(rebuild_run_id),
  entity_type          text NOT NULL,
  source_system        text,
  competition_key      text,
  edition_key          text,
  observed_rows        bigint NOT NULL DEFAULT 0,
  accepted_rows        bigint NOT NULL DEFAULT 0,
  quarantined_rows     bigint NOT NULL DEFAULT 0,
  first_date           date,
  last_date            date,
  metadata             jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (rebuild_run_id, entity_type, source_system, competition_key, edition_key)
);

CREATE TABLE IF NOT EXISTS control.coverage_reconciliation (
  rebuild_run_id       bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  scope_name           text NOT NULL,
  source_system        text NOT NULL,
  competition_key      text NOT NULL DEFAULT '',
  edition_key          text NOT NULL DEFAULT '',
  reference_rows       bigint NOT NULL DEFAULT 0,
  observed_rows        bigint NOT NULL DEFAULT 0,
  published_rows       bigint NOT NULL DEFAULT 0,
  quarantined_rows     bigint NOT NULL DEFAULT 0,
  pending_rows         bigint NOT NULL DEFAULT 0,
  disposition          text NOT NULL CHECK (disposition IN ('complete', 'explained', 'quarantined', 'pending')),
  evidence             jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (rebuild_run_id, scope_name, source_system, competition_key, edition_key)
);

CREATE TABLE IF NOT EXISTS control.rebuild_fingerprint (
  rebuild_fingerprint_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  rebuild_run_id         bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  object_name            text NOT NULL,
  row_count              bigint NOT NULL,
  fingerprint            text NOT NULL,
  metadata               jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (rebuild_run_id, object_name)
);

CREATE TABLE IF NOT EXISTS control.restore_validation (
  restore_validation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  rebuild_run_id        bigint REFERENCES control.rebuild_run(rebuild_run_id),
  backup_path           text NOT NULL,
  restore_database      text NOT NULL,
  status                text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
  started_at            timestamptz NOT NULL DEFAULT now(),
  finished_at           timestamptz,
  source_counts         jsonb NOT NULL DEFAULT '{}'::jsonb,
  restored_counts       jsonb NOT NULL DEFAULT '{}'::jsonb,
  log_path              text,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb
);

INSERT INTO control.source_system (source_system, source_kind, priority, description)
VALUES
  ('sportmonks', 'provider', 10, 'Primary structured fixture and detail provider'),
  ('statsbomb_open_data', 'provider', 20, 'StatsBomb open data'),
  ('transfermarkt', 'provider', 30, 'Transfermarkt historical and market data'),
  ('eloratings', 'provider', 40, 'Elo historical match and rating data'),
  ('dataset_brasileirao', 'dataset', 50, 'Brasileirão historical dataset'),
  ('fjelstul_worldcup', 'dataset', 50, 'World Cup historical dataset'),
  ('wikipedia', 'reference', 70, 'Historical competition reference data'),
  ('legacy_control', 'derived', 90, 'Existing control registries used as migration evidence')
ON CONFLICT (source_system) DO UPDATE
SET source_kind = EXCLUDED.source_kind,
    priority = EXCLUDED.priority,
    description = EXCLUDED.description;

-- The candidate keeps these legacy control registries locally because later
-- rebuild phases need stable joins. Their source remains the read-only FDW.
CREATE TABLE IF NOT EXISTS control.team_identity AS
SELECT * FROM raw_reference.team_identity WITH NO DATA;
CREATE TABLE IF NOT EXISTS control.competitions AS
SELECT * FROM raw_reference.competitions WITH NO DATA;
CREATE TABLE IF NOT EXISTS control.competition_provider_map AS
SELECT * FROM raw_reference.competition_provider_map WITH NO DATA;
CREATE TABLE IF NOT EXISTS control.external_match_publication_xref AS
SELECT * FROM raw_reference.external_match_publication_xref WITH NO DATA;
CREATE TABLE IF NOT EXISTS control.tm_game_fixture_xref AS
SELECT * FROM raw_reference.tm_game_fixture_xref WITH NO DATA;
CREATE TABLE IF NOT EXISTS control.elo_match_xref AS
SELECT * FROM raw_reference.elo_match_xref WITH NO DATA;
CREATE TABLE IF NOT EXISTS control.brasileirao_fixture_xref AS
SELECT * FROM raw_reference.brasileirao_fixture_xref WITH NO DATA;
CREATE TABLE IF NOT EXISTS control.season_catalog AS
SELECT * FROM raw_reference.season_catalog WITH NO DATA;

TRUNCATE TABLE
  control.team_identity,
  control.competitions,
  control.competition_provider_map,
  control.external_match_publication_xref,
  control.tm_game_fixture_xref,
  control.elo_match_xref,
  control.brasileirao_fixture_xref,
  control.season_catalog;

INSERT INTO control.team_identity SELECT * FROM raw_reference.team_identity;
INSERT INTO control.competitions SELECT * FROM raw_reference.competitions;
INSERT INTO control.competition_provider_map SELECT * FROM raw_reference.competition_provider_map;
INSERT INTO control.external_match_publication_xref SELECT * FROM raw_reference.external_match_publication_xref;
INSERT INTO control.tm_game_fixture_xref SELECT * FROM raw_reference.tm_game_fixture_xref;
INSERT INTO control.elo_match_xref SELECT * FROM raw_reference.elo_match_xref;
INSERT INTO control.brasileirao_fixture_xref SELECT * FROM raw_reference.brasileirao_fixture_xref;
INSERT INTO control.season_catalog SELECT * FROM raw_reference.season_catalog;
