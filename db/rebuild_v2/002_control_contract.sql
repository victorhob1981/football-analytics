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
