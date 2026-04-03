-- migrate:up
-- Copa do Mundo | Bloco 1
-- Snapshot gate operacional para versionamento, licença e proveniência local.

CREATE TABLE IF NOT EXISTS control.wc_source_snapshots (
  snapshot_pk               BIGSERIAL PRIMARY KEY,
  source_name               TEXT NOT NULL,
  source_url                TEXT NOT NULL,
  source_version            TEXT NOT NULL,
  source_commit_or_release  TEXT NOT NULL,
  edition_scope             TEXT NOT NULL,
  accessed_at               TIMESTAMPTZ NOT NULL,
  checksum_sha256           TEXT NOT NULL,
  local_path                TEXT NOT NULL,
  license_code              TEXT NOT NULL,
  attribution_note          TEXT NOT NULL,
  usage_decision            TEXT NOT NULL,
  is_active                 BOOLEAN NOT NULL DEFAULT TRUE,
  created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_wc_source_snapshots_source_version
    UNIQUE (source_name, source_commit_or_release),
  CONSTRAINT chk_wc_source_snapshots_source_name
    CHECK (source_name IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more'
    )),
  CONSTRAINT chk_wc_source_snapshots_usage_decision
    CHECK (usage_decision IN (
      'now',
      'later',
      'optional_validation_only',
      'not_in_scope_now'
    ))
);

CREATE INDEX IF NOT EXISTS idx_wc_source_snapshots_source_name
  ON control.wc_source_snapshots (source_name);

CREATE UNIQUE INDEX IF NOT EXISTS uq_wc_source_snapshots_active_source
  ON control.wc_source_snapshots (source_name)
  WHERE is_active;

-- migrate:down
SELECT 1;
