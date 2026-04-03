-- migrate:up
-- Copa do Mundo | Bloco 3
-- Bronze inicial 2022 a partir de snapshots locais versionados.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.statsbomb_wc_matches (
  source_name                TEXT NOT NULL,
  source_version             TEXT NOT NULL,
  edition_key                TEXT NOT NULL,
  snapshot_path              TEXT NOT NULL,
  snapshot_checksum_sha256   TEXT NOT NULL,
  source_file                TEXT NOT NULL,
  competition_id             BIGINT NOT NULL,
  season_id                  BIGINT NOT NULL,
  match_id                   BIGINT NOT NULL,
  match_date                 TEXT,
  payload                    JSONB NOT NULL,
  ingested_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_statsbomb_wc_matches PRIMARY KEY (source_version, match_id),
  CONSTRAINT chk_statsbomb_wc_matches_source_name
    CHECK (source_name = 'statsbomb_open_data')
);

CREATE INDEX IF NOT EXISTS idx_statsbomb_wc_matches_edition
  ON bronze.statsbomb_wc_matches (edition_key);

CREATE INDEX IF NOT EXISTS idx_statsbomb_wc_matches_match_id
  ON bronze.statsbomb_wc_matches (match_id);

CREATE TABLE IF NOT EXISTS bronze.statsbomb_wc_events (
  source_name                TEXT NOT NULL,
  source_version             TEXT NOT NULL,
  edition_key                TEXT NOT NULL,
  snapshot_path              TEXT NOT NULL,
  snapshot_checksum_sha256   TEXT NOT NULL,
  source_file                TEXT NOT NULL,
  match_id                   BIGINT NOT NULL,
  payload_item_count         INTEGER NOT NULL,
  payload                    JSONB NOT NULL,
  ingested_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_statsbomb_wc_events PRIMARY KEY (source_version, match_id),
  CONSTRAINT chk_statsbomb_wc_events_source_name
    CHECK (source_name = 'statsbomb_open_data')
);

CREATE INDEX IF NOT EXISTS idx_statsbomb_wc_events_edition
  ON bronze.statsbomb_wc_events (edition_key);

CREATE INDEX IF NOT EXISTS idx_statsbomb_wc_events_match_id
  ON bronze.statsbomb_wc_events (match_id);

CREATE TABLE IF NOT EXISTS bronze.statsbomb_wc_lineups (
  source_name                TEXT NOT NULL,
  source_version             TEXT NOT NULL,
  edition_key                TEXT NOT NULL,
  snapshot_path              TEXT NOT NULL,
  snapshot_checksum_sha256   TEXT NOT NULL,
  source_file                TEXT NOT NULL,
  match_id                   BIGINT NOT NULL,
  payload_item_count         INTEGER NOT NULL,
  payload                    JSONB NOT NULL,
  ingested_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_statsbomb_wc_lineups PRIMARY KEY (source_version, match_id),
  CONSTRAINT chk_statsbomb_wc_lineups_source_name
    CHECK (source_name = 'statsbomb_open_data')
);

CREATE INDEX IF NOT EXISTS idx_statsbomb_wc_lineups_edition
  ON bronze.statsbomb_wc_lineups (edition_key);

CREATE INDEX IF NOT EXISTS idx_statsbomb_wc_lineups_match_id
  ON bronze.statsbomb_wc_lineups (match_id);

CREATE TABLE IF NOT EXISTS bronze.statsbomb_wc_three_sixty (
  source_name                TEXT NOT NULL,
  source_version             TEXT NOT NULL,
  edition_key                TEXT NOT NULL,
  snapshot_path              TEXT NOT NULL,
  snapshot_checksum_sha256   TEXT NOT NULL,
  source_file                TEXT NOT NULL,
  match_id                   BIGINT NOT NULL,
  payload_item_count         INTEGER NOT NULL,
  payload                    JSONB NOT NULL,
  ingested_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_statsbomb_wc_three_sixty PRIMARY KEY (source_version, match_id),
  CONSTRAINT chk_statsbomb_wc_three_sixty_source_name
    CHECK (source_name = 'statsbomb_open_data')
);

CREATE INDEX IF NOT EXISTS idx_statsbomb_wc_three_sixty_edition
  ON bronze.statsbomb_wc_three_sixty (edition_key);

CREATE INDEX IF NOT EXISTS idx_statsbomb_wc_three_sixty_match_id
  ON bronze.statsbomb_wc_three_sixty (match_id);

CREATE TABLE IF NOT EXISTS bronze.fjelstul_wc_matches (
  source_name                TEXT NOT NULL,
  source_version             TEXT NOT NULL,
  edition_key                TEXT NOT NULL,
  snapshot_path              TEXT NOT NULL,
  snapshot_checksum_sha256   TEXT NOT NULL,
  source_file                TEXT NOT NULL,
  key_id                     TEXT NOT NULL,
  tournament_id              TEXT NOT NULL,
  match_id                   TEXT NOT NULL,
  stage_name                 TEXT,
  group_name                 TEXT,
  home_team_id               TEXT,
  away_team_id               TEXT,
  match_date                 TEXT,
  payload                    JSONB NOT NULL,
  ingested_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_fjelstul_wc_matches PRIMARY KEY (source_version, key_id),
  CONSTRAINT chk_fjelstul_wc_matches_source_name
    CHECK (source_name = 'fjelstul_worldcup')
);

CREATE INDEX IF NOT EXISTS idx_fjelstul_wc_matches_edition
  ON bronze.fjelstul_wc_matches (edition_key);

CREATE INDEX IF NOT EXISTS idx_fjelstul_wc_matches_tournament
  ON bronze.fjelstul_wc_matches (tournament_id);

CREATE INDEX IF NOT EXISTS idx_fjelstul_wc_matches_match_id
  ON bronze.fjelstul_wc_matches (match_id);

CREATE TABLE IF NOT EXISTS bronze.fjelstul_wc_groups (
  source_name                TEXT NOT NULL,
  source_version             TEXT NOT NULL,
  edition_key                TEXT NOT NULL,
  snapshot_path              TEXT NOT NULL,
  snapshot_checksum_sha256   TEXT NOT NULL,
  source_file                TEXT NOT NULL,
  key_id                     TEXT NOT NULL,
  tournament_id              TEXT NOT NULL,
  stage_number               TEXT,
  stage_name                 TEXT,
  group_name                 TEXT,
  count_teams                TEXT,
  payload                    JSONB NOT NULL,
  ingested_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_fjelstul_wc_groups PRIMARY KEY (source_version, key_id),
  CONSTRAINT chk_fjelstul_wc_groups_source_name
    CHECK (source_name = 'fjelstul_worldcup')
);

CREATE INDEX IF NOT EXISTS idx_fjelstul_wc_groups_edition
  ON bronze.fjelstul_wc_groups (edition_key);

CREATE INDEX IF NOT EXISTS idx_fjelstul_wc_groups_tournament
  ON bronze.fjelstul_wc_groups (tournament_id);

CREATE TABLE IF NOT EXISTS bronze.fjelstul_wc_group_standings (
  source_name                TEXT NOT NULL,
  source_version             TEXT NOT NULL,
  edition_key                TEXT NOT NULL,
  snapshot_path              TEXT NOT NULL,
  snapshot_checksum_sha256   TEXT NOT NULL,
  source_file                TEXT NOT NULL,
  key_id                     TEXT NOT NULL,
  tournament_id              TEXT NOT NULL,
  stage_number               TEXT,
  stage_name                 TEXT,
  group_name                 TEXT,
  position                   TEXT,
  team_id                    TEXT,
  team_name                  TEXT,
  team_code                  TEXT,
  advanced                   TEXT,
  payload                    JSONB NOT NULL,
  ingested_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_fjelstul_wc_group_standings PRIMARY KEY (source_version, key_id),
  CONSTRAINT chk_fjelstul_wc_group_standings_source_name
    CHECK (source_name = 'fjelstul_worldcup')
);

CREATE INDEX IF NOT EXISTS idx_fjelstul_wc_group_standings_edition
  ON bronze.fjelstul_wc_group_standings (edition_key);

CREATE INDEX IF NOT EXISTS idx_fjelstul_wc_group_standings_tournament
  ON bronze.fjelstul_wc_group_standings (tournament_id);

CREATE INDEX IF NOT EXISTS idx_fjelstul_wc_group_standings_group_name
  ON bronze.fjelstul_wc_group_standings (group_name);

CREATE TABLE IF NOT EXISTS bronze.fjelstul_wc_manager_appointments (
  source_name                TEXT NOT NULL,
  source_version             TEXT NOT NULL,
  edition_key                TEXT NOT NULL,
  snapshot_path              TEXT NOT NULL,
  snapshot_checksum_sha256   TEXT NOT NULL,
  source_file                TEXT NOT NULL,
  key_id                     TEXT NOT NULL,
  tournament_id              TEXT NOT NULL,
  team_id                    TEXT,
  team_name                  TEXT,
  team_code                  TEXT,
  manager_id                 TEXT,
  family_name                TEXT,
  given_name                 TEXT,
  country_name               TEXT,
  payload                    JSONB NOT NULL,
  ingested_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT pk_fjelstul_wc_manager_appointments PRIMARY KEY (source_version, key_id),
  CONSTRAINT chk_fjelstul_wc_manager_appointments_source_name
    CHECK (source_name = 'fjelstul_worldcup')
);

CREATE INDEX IF NOT EXISTS idx_fjelstul_wc_manager_appointments_edition
  ON bronze.fjelstul_wc_manager_appointments (edition_key);

CREATE INDEX IF NOT EXISTS idx_fjelstul_wc_manager_appointments_tournament
  ON bronze.fjelstul_wc_manager_appointments (tournament_id);

CREATE INDEX IF NOT EXISTS idx_fjelstul_wc_manager_appointments_team_id
  ON bronze.fjelstul_wc_manager_appointments (team_id);

-- migrate:down
SELECT 1;
