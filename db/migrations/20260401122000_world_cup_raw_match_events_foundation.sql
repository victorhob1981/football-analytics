-- migrate:up
-- Copa do Mundo | Bloco 2
-- Raw permanente de eventos ricos da Copa, sem convergência para raw.match_events.

CREATE TABLE IF NOT EXISTS raw.wc_match_events (
  wc_match_event_pk      BIGSERIAL PRIMARY KEY,
  internal_match_id      TEXT NOT NULL,
  edition_key            TEXT NOT NULL,
  source_name            TEXT NOT NULL,
  source_version         TEXT NOT NULL,
  source_match_id        TEXT NOT NULL,
  source_event_id        TEXT NOT NULL,
  event_index            INTEGER NOT NULL,
  team_internal_id       TEXT,
  player_internal_id     TEXT,
  event_type             TEXT NOT NULL,
  period                 INTEGER,
  minute                 INTEGER,
  second                 NUMERIC(8,3),
  location_x             NUMERIC(8,3),
  location_y             NUMERIC(8,3),
  outcome_label          TEXT,
  play_pattern_label     TEXT,
  is_three_sixty_backed  BOOLEAN NOT NULL DEFAULT FALSE,
  event_payload          JSONB NOT NULL,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_wc_match_events_source_event
    UNIQUE (source_name, source_match_id, source_event_id),
  CONSTRAINT uq_wc_match_events_match_event_index
    UNIQUE (internal_match_id, source_name, event_index),
  CONSTRAINT chk_wc_match_events_source_name
    CHECK (source_name IN (
      'statsbomb_open_data',
      'fjelstul_worldcup',
      'openfootball_worldcup',
      'openfootball_worldcup_more'
    ))
);

CREATE INDEX IF NOT EXISTS idx_wc_match_events_internal_match_id
  ON raw.wc_match_events (internal_match_id);

CREATE INDEX IF NOT EXISTS idx_wc_match_events_edition_key
  ON raw.wc_match_events (edition_key);

CREATE INDEX IF NOT EXISTS idx_wc_match_events_team_internal_id
  ON raw.wc_match_events (team_internal_id);

CREATE INDEX IF NOT EXISTS idx_wc_match_events_player_internal_id
  ON raw.wc_match_events (player_internal_id);

-- migrate:down
SELECT 1;
