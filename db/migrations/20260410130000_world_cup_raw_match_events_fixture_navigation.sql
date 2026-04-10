-- migrate:up
-- Copa do Mundo 2022 | Correcao minima de navegabilidade raw.fixtures -> raw.wc_match_events
-- Publica fixture_id diretamente em raw.wc_match_events para viabilizar match view
-- sem tocar em raw.match_events nem contaminar raw.fixtures com ids internos/source-scoped.

ALTER TABLE raw.wc_match_events
  ADD COLUMN IF NOT EXISTS fixture_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_wc_match_events_fixture_id
  ON raw.wc_match_events (fixture_id);

-- migrate:down
SELECT 1;
