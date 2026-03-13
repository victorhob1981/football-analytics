-- migrate:up
-- ============================================================
-- Partições históricas de raw.match_events para o backfill
--
-- O schema base criou apenas match_events_2024.
-- O backfill cobre: Europa 2020/21–2024/25 e Brasil 2021–2025.
-- Inserções em seasons sem partição falham com:
--   ERROR: no partition of relation "match_events" found for row
--
-- Adicionar aqui todas as seasons do escopo de backfill.
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.match_events_2020
  PARTITION OF raw.match_events
  FOR VALUES IN (2020);

CREATE TABLE IF NOT EXISTS raw.match_events_2021
  PARTITION OF raw.match_events
  FOR VALUES IN (2021);

CREATE TABLE IF NOT EXISTS raw.match_events_2022
  PARTITION OF raw.match_events
  FOR VALUES IN (2022);

CREATE TABLE IF NOT EXISTS raw.match_events_2023
  PARTITION OF raw.match_events
  FOR VALUES IN (2023);

-- 2024 já existe no schema base. Incluído aqui apenas como
-- documentação — o IF NOT EXISTS protege contra erro de duplicata.
CREATE TABLE IF NOT EXISTS raw.match_events_2024
  PARTITION OF raw.match_events
  FOR VALUES IN (2024);

CREATE TABLE IF NOT EXISTS raw.match_events_2025
  PARTITION OF raw.match_events
  FOR VALUES IN (2025);

-- Índices por partição são herdados da tabela pai quando criados
-- com CREATE INDEX ON raw.match_events (...). Não é necessário
-- recriar por partição individualmente.

-- migrate:down
DROP TABLE IF EXISTS raw.match_events_2025;
DROP TABLE IF EXISTS raw.match_events_2023;
DROP TABLE IF EXISTS raw.match_events_2022;
DROP TABLE IF EXISTS raw.match_events_2021;
DROP TABLE IF EXISTS raw.match_events_2020;
-- Não dropamos 2024 aqui pois pertence ao schema base.
