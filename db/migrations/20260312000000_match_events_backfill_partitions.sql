-- migrate:up
-- Particoes historicas de raw.match_events necessarias para o backfill 2020-2025.

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

CREATE TABLE IF NOT EXISTS raw.match_events_2024
  PARTITION OF raw.match_events
  FOR VALUES IN (2024);

CREATE TABLE IF NOT EXISTS raw.match_events_2025
  PARTITION OF raw.match_events
  FOR VALUES IN (2025);

-- migrate:down
DROP TABLE IF EXISTS raw.match_events_2025;
DROP TABLE IF EXISTS raw.match_events_2023;
DROP TABLE IF EXISTS raw.match_events_2022;
DROP TABLE IF EXISTS raw.match_events_2021;
DROP TABLE IF EXISTS raw.match_events_2020;
