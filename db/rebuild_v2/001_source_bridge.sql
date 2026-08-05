\set ON_ERROR_STOP on

-- Local rebuild only. The source database remains immutable; this schema is
-- a read-only foreign view over raw so the candidate does not copy the large
-- raw payloads into a Windows bind mount.
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SCHEMA IF EXISTS raw_src CASCADE;
DROP SERVER IF EXISTS source_dw CASCADE;

CREATE SERVER source_dw
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'football_postgres', port '5432', dbname 'football_dw');

CREATE USER MAPPING FOR football
  SERVER source_dw
  OPTIONS (user 'football', password :'source_password');

CREATE SCHEMA raw_src;
IMPORT FOREIGN SCHEMA raw FROM SERVER source_dw INTO raw_src;

DROP SCHEMA IF EXISTS raw_reference CASCADE;
CREATE SCHEMA raw_reference;
IMPORT FOREIGN SCHEMA shadow_dbt_20260716 LIMIT TO (fact_matches)
  FROM SERVER source_dw INTO raw_reference;
IMPORT FOREIGN SCHEMA mart LIMIT TO (fact_tie_results)
  FROM SERVER source_dw INTO raw_reference;
IMPORT FOREIGN SCHEMA control LIMIT TO (
  team_identity,
  competitions,
  competition_provider_map,
  external_match_publication_xref,
  tm_game_fixture_xref,
  elo_match_xref,
  brasileirao_fixture_xref,
  season_catalog
) FROM SERVER source_dw INTO raw_reference;
