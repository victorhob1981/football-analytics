-- migrate:up
CREATE INDEX IF NOT EXISTS idx_mart_fact_fixture_lineups_team_match
  ON mart.fact_fixture_lineups (team_id, match_id);

CREATE INDEX IF NOT EXISTS idx_raw_fixture_lineups_team_fixture
  ON raw.fixture_lineups (team_id, fixture_id);

ANALYZE mart.fact_fixture_lineups;
ANALYZE raw.fixture_lineups;

-- migrate:down
DROP INDEX IF EXISTS raw.idx_raw_fixture_lineups_team_fixture;
DROP INDEX IF EXISTS mart.idx_mart_fact_fixture_lineups_team_match;
