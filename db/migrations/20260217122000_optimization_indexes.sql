-- migrate:up
CREATE INDEX IF NOT EXISTS idx_raw_match_events_fixture_type
  ON raw.match_events (fixture_id, type);

CREATE INDEX IF NOT EXISTS idx_gold_fact_match_events_team_event_type
  ON gold.fact_match_events (team_id, event_type) INCLUDE (is_goal);

CREATE INDEX IF NOT EXISTS idx_gold_fact_matches_season_league_date
  ON gold.fact_matches (season, league_id, date_day);

-- migrate:down
DROP INDEX IF EXISTS idx_gold_fact_matches_season_league_date;
DROP INDEX IF EXISTS idx_gold_fact_match_events_team_event_type;
DROP INDEX IF EXISTS idx_raw_match_events_fixture_type;
