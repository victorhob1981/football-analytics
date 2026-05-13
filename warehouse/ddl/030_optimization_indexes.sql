-- Performance indexes for BI workloads (Metabase)

-- raw.match_events: frequent join/filter by fixture and event type
-- Note: event type is stored in column "type" in raw layer.
CREATE INDEX IF NOT EXISTS idx_raw_match_events_fixture_type
  ON raw.match_events (fixture_id, type);

-- gold.fact_match_events: frequent filter by team and event type, covering is_goal
CREATE INDEX IF NOT EXISTS idx_gold_fact_match_events_team_event_type
  ON gold.fact_match_events (team_id, event_type) INCLUDE (is_goal);

-- gold.fact_matches: frequent filters by season/league and time range
CREATE INDEX IF NOT EXISTS idx_gold_fact_matches_season_league_date
  ON gold.fact_matches (season, league_id, date_day);
