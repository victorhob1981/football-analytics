-- Diagnostico de cobertura ponta-a-ponta para um escopo league/season/provider.
-- Ajuste os valores da CTE `params` para o escopo desejado.
-- Resultado esperado para "completo":
--   - pct_* de fixtures proximo de 100% para dominos por fixture
--   - pct_players_with_player_season_stats proximo de 100%
--   - pct_h2h_pairs_with_data proximo de 100%

WITH params AS (
    SELECT
        648::BIGINT AS league_id,
        2024::INT AS season,
        'sportmonks'::TEXT AS provider
),
season_ids AS (
    SELECT DISTINCT cs.season_id
    FROM raw.competition_seasons cs
    JOIN params p
      ON p.provider = cs.provider
     AND p.league_id = cs.league_id
    WHERE cs.season_year = p.season
),
fixtures_scope AS (
    SELECT
        f.fixture_id,
        f.home_team_id,
        f.away_team_id,
        f.status_short
    FROM raw.fixtures f
    JOIN params p
      ON p.league_id = f.league_id
     AND p.season = f.season
    WHERE f.fixture_id IS NOT NULL
),
finished_fixtures AS (
    SELECT *
    FROM fixtures_scope
    WHERE status_short IN ('FT', 'AET', 'PEN')
),
stats_fixtures AS (
    SELECT DISTINCT s.fixture_id
    FROM raw.match_statistics s
),
event_fixtures AS (
    SELECT DISTINCT e.fixture_id
    FROM raw.match_events e
),
lineup_fixtures AS (
    SELECT DISTINCT fl.fixture_id
    FROM raw.fixture_lineups fl
    JOIN params p
      ON p.provider = fl.provider
),
fixture_player_stats_fixtures AS (
    SELECT DISTINCT fps.fixture_id
    FROM raw.fixture_player_statistics fps
    JOIN params p
      ON p.provider = fps.provider
),
players_scope AS (
    SELECT DISTINCT fl.player_id
    FROM raw.fixture_lineups fl
    JOIN params p
      ON p.provider = fl.provider
    JOIN fixtures_scope fs
      ON fs.fixture_id = fl.fixture_id
    WHERE fl.player_id IS NOT NULL
),
players_with_player_season_stats AS (
    SELECT DISTINCT pss.player_id
    FROM raw.player_season_statistics pss
    JOIN params p
      ON p.provider = pss.provider
    JOIN players_scope ps
      ON ps.player_id = pss.player_id
    WHERE pss.league_id = p.league_id
      AND (
        pss.season_id IN (SELECT season_id FROM season_ids)
        OR pss.season_name = p.season::TEXT
        OR pss.season_name LIKE '%' || p.season::TEXT || '%'
      )
),
teams_scope AS (
    SELECT DISTINCT team_id
    FROM (
        SELECT home_team_id AS team_id
        FROM fixtures_scope
        WHERE home_team_id IS NOT NULL
        UNION
        SELECT away_team_id AS team_id
        FROM fixtures_scope
        WHERE away_team_id IS NOT NULL
    ) t
),
teams_with_sidelined AS (
    SELECT DISTINCT ts.team_id
    FROM raw.team_sidelined ts
    JOIN params p
      ON p.provider = ts.provider
    JOIN teams_scope t
      ON t.team_id = ts.team_id
),
teams_with_coaches AS (
    SELECT DISTINCT tc.team_id
    FROM raw.team_coaches tc
    JOIN params p
      ON p.provider = tc.provider
    JOIN teams_scope t
      ON t.team_id = tc.team_id
),
pairs_scope AS (
    SELECT DISTINCT
        LEAST(home_team_id, away_team_id) AS pair_team_id,
        GREATEST(home_team_id, away_team_id) AS pair_opponent_id
    FROM fixtures_scope
    WHERE home_team_id IS NOT NULL
      AND away_team_id IS NOT NULL
),
pairs_with_h2h AS (
    SELECT DISTINCT
        h.pair_team_id,
        h.pair_opponent_id
    FROM raw.head_to_head_fixtures h
    JOIN params p
      ON p.provider = h.provider
    JOIN pairs_scope ps
      ON ps.pair_team_id = h.pair_team_id
     AND ps.pair_opponent_id = h.pair_opponent_id
),
competition_rows AS (
    SELECT
        (SELECT COUNT(*) FROM raw.competition_seasons cs JOIN params p ON p.provider = cs.provider AND p.league_id = cs.league_id WHERE cs.season_year = p.season) AS competition_seasons_rows,
        (
            SELECT COUNT(*)
            FROM raw.competition_stages st
            JOIN params p
              ON p.provider = st.provider
             AND p.league_id = st.league_id
            WHERE st.season_id IN (SELECT season_id FROM season_ids)
        ) AS competition_stages_rows,
        (
            SELECT COUNT(*)
            FROM raw.competition_rounds r
            JOIN params p
              ON p.provider = r.provider
             AND p.league_id = r.league_id
            WHERE r.season_id IN (SELECT season_id FROM season_ids)
        ) AS competition_rounds_rows,
        (
            SELECT COUNT(*)
            FROM raw.standings_snapshots s
            JOIN params p
              ON p.provider = s.provider
             AND p.league_id = s.league_id
            WHERE s.season_id IN (SELECT season_id FROM season_ids)
        ) AS standings_rows
)
SELECT
    p.provider,
    p.league_id,
    p.season,
    (SELECT COUNT(*) FROM fixtures_scope) AS fixtures_total,
    (SELECT COUNT(*) FROM finished_fixtures) AS fixtures_finished,
    (
        SELECT COUNT(*)
        FROM finished_fixtures ff
        WHERE EXISTS (SELECT 1 FROM stats_fixtures sf WHERE sf.fixture_id = ff.fixture_id)
    ) AS fixtures_with_statistics,
    ROUND(
        (
            (
                SELECT COUNT(*)
                FROM finished_fixtures ff
                WHERE EXISTS (SELECT 1 FROM stats_fixtures sf WHERE sf.fixture_id = ff.fixture_id)
            )::NUMERIC
            / NULLIF((SELECT COUNT(*) FROM finished_fixtures), 0)
        ) * 100.0,
        2
    ) AS pct_fixtures_with_statistics,
    (
        SELECT COUNT(*)
        FROM finished_fixtures ff
        WHERE EXISTS (SELECT 1 FROM event_fixtures ef WHERE ef.fixture_id = ff.fixture_id)
    ) AS fixtures_with_events,
    ROUND(
        (
            (
                SELECT COUNT(*)
                FROM finished_fixtures ff
                WHERE EXISTS (SELECT 1 FROM event_fixtures ef WHERE ef.fixture_id = ff.fixture_id)
            )::NUMERIC
            / NULLIF((SELECT COUNT(*) FROM finished_fixtures), 0)
        ) * 100.0,
        2
    ) AS pct_fixtures_with_events,
    (
        SELECT COUNT(*)
        FROM finished_fixtures ff
        WHERE EXISTS (SELECT 1 FROM lineup_fixtures lf WHERE lf.fixture_id = ff.fixture_id)
    ) AS fixtures_with_lineups,
    ROUND(
        (
            (
                SELECT COUNT(*)
                FROM finished_fixtures ff
                WHERE EXISTS (SELECT 1 FROM lineup_fixtures lf WHERE lf.fixture_id = ff.fixture_id)
            )::NUMERIC
            / NULLIF((SELECT COUNT(*) FROM finished_fixtures), 0)
        ) * 100.0,
        2
    ) AS pct_fixtures_with_lineups,
    (
        SELECT COUNT(*)
        FROM finished_fixtures ff
        WHERE EXISTS (
            SELECT 1
            FROM fixture_player_stats_fixtures fps
            WHERE fps.fixture_id = ff.fixture_id
        )
    ) AS fixtures_with_fixture_player_stats,
    ROUND(
        (
            (
                SELECT COUNT(*)
                FROM finished_fixtures ff
                WHERE EXISTS (
                    SELECT 1
                    FROM fixture_player_stats_fixtures fps
                    WHERE fps.fixture_id = ff.fixture_id
                )
            )::NUMERIC
            / NULLIF((SELECT COUNT(*) FROM finished_fixtures), 0)
        ) * 100.0,
        2
    ) AS pct_fixtures_with_fixture_player_stats,
    (SELECT COUNT(*) FROM players_scope) AS players_in_scope,
    (SELECT COUNT(*) FROM players_with_player_season_stats) AS players_with_player_season_stats,
    ROUND(
        (
            (SELECT COUNT(*) FROM players_with_player_season_stats)::NUMERIC
            / NULLIF((SELECT COUNT(*) FROM players_scope), 0)
        ) * 100.0,
        2
    ) AS pct_players_with_player_season_stats,
    (SELECT COUNT(*) FROM teams_scope) AS teams_in_scope,
    (SELECT COUNT(*) FROM teams_with_sidelined) AS teams_with_sidelined,
    ROUND(
        (
            (SELECT COUNT(*) FROM teams_with_sidelined)::NUMERIC
            / NULLIF((SELECT COUNT(*) FROM teams_scope), 0)
        ) * 100.0,
        2
    ) AS pct_teams_with_sidelined,
    (SELECT COUNT(*) FROM teams_with_coaches) AS teams_with_coaches,
    ROUND(
        (
            (SELECT COUNT(*) FROM teams_with_coaches)::NUMERIC
            / NULLIF((SELECT COUNT(*) FROM teams_scope), 0)
        ) * 100.0,
        2
    ) AS pct_teams_with_coaches,
    (SELECT COUNT(*) FROM pairs_scope) AS h2h_pairs_in_scope,
    (SELECT COUNT(*) FROM pairs_with_h2h) AS h2h_pairs_with_data,
    ROUND(
        (
            (SELECT COUNT(*) FROM pairs_with_h2h)::NUMERIC
            / NULLIF((SELECT COUNT(*) FROM pairs_scope), 0)
        ) * 100.0,
        2
    ) AS pct_h2h_pairs_with_data,
    c.competition_seasons_rows,
    c.competition_stages_rows,
    c.competition_rounds_rows,
    c.standings_rows
FROM params p
CROSS JOIN competition_rows c;
