WITH fixture_team_rows AS (
    SELECT
        f.season,
        f.year,
        f.month,
        f.fixture_id,
        f.home_team_id AS team_id,
        COALESCE(f.home_goals, 0) AS goals_for
    FROM raw.fixtures f
    WHERE f.league_id = :league_id
      AND f.season = :season
      AND f.home_team_id IS NOT NULL

    UNION ALL

    SELECT
        f.season,
        f.year,
        f.month,
        f.fixture_id,
        f.away_team_id AS team_id,
        COALESCE(f.away_goals, 0) AS goals_for
    FROM raw.fixtures f
    WHERE f.league_id = :league_id
      AND f.season = :season
      AND f.away_team_id IS NOT NULL
),
joined AS (
    SELECT
        fr.season,
        fr.year,
        fr.month,
        fr.team_id,
        fr.goals_for,
        COALESCE(ms.ball_possession, 0) AS ball_possession,
        COALESCE(ms.total_shots, 0) AS total_shots,
        COALESCE(ms.shots_on_goal, 0) AS shots_on_goal,
        ms.passes_pct,
        COALESCE(ms.fouls, 0) AS fouls
    FROM fixture_team_rows fr
    JOIN raw.match_statistics ms
      ON ms.fixture_id = fr.fixture_id
     AND ms.team_id = fr.team_id
),
aggregated AS (
    SELECT
        season,
        year,
        month,
        team_id,
        ROUND(AVG(ball_possession)::NUMERIC, 2) AS avg_ball_possession,
        SUM(total_shots)::INT AS total_shots,
        SUM(shots_on_goal)::INT AS shots_on_target,
        ROUND(
            (SUM(goals_for)::NUMERIC / NULLIF(SUM(total_shots), 0)) * 100,
            2
        ) AS conversion_rate,
        ROUND(AVG(passes_pct)::NUMERIC, 2) AS pass_accuracy,
        SUM(fouls)::INT AS fouls_committed
    FROM joined
    GROUP BY season, year, month, team_id
),
upserted AS (
    INSERT INTO mart.team_performance_monthly (
        season,
        year,
        month,
        team_id,
        avg_ball_possession,
        total_shots,
        shots_on_target,
        conversion_rate,
        pass_accuracy,
        fouls_committed,
        updated_at
    )
    SELECT
        season,
        year,
        month,
        team_id,
        avg_ball_possession,
        total_shots,
        shots_on_target,
        conversion_rate,
        pass_accuracy,
        fouls_committed,
        now()
    FROM aggregated
    ON CONFLICT (season, year, month, team_id) DO UPDATE
    SET
        avg_ball_possession = EXCLUDED.avg_ball_possession,
        total_shots = EXCLUDED.total_shots,
        shots_on_target = EXCLUDED.shots_on_target,
        conversion_rate = EXCLUDED.conversion_rate,
        pass_accuracy = EXCLUDED.pass_accuracy,
        fouls_committed = EXCLUDED.fouls_committed,
        updated_at = now()
    WHERE mart.team_performance_monthly.avg_ball_possession IS DISTINCT FROM EXCLUDED.avg_ball_possession
       OR mart.team_performance_monthly.total_shots IS DISTINCT FROM EXCLUDED.total_shots
       OR mart.team_performance_monthly.shots_on_target IS DISTINCT FROM EXCLUDED.shots_on_target
       OR mart.team_performance_monthly.conversion_rate IS DISTINCT FROM EXCLUDED.conversion_rate
       OR mart.team_performance_monthly.pass_accuracy IS DISTINCT FROM EXCLUDED.pass_accuracy
       OR mart.team_performance_monthly.fouls_committed IS DISTINCT FROM EXCLUDED.fouls_committed
    RETURNING (xmax = 0) AS inserted
)
SELECT
    COALESCE(SUM(CASE WHEN inserted THEN 1 ELSE 0 END), 0)::INT AS inserted,
    COALESCE(SUM(CASE WHEN NOT inserted THEN 1 ELSE 0 END), 0)::INT AS updated
FROM upserted;
