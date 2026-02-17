from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from datetime import datetime
import os
from sqlalchemy import create_engine, text


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria ausente: {name}")
    return value


def _assert_mart_objects(conn):
    schema_exists = conn.execute(
        text("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'mart')")
    ).scalar_one()
    if not schema_exists:
        raise ValueError("Schema mart nao existe. Aplique warehouse/ddl/010_mart_schema.sql.")

    required_tables = {"team_match_goals_monthly", "league_summary"}
    found_tables = {
        row[0]
        for row in conn.execute(
            text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'mart'
                """
            )
        )
    }
    missing_tables = sorted(required_tables - found_tables)
    if missing_tables:
        raise ValueError(
            f"Tabelas mart ausentes: {missing_tables}. "
            "Aplique warehouse/ddl/011_mart_tables.sql."
        )

    required_team_cols = {"points", "goal_diff"}
    found_team_cols = {
        row[0]
        for row in conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'mart' AND table_name = 'team_match_goals_monthly'
                """
            )
        )
    }
    missing_team_cols = sorted(required_team_cols - found_team_cols)
    if missing_team_cols:
        raise ValueError(
            f"Colunas mart.team_match_goals_monthly ausentes: {missing_team_cols}. "
            "Aplique warehouse/ddl/011_mart_tables.sql atualizado."
        )


def _safe_int(value, default_value: int, field_name: str) -> int:
    if value is None:
        return default_value
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Parametro invalido para {field_name}: {value}") from exc


def _read_run_params() -> tuple[int, int]:
    context = get_current_context()
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    league_id = _safe_int(conf.get("league_id", params.get("league_id", 71)), 71, "league_id")
    season = _safe_int(conf.get("season", params.get("season", 2024)), 2024, "season")
    return league_id, season


def build_mart():
    engine = create_engine(_get_required_env("FOOTBALL_PG_DSN"))
    league_id, season = _read_run_params()

    team_monthly_sql = text(
        """
        WITH raw_scope AS (
            SELECT
                season,
                year,
                month,
                home_team_id,
                home_team_name,
                away_team_id,
                away_team_name,
                COALESCE(home_goals, 0) AS home_goals,
                COALESCE(away_goals, 0) AS away_goals
            FROM raw.fixtures
            WHERE league_id = :league_id
              AND season = :season
        ),
        team_rows AS (
            SELECT
                season,
                year,
                month,
                home_team_id AS team_id,
                home_team_name AS team_name,
                home_goals AS goals_for,
                away_goals AS goals_against,
                CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END AS wins,
                CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END AS draws,
                CASE WHEN home_goals < away_goals THEN 1 ELSE 0 END AS losses
            FROM raw_scope
            WHERE home_team_name IS NOT NULL

            UNION ALL

            SELECT
                season,
                year,
                month,
                away_team_id AS team_id,
                away_team_name AS team_name,
                away_goals AS goals_for,
                home_goals AS goals_against,
                CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END AS wins,
                CASE WHEN away_goals = home_goals THEN 1 ELSE 0 END AS draws,
                CASE WHEN away_goals < home_goals THEN 1 ELSE 0 END AS losses
            FROM raw_scope
            WHERE away_team_name IS NOT NULL
        ),
        aggregated AS (
            SELECT
                season,
                year,
                month,
                team_id,
                team_name,
                SUM(goals_for)::INT AS goals_for,
                SUM(goals_against)::INT AS goals_against,
                COUNT(*)::INT AS matches,
                SUM(wins)::INT AS wins,
                SUM(draws)::INT AS draws,
                SUM(losses)::INT AS losses,
                (SUM(wins) * 3 + SUM(draws))::INT AS points,
                (SUM(goals_for) - SUM(goals_against))::INT AS goal_diff
            FROM team_rows
            GROUP BY season, year, month, team_id, team_name
        ),
        upserted AS (
            INSERT INTO mart.team_match_goals_monthly (
                season, year, month, team_id, team_name,
                goals_for, goals_against, matches, wins, draws, losses, points, goal_diff, updated_at
            )
            SELECT
                season, year, month, team_id, team_name,
                goals_for, goals_against, matches, wins, draws, losses, points, goal_diff, now()
            FROM aggregated
            ON CONFLICT (season, year, month, team_name) DO UPDATE
            SET
                team_id = EXCLUDED.team_id,
                goals_for = EXCLUDED.goals_for,
                goals_against = EXCLUDED.goals_against,
                matches = EXCLUDED.matches,
                wins = EXCLUDED.wins,
                draws = EXCLUDED.draws,
                losses = EXCLUDED.losses,
                points = EXCLUDED.points,
                goal_diff = EXCLUDED.goal_diff,
                updated_at = now()
            WHERE mart.team_match_goals_monthly.team_id IS DISTINCT FROM EXCLUDED.team_id
               OR mart.team_match_goals_monthly.goals_for IS DISTINCT FROM EXCLUDED.goals_for
               OR mart.team_match_goals_monthly.goals_against IS DISTINCT FROM EXCLUDED.goals_against
               OR mart.team_match_goals_monthly.matches IS DISTINCT FROM EXCLUDED.matches
               OR mart.team_match_goals_monthly.wins IS DISTINCT FROM EXCLUDED.wins
               OR mart.team_match_goals_monthly.draws IS DISTINCT FROM EXCLUDED.draws
               OR mart.team_match_goals_monthly.losses IS DISTINCT FROM EXCLUDED.losses
               OR mart.team_match_goals_monthly.points IS DISTINCT FROM EXCLUDED.points
               OR mart.team_match_goals_monthly.goal_diff IS DISTINCT FROM EXCLUDED.goal_diff
            RETURNING (xmax = 0) AS inserted
        )
        SELECT
            COALESCE(SUM(CASE WHEN inserted THEN 1 ELSE 0 END), 0)::INT AS inserted,
            COALESCE(SUM(CASE WHEN NOT inserted THEN 1 ELSE 0 END), 0)::INT AS updated
        FROM upserted
        """
    )

    league_summary_sql = text(
        """
        WITH aggregated AS (
            SELECT
                league_id,
                league_name,
                season,
                COUNT(*)::INT AS total_matches,
                SUM(COALESCE(home_goals, 0) + COALESCE(away_goals, 0))::INT AS total_goals,
                ROUND(
                    SUM(COALESCE(home_goals, 0) + COALESCE(away_goals, 0))::NUMERIC
                    / NULLIF(COUNT(*), 0),
                    4
                ) AS avg_goals_per_match,
                MIN(date_utc::date) AS first_match_date,
                MAX(date_utc::date) AS last_match_date
            FROM raw.fixtures
            WHERE league_id = :league_id
              AND season = :season
            GROUP BY league_id, league_name, season
        ),
        upserted AS (
            INSERT INTO mart.league_summary (
                league_id, league_name, season, total_matches, total_goals,
                avg_goals_per_match, first_match_date, last_match_date, updated_at
            )
            SELECT
                league_id, league_name, season, total_matches, total_goals,
                avg_goals_per_match, first_match_date, last_match_date, now()
            FROM aggregated
            ON CONFLICT (league_id, season) DO UPDATE
            SET
                league_name = EXCLUDED.league_name,
                total_matches = EXCLUDED.total_matches,
                total_goals = EXCLUDED.total_goals,
                avg_goals_per_match = EXCLUDED.avg_goals_per_match,
                first_match_date = EXCLUDED.first_match_date,
                last_match_date = EXCLUDED.last_match_date,
                updated_at = now()
            WHERE mart.league_summary.league_name IS DISTINCT FROM EXCLUDED.league_name
               OR mart.league_summary.total_matches IS DISTINCT FROM EXCLUDED.total_matches
               OR mart.league_summary.total_goals IS DISTINCT FROM EXCLUDED.total_goals
               OR mart.league_summary.avg_goals_per_match IS DISTINCT FROM EXCLUDED.avg_goals_per_match
               OR mart.league_summary.first_match_date IS DISTINCT FROM EXCLUDED.first_match_date
               OR mart.league_summary.last_match_date IS DISTINCT FROM EXCLUDED.last_match_date
            RETURNING (xmax = 0) AS inserted
        )
        SELECT
            COALESCE(SUM(CASE WHEN inserted THEN 1 ELSE 0 END), 0)::INT AS inserted,
            COALESCE(SUM(CASE WHEN NOT inserted THEN 1 ELSE 0 END), 0)::INT AS updated
        FROM upserted
        """
    )

    with engine.begin() as conn:
        _assert_mart_objects(conn)

        team_stats = conn.execute(team_monthly_sql, {"league_id": league_id, "season": season}).mappings().one()
        league_stats = conn.execute(league_summary_sql, {"league_id": league_id, "season": season}).mappings().one()

    print(
        "MART build concluido | "
        f"league_id={league_id} | season={season} | "
        f"team_match_goals_monthly: inseridas={team_stats['inserted']}, atualizadas={team_stats['updated']} | "
        f"league_summary: inseridas={league_stats['inserted']}, atualizadas={league_stats['updated']}"
    )


with DAG(
    dag_id="mart_build_brasileirao_2024",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    params={"league_id": 71, "season": 2024},
    tags=["mart", "gold", "warehouse"],
) as dag:
    PythonOperator(
        task_id="build_mart_tables",
        python_callable=build_mart,
    )
