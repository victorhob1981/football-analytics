from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from sqlalchemy import create_engine, text

from common.observability import DEFAULT_DAG_ARGS, StepMetrics, log_event


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria ausente: {name}")
    return value


CHECKS = [
    {
        "check_name": "raw_fixtures_null_pk",
        "description": "raw.fixtures possui fixture_id nulo.",
        "sql": """
            SELECT *
            FROM raw.fixtures
            WHERE fixture_id IS NULL
        """,
    },
    {
        "check_name": "raw_match_statistics_null_pk",
        "description": "raw.match_statistics possui fixture_id ou team_id nulos.",
        "sql": """
            SELECT *
            FROM raw.match_statistics
            WHERE fixture_id IS NULL
               OR team_id IS NULL
        """,
    },
    {
        "check_name": "raw_events_orphan",
        "description": "raw.match_events possui eventos com fixture_id inexistente em raw.fixtures.",
        "sql": """
            SELECT e.*
            FROM raw.match_events e
            LEFT JOIN raw.fixtures f
              ON e.fixture_id = f.fixture_id
            WHERE f.fixture_id IS NULL
        """,
    },
    {
        "check_name": "mart_fact_matches_no_date",
        "description": "mart.fact_matches possui date_day nulo.",
        "sql": """
            SELECT *
            FROM mart.fact_matches
            WHERE date_day IS NULL
        """,
    },
    {
        "check_name": "mart_score_mismatch",
        "description": "mart.league_summary possui total_matches > 0 com total_goals = 0.",
        "sql": """
            SELECT *
            FROM mart.league_summary
            WHERE total_matches > 0
              AND total_goals = 0
        """,
    },
    {
        "check_name": "raw_fixture_lineups_duplicate_grain",
        "description": "raw.fixture_lineups possui duplicidade no grain provider/fixture_id/team_id/lineup_id.",
        "sql": """
            SELECT provider, fixture_id, team_id, lineup_id, COUNT(*) AS duplicate_rows
            FROM raw.fixture_lineups
            GROUP BY provider, fixture_id, team_id, lineup_id
            HAVING COUNT(*) > 1
        """,
    },
    {
        "check_name": "raw_fixture_player_statistics_duplicate_grain",
        "description": "raw.fixture_player_statistics possui duplicidade no grain provider/fixture_id/team_id/player_id.",
        "sql": """
            SELECT provider, fixture_id, team_id, player_id, COUNT(*) AS duplicate_rows
            FROM raw.fixture_player_statistics
            GROUP BY provider, fixture_id, team_id, player_id
            HAVING COUNT(*) > 1
        """,
    },
    {
        "check_name": "raw_standings_snapshots_duplicate_grain",
        "description": "raw.standings_snapshots possui duplicidade no grain provider/season_id/stage_id/round_id/team_id.",
        "sql": """
            SELECT provider, season_id, stage_id, round_id, team_id, COUNT(*) AS duplicate_rows
            FROM raw.standings_snapshots
            GROUP BY provider, season_id, stage_id, round_id, team_id
            HAVING COUNT(*) > 1
        """,
    },
    {
        "check_name": "raw_fixture_player_statistics_orphan_fixture",
        "description": "raw.fixture_player_statistics possui fixture_id inexistente em raw.fixtures.",
        "sql": """
            SELECT s.*
            FROM raw.fixture_player_statistics s
            LEFT JOIN raw.fixtures f
              ON f.fixture_id = s.fixture_id
            WHERE f.fixture_id IS NULL
        """,
    },
    {
        "check_name": "raw_fixture_lineups_min_starters",
        "description": "Fixtures finalizadas com lineup carregada devem ter ao menos 11 titulares por time.",
        "sql": """
            WITH fixtures_with_lineups AS (
                SELECT DISTINCT l.fixture_id
                FROM raw.fixture_lineups l
            ),
            final_fixtures AS (
                SELECT f.fixture_id, f.home_team_id, f.away_team_id
                FROM raw.fixtures f
                JOIN fixtures_with_lineups fl
                  ON fl.fixture_id = f.fixture_id
                WHERE f.status_short IN ('FT', 'AET', 'PEN')
            ),
            expected_teams AS (
                SELECT fixture_id, home_team_id AS team_id FROM final_fixtures
                UNION ALL
                SELECT fixture_id, away_team_id AS team_id FROM final_fixtures
            ),
            starters AS (
                SELECT fixture_id, team_id, COUNT(*) AS starters
                FROM raw.fixture_lineups
                WHERE lineup_type_id IN (1, 11)
                GROUP BY fixture_id, team_id
            )
            SELECT e.fixture_id, e.team_id, COALESCE(s.starters, 0) AS starters
            FROM expected_teams e
            LEFT JOIN starters s
              ON s.fixture_id = e.fixture_id
             AND s.team_id = e.team_id
            WHERE COALESCE(s.starters, 0) < 11
        """,
    },
]


def run_data_quality_checks():
    context = get_current_context()
    engine = create_engine(_get_required_env("FOOTBALL_PG_DSN"))
    failed = []

    with StepMetrics(
        service="airflow",
        module="data_quality_checks",
        step="run_data_quality_checks",
        context=context,
        dataset="quality.sql_assertions",
    ) as metric:
        with engine.begin() as conn:
            for check in CHECKS:
                rows = conn.execute(text(check["sql"])).fetchall()
                bad_count = len(rows)

                log_event(
                    level="info" if bad_count == 0 else "error",
                    service="airflow",
                    module="data_quality_checks",
                    step=check["check_name"],
                    status="success" if bad_count == 0 else "failed",
                    context=context,
                    dataset="quality.sql_assertions",
                    row_count=bad_count,
                    message=(
                        f"[DQ {'PASS' if bad_count == 0 else 'FAIL'}] "
                        f"check={check['check_name']} | bad_rows={bad_count} | description={check['description']}"
                    ),
                )

                if bad_count > 0:
                    failed.append((check["check_name"], bad_count, check["description"]))

        metric.set_counts(rows_in=len(CHECKS), rows_out=len(CHECKS) - len(failed), row_count=len(CHECKS))

    if failed:
        summary = "; ".join([f"{name}(bad_rows={count}): {desc}" for name, count, desc in failed])
        raise ValueError(f"Data quality checks falharam: {summary}")


with DAG(
    dag_id="data_quality_checks",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_DAG_ARGS,
    tags=["quality", "validation", "warehouse"],
) as dag:
    PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks,
        execution_timeout=timedelta(minutes=10),
    )
