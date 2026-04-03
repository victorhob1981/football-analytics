from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from sqlalchemy import create_engine, text

from common.fixture_status import FINAL_STATUSES_SQL
from common.observability import DEFAULT_DAG_ARGS, StepMetrics, log_event


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria ausente: {name}")
    return value


LINEUPS_MIN_STARTERS_PROVIDER_CAVEATS = [
    {
        "fixture_id": 18809781,
        "team_id": 6188,
        "reason": (
            "Provider caveat: copa_do_brasil/2023 fixture 18809781 returned HTTP 200 and results=45, "
            "but only 10 starters for team_id=6188."
        ),
    }
]


def _lineups_min_starters_provider_caveats_sql() -> str:
    values = ",\n                ".join(
        (
            f"({caveat['fixture_id']}::bigint, {caveat['team_id']}::bigint, "
            f"'{caveat['reason']}')"
        )
        for caveat in LINEUPS_MIN_STARTERS_PROVIDER_CAVEATS
    )
    return (
        "SELECT *\n"
        "            FROM (VALUES\n"
        f"                {values}\n"
        "            ) AS t(fixture_id, team_id, caveat_reason)"
    )


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
        "check_name": "raw_fixtures_null_competition_key",
        "description": "raw.fixtures possui competition_key nulo.",
        "sql": """
            SELECT *
            FROM raw.fixtures
            WHERE competition_key IS NULL
        """,
    },
    {
        "check_name": "raw_fixtures_null_season_label",
        "description": "raw.fixtures possui season_label nulo.",
        "sql": """
            SELECT *
            FROM raw.fixtures
            WHERE season_label IS NULL
        """,
    },
    {
        "check_name": "raw_fixtures_null_provider_season_id",
        "description": "raw.fixtures possui provider_season_id nulo.",
        "sql": """
            SELECT *
            FROM raw.fixtures
            WHERE provider_season_id IS NULL
        """,
    },
    {
        "check_name": "raw_fixtures_outside_catalog",
        "description": "raw.fixtures possui linhas sem match no control.season_catalog.",
        "sql": """
            SELECT fixtures.*
            FROM raw.fixtures fixtures
            LEFT JOIN control.season_catalog sc
              ON sc.provider = fixtures.source_provider
             AND sc.competition_key = fixtures.competition_key
             AND sc.season_label = fixtures.season_label
             AND sc.provider_season_id = fixtures.provider_season_id
            WHERE fixtures.competition_key IS NOT NULL
              AND fixtures.season_label IS NOT NULL
              AND fixtures.provider_season_id IS NOT NULL
              AND sc.provider IS NULL
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
        "check_name": "raw_match_statistics_null_competition_key",
        "description": "raw.match_statistics possui competition_key nulo.",
        "sql": """
            SELECT *
            FROM raw.match_statistics
            WHERE competition_key IS NULL
        """,
    },
    {
        "check_name": "raw_match_statistics_null_season_label",
        "description": "raw.match_statistics possui season_label nulo.",
        "sql": """
            SELECT *
            FROM raw.match_statistics
            WHERE season_label IS NULL
        """,
    },
    {
        "check_name": "raw_match_statistics_null_provider_season_id",
        "description": "raw.match_statistics possui provider_season_id nulo.",
        "sql": """
            SELECT *
            FROM raw.match_statistics
            WHERE provider_season_id IS NULL
        """,
    },
    {
        "check_name": "raw_match_statistics_outside_catalog",
        "description": "raw.match_statistics possui linhas sem match no control.season_catalog.",
        "sql": """
            SELECT stats.*
            FROM raw.match_statistics stats
            LEFT JOIN control.season_catalog sc
              ON sc.provider = stats.provider
             AND sc.competition_key = stats.competition_key
             AND sc.season_label = stats.season_label
             AND sc.provider_season_id = stats.provider_season_id
            WHERE stats.competition_key IS NOT NULL
              AND stats.season_label IS NOT NULL
              AND stats.provider_season_id IS NOT NULL
              AND sc.provider IS NULL
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
        "check_name": "raw_fixture_lineups_null_competition_key",
        "description": "raw.fixture_lineups possui competition_key nulo.",
        "sql": """
            SELECT *
            FROM raw.fixture_lineups
            WHERE competition_key IS NULL
        """,
    },
    {
        "check_name": "raw_fixture_lineups_null_season_label",
        "description": "raw.fixture_lineups possui season_label nulo.",
        "sql": """
            SELECT *
            FROM raw.fixture_lineups
            WHERE season_label IS NULL
        """,
    },
    {
        "check_name": "raw_fixture_lineups_null_provider_season_id",
        "description": "raw.fixture_lineups possui provider_season_id nulo.",
        "sql": """
            SELECT *
            FROM raw.fixture_lineups
            WHERE provider_season_id IS NULL
        """,
    },
    {
        "check_name": "raw_fixture_lineups_outside_catalog",
        "description": "raw.fixture_lineups possui linhas sem match no control.season_catalog.",
        "sql": """
            SELECT lineups.*
            FROM raw.fixture_lineups lineups
            LEFT JOIN control.season_catalog sc
              ON sc.provider = lineups.provider
             AND sc.competition_key = lineups.competition_key
             AND sc.season_label = lineups.season_label
             AND sc.provider_season_id = lineups.provider_season_id
            WHERE lineups.competition_key IS NOT NULL
              AND lineups.season_label IS NOT NULL
              AND lineups.provider_season_id IS NOT NULL
              AND sc.provider IS NULL
        """,
    },
    {
        "check_name": "raw_fixture_lineups_team_scope_mismatch",
        "description": "raw.fixture_lineups possui team_id fora do par autoritativo de raw.fixtures.",
        "sql": """
            SELECT l.*
            FROM raw.fixture_lineups l
            JOIN raw.fixtures f
              ON f.fixture_id = l.fixture_id
            WHERE l.team_id IS DISTINCT FROM f.home_team_id
              AND l.team_id IS DISTINCT FROM f.away_team_id
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
        "check_name": "raw_head_to_head_orphan_fixture",
        "description": "raw.head_to_head_fixtures possui fixture_id inexistente em raw.fixtures.",
        "sql": """
            SELECT h.*
            FROM raw.head_to_head_fixtures h
            LEFT JOIN raw.fixtures f
              ON f.fixture_id = h.fixture_id
            WHERE f.fixture_id IS NULL
        """,
    },
    {
        "check_name": "raw_head_to_head_null_competition_key",
        "description": "raw.head_to_head_fixtures possui competition_key nulo.",
        "sql": """
            SELECT *
            FROM raw.head_to_head_fixtures
            WHERE competition_key IS NULL
        """,
    },
    {
        "check_name": "raw_head_to_head_null_season_label",
        "description": "raw.head_to_head_fixtures possui season_label nulo.",
        "sql": """
            SELECT *
            FROM raw.head_to_head_fixtures
            WHERE season_label IS NULL
        """,
    },
    {
        "check_name": "raw_head_to_head_null_provider_season_id",
        "description": "raw.head_to_head_fixtures possui provider_season_id nulo.",
        "sql": """
            SELECT *
            FROM raw.head_to_head_fixtures
            WHERE provider_season_id IS NULL
        """,
    },
    {
        "check_name": "raw_head_to_head_outside_catalog",
        "description": "raw.head_to_head_fixtures possui linhas sem match no control.season_catalog.",
        "sql": """
            SELECT h.*
            FROM raw.head_to_head_fixtures h
            LEFT JOIN control.season_catalog sc
              ON sc.provider = h.provider
             AND sc.competition_key = h.competition_key
             AND sc.season_label = h.season_label
             AND sc.provider_season_id = h.provider_season_id
            WHERE h.competition_key IS NOT NULL
              AND h.season_label IS NOT NULL
              AND h.provider_season_id IS NOT NULL
              AND sc.provider IS NULL
        """,
    },
    {
        "check_name": "raw_head_to_head_fixture_scope_mismatch",
        "description": "raw.head_to_head_fixtures diverge do fixture autoritativo no raw.fixtures.",
        "sql": """
            SELECT h.*
            FROM raw.head_to_head_fixtures h
            JOIN raw.fixtures f
              ON f.fixture_id = h.fixture_id
            WHERE f.source_provider IS DISTINCT FROM h.provider
               OR f.league_id IS DISTINCT FROM h.provider_league_id
               OR f.competition_key IS DISTINCT FROM h.competition_key
               OR f.season_label IS DISTINCT FROM h.season_label
               OR f.provider_season_id IS DISTINCT FROM h.provider_season_id
               OR f.provider_season_id IS DISTINCT FROM h.season_id
               OR f.date_utc::date IS DISTINCT FROM h.match_date::date
               OR LEAST(f.home_team_id, f.away_team_id) IS DISTINCT FROM h.pair_team_id
               OR GREATEST(f.home_team_id, f.away_team_id) IS DISTINCT FROM h.pair_opponent_id
        """,
    },
    {
        "check_name": "raw_player_season_statistics_null_competition_key",
        "description": "raw.player_season_statistics possui competition_key nulo.",
        "sql": """
            SELECT *
            FROM raw.player_season_statistics
            WHERE competition_key IS NULL
        """,
    },
    {
        "check_name": "raw_player_season_statistics_null_season_label",
        "description": "raw.player_season_statistics possui season_label nulo.",
        "sql": """
            SELECT *
            FROM raw.player_season_statistics
            WHERE season_label IS NULL
        """,
    },
    {
        "check_name": "raw_player_season_statistics_null_provider_season_id",
        "description": "raw.player_season_statistics possui provider_season_id nulo.",
        "sql": """
            SELECT *
            FROM raw.player_season_statistics
            WHERE provider_season_id IS NULL
        """,
    },
    {
        "check_name": "raw_player_season_statistics_outside_catalog",
        "description": "raw.player_season_statistics possui linhas sem match no control.season_catalog.",
        "sql": """
            SELECT pss.*
            FROM raw.player_season_statistics pss
            LEFT JOIN control.season_catalog sc
              ON sc.provider = pss.provider
             AND sc.competition_key = pss.competition_key
             AND sc.season_label = pss.season_label
             AND sc.provider_season_id = pss.provider_season_id
            WHERE pss.competition_key IS NOT NULL
              AND pss.season_label IS NOT NULL
              AND pss.provider_season_id IS NOT NULL
              AND sc.provider IS NULL
        """,
    },
    {
        "check_name": "raw_player_season_statistics_semantically_invisible_scope",
        "description": "raw.player_season_statistics possui escopos carregados fisicamente, mas sem identidade semantica completa.",
        "sql": """
            SELECT provider, league_id, season_id, COUNT(*) AS bad_rows
            FROM raw.player_season_statistics
            WHERE league_id IS NOT NULL
              AND season_id IS NOT NULL
              AND (
                    competition_key IS NULL
                 OR season_label IS NULL
                 OR provider_season_id IS NULL
              )
            GROUP BY provider, league_id, season_id
        """,
    },
    {
        "check_name": "raw_fixture_lineups_min_starters",
        "description": "Fixtures finalizadas com lineup carregada devem ter ao menos 11 titulares por time.",
        "sql": f"""
            WITH fixtures_with_lineups AS (
                SELECT DISTINCT l.fixture_id
                FROM raw.fixture_lineups l
            ),
            final_fixtures AS (
                SELECT f.fixture_id, f.home_team_id, f.away_team_id
                FROM raw.fixtures f
                JOIN fixtures_with_lineups fl
                  ON fl.fixture_id = f.fixture_id
                WHERE f.status_short IN ({FINAL_STATUSES_SQL})
            ),
            expected_teams AS (
                SELECT fixture_id, home_team_id AS team_id FROM final_fixtures
                UNION ALL
                SELECT fixture_id, away_team_id AS team_id FROM final_fixtures
            ),
            provider_caveat_exceptions AS (
                {_lineups_min_starters_provider_caveats_sql()}
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
            LEFT JOIN provider_caveat_exceptions exc
              ON exc.fixture_id = e.fixture_id
             AND exc.team_id = e.team_id
            WHERE COALESCE(s.starters, 0) < 11
              AND exc.fixture_id IS NULL
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
