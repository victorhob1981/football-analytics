from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from common.observability import DEFAULT_DAG_ARGS, log_event
from common.providers import get_default_league_id, get_default_provider, normalize_provider_name

DEFAULT_SEASON = 2024
DEFAULT_PROVIDER = get_default_provider()
DEFAULT_LEAGUE_ID = get_default_league_id(DEFAULT_PROVIDER)


def _safe_int(value, default_value: int, field_name: str) -> int:
    if value is None:
        return default_value
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Parametro invalido para {field_name}: {value}") from exc


def resolve_params() -> dict:
    context = get_current_context()
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    resolved_provider = normalize_provider_name(str(conf.get("provider", params.get("provider", DEFAULT_PROVIDER))))
    provider_default_league = get_default_league_id(resolved_provider)

    resolved = {
        "league_id": _safe_int(
            conf.get("league_id", params.get("league_id", provider_default_league)),
            provider_default_league,
            "league_id",
        ),
        "season": _safe_int(conf.get("season", params.get("season", DEFAULT_SEASON)), DEFAULT_SEASON, "season"),
        "provider": resolved_provider,
    }

    log_event(
        service="airflow",
        module="pipeline_brasileirao",
        step="resolve_params",
        status="success",
        context=context,
        dataset="pipeline",
        message=f"[pipeline_brasileirao] Params resolvidos: {resolved}",
    )
    return resolved


def log_stage(stage: str, action: str):
    context = get_current_context()
    log_event(
        service="airflow",
        module="pipeline_brasileirao",
        step=stage,
        status="success",
        context=context,
        dataset="pipeline",
        message=f"[pipeline_brasileirao] {action} | stage={stage} | run_id={context.get('run_id')}",
    )


with DAG(
    dag_id="pipeline_brasileirao",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    params={"league_id": DEFAULT_LEAGUE_ID, "season": DEFAULT_SEASON, "provider": DEFAULT_PROVIDER},
    render_template_as_native_obj=True,
    default_args=DEFAULT_DAG_ARGS,
    tags=["pipeline", "orchestrator", "brasileirao"],
) as dag:
    resolve_runtime_params = PythonOperator(
        task_id="resolve_runtime_params",
        python_callable=resolve_params,
        execution_timeout=timedelta(minutes=5),
    )

    def trigger_task(task_id: str, dag_id: str) -> TriggerDagRunOperator:
        return TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id=dag_id,
            conf="{{ ti.xcom_pull(task_ids='resolve_runtime_params') }}",
            wait_for_completion=True,
            poke_interval=10,
            reset_dag_run=True,
            allowed_states=["success"],
            failed_states=["failed"],
            execution_timeout=timedelta(minutes=120),
        )

    with TaskGroup(group_id="group_competition_structure") as group_competition_structure:
        run_ingest_competition_structure = trigger_task(
            "run_ingest_competition_structure_bronze",
            "ingest_competition_structure_bronze",
        )
        run_bronze_to_silver_competition_structure = trigger_task(
            "run_bronze_to_silver_competition_structure",
            "bronze_to_silver_competition_structure",
        )
        run_silver_to_postgres_competition_structure = trigger_task(
            "run_silver_to_postgres_competition_structure",
            "silver_to_postgres_competition_structure",
        )
        run_ingest_competition_structure >> run_bronze_to_silver_competition_structure >> run_silver_to_postgres_competition_structure

        run_ingest_standings = trigger_task("run_ingest_standings_bronze", "ingest_standings_bronze")
        run_bronze_to_silver_standings = trigger_task("run_bronze_to_silver_standings", "bronze_to_silver_standings")
        run_silver_to_postgres_standings = trigger_task("run_silver_to_postgres_standings", "silver_to_postgres_standings")
        run_silver_to_postgres_competition_structure >> run_ingest_standings
        run_ingest_standings >> run_bronze_to_silver_standings >> run_silver_to_postgres_standings

    with TaskGroup(group_id="group_fixture_enrichment") as group_fixture_enrichment:
        run_ingest_fixtures = trigger_task("run_ingest_brasileirao_2024_backfill", "ingest_brasileirao_2024_backfill")
        run_bronze_to_silver_fixtures = trigger_task("run_bronze_to_silver_fixtures_backfill", "bronze_to_silver_fixtures_backfill")
        run_silver_to_postgres_fixtures = trigger_task("run_silver_to_postgres_fixtures", "silver_to_postgres_fixtures")
        run_ingest_fixtures >> run_bronze_to_silver_fixtures >> run_silver_to_postgres_fixtures

        run_ingest_statistics = trigger_task("run_ingest_statistics_bronze", "ingest_statistics_bronze")
        run_bronze_to_silver_statistics = trigger_task("run_bronze_to_silver_statistics", "bronze_to_silver_statistics")
        run_silver_to_postgres_statistics = trigger_task("run_silver_to_postgres_statistics", "silver_to_postgres_statistics")
        run_silver_to_postgres_fixtures >> run_ingest_statistics
        run_ingest_statistics >> run_bronze_to_silver_statistics >> run_silver_to_postgres_statistics

        run_ingest_match_events = trigger_task("run_ingest_match_events_bronze", "ingest_match_events_bronze")
        run_bronze_to_silver_match_events = trigger_task("run_bronze_to_silver_match_events", "bronze_to_silver_match_events")
        run_silver_to_postgres_match_events = trigger_task("run_silver_to_postgres_match_events", "silver_to_postgres_match_events")
        run_silver_to_postgres_fixtures >> run_ingest_match_events
        run_ingest_match_events >> run_bronze_to_silver_match_events >> run_silver_to_postgres_match_events

    with TaskGroup(group_id="group_player_layer") as group_player_layer:
        run_ingest_lineups = trigger_task("run_ingest_lineups_bronze", "ingest_lineups_bronze")
        run_bronze_to_silver_lineups = trigger_task("run_bronze_to_silver_lineups", "bronze_to_silver_lineups")
        run_silver_to_postgres_lineups = trigger_task("run_silver_to_postgres_lineups", "silver_to_postgres_lineups")
        run_ingest_lineups >> run_bronze_to_silver_lineups >> run_silver_to_postgres_lineups

        run_ingest_fixture_player_statistics = trigger_task(
            "run_ingest_fixture_player_statistics_bronze",
            "ingest_fixture_player_statistics_bronze",
        )
        run_bronze_to_silver_fixture_player_statistics = trigger_task(
            "run_bronze_to_silver_fixture_player_statistics",
            "bronze_to_silver_fixture_player_statistics",
        )
        run_silver_to_postgres_fixture_player_statistics = trigger_task(
            "run_silver_to_postgres_fixture_player_statistics",
            "silver_to_postgres_fixture_player_statistics",
        )
        run_ingest_fixture_player_statistics >> run_bronze_to_silver_fixture_player_statistics >> run_silver_to_postgres_fixture_player_statistics

        run_ingest_player_season_statistics = trigger_task(
            "run_ingest_player_season_statistics_bronze",
            "ingest_player_season_statistics_bronze",
        )
        run_bronze_to_silver_player_season_statistics = trigger_task(
            "run_bronze_to_silver_player_season_statistics",
            "bronze_to_silver_player_season_statistics",
        )
        run_silver_to_postgres_player_season_statistics = trigger_task(
            "run_silver_to_postgres_player_season_statistics",
            "silver_to_postgres_player_season_statistics",
        )
        run_silver_to_postgres_lineups >> run_ingest_player_season_statistics
        run_ingest_player_season_statistics >> run_bronze_to_silver_player_season_statistics >> run_silver_to_postgres_player_season_statistics

    with TaskGroup(group_id="group_context_extras") as group_context_extras:
        run_ingest_player_transfers = trigger_task("run_ingest_player_transfers_bronze", "ingest_player_transfers_bronze")
        run_bronze_to_silver_player_transfers = trigger_task("run_bronze_to_silver_player_transfers", "bronze_to_silver_player_transfers")
        run_silver_to_postgres_player_transfers = trigger_task("run_silver_to_postgres_player_transfers", "silver_to_postgres_player_transfers")
        run_ingest_player_transfers >> run_bronze_to_silver_player_transfers >> run_silver_to_postgres_player_transfers

        run_ingest_team_sidelined = trigger_task("run_ingest_team_sidelined_bronze", "ingest_team_sidelined_bronze")
        run_bronze_to_silver_team_sidelined = trigger_task("run_bronze_to_silver_team_sidelined", "bronze_to_silver_team_sidelined")
        run_silver_to_postgres_team_sidelined = trigger_task("run_silver_to_postgres_team_sidelined", "silver_to_postgres_team_sidelined")
        run_ingest_team_sidelined >> run_bronze_to_silver_team_sidelined >> run_silver_to_postgres_team_sidelined

        run_ingest_team_coaches = trigger_task("run_ingest_team_coaches_bronze", "ingest_team_coaches_bronze")
        run_bronze_to_silver_team_coaches = trigger_task("run_bronze_to_silver_team_coaches", "bronze_to_silver_team_coaches")
        run_silver_to_postgres_team_coaches = trigger_task("run_silver_to_postgres_team_coaches", "silver_to_postgres_team_coaches")
        run_ingest_team_coaches >> run_bronze_to_silver_team_coaches >> run_silver_to_postgres_team_coaches

        run_ingest_head_to_head = trigger_task("run_ingest_head_to_head_bronze", "ingest_head_to_head_bronze")
        run_bronze_to_silver_head_to_head = trigger_task("run_bronze_to_silver_head_to_head", "bronze_to_silver_head_to_head")
        run_silver_to_postgres_head_to_head = trigger_task("run_silver_to_postgres_head_to_head", "silver_to_postgres_head_to_head")
        run_ingest_head_to_head >> run_bronze_to_silver_head_to_head >> run_silver_to_postgres_head_to_head

    with TaskGroup(group_id="group_dbt_quality") as group_dbt_quality:
        run_dbt = trigger_task("run_dbt", "dbt_run")
        run_ge = trigger_task("run_great_expectations_checks", "great_expectations_checks")
        run_quality = trigger_task("run_data_quality_checks", "data_quality_checks")
        run_dbt >> run_ge >> run_quality

    start_pipeline = PythonOperator(
        task_id="start_pipeline",
        python_callable=log_stage,
        op_kwargs={"stage": "pipeline_brasileirao", "action": "START"},
        execution_timeout=timedelta(minutes=5),
    )
    end_pipeline = PythonOperator(
        task_id="end_pipeline",
        python_callable=log_stage,
        op_kwargs={"stage": "pipeline_brasileirao", "action": "END"},
        execution_timeout=timedelta(minutes=5),
    )

    resolve_runtime_params >> start_pipeline
    start_pipeline >> group_competition_structure
    group_competition_structure >> group_fixture_enrichment
    group_fixture_enrichment >> group_player_layer
    group_player_layer >> group_context_extras
    group_context_extras >> group_dbt_quality
    group_dbt_quality >> end_pipeline
