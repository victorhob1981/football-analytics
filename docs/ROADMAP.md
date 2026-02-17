# Roadmap - football-analytics

Status date: 2026-02-17

## Phase 0 - Baseline stack and fixtures pipeline
Status: Done

Definition of Done:
- `docker compose up -d` sobe Postgres, MinIO, Airflow, Metabase.
- Fixtures pipeline roda ponta a ponta sem duplicacao.
- Reexecucao mantem comportamento idempotente.

Evidence:
- DAGs: `ingest_brasileirao_2024_backfill`, `bronze_to_silver_fixtures_backfill`, `silver_to_postgres_fixtures`
- Compose services: `docker-compose.yml`
- Idempotent load logic: `infra/airflow/dags/silver_to_postgres_fixtures.py`

## Phase 1 - Consolidacao Raw (fixtures + statistics)
Status: Done

Definition of Done:
- `raw.match_statistics` com PK composta e carga idempotente.
- Contrato explicito de colunas no loader.
- FK de `raw.match_statistics.fixture_id` para `raw.fixtures.fixture_id` com rollout seguro.

Evidence:
- Migrations: `db/migrations/20260217120000_baseline_schema.sql`, `db/migrations/20260217121000_raw_statistics_fixture_fk_not_valid.sql`
- DAGs: `bronze_to_silver_statistics`, `silver_to_postgres_statistics`

## Phase 2 - Event-level analytics
Status: Done

Definition of Done:
- Ingestao de eventos finalizados para Bronze.
- Silver normalizado com surrogate `event_id` e parquet particionado.
- Carga Raw particionada por `season` com upsert idempotente.

Evidence:
- DAGs: `ingest_match_events_bronze`, `bronze_to_silver_match_events`, `silver_to_postgres_match_events`
- Particao raw: `raw.match_events` (`PARTITION BY LIST (season)`) em migration baseline

## Phase 3 - Modelo dimensional (Gold/Mart via dbt)
Status: Done (engine migrado para dbt)

Definition of Done:
- Dimensoes e fatos modelados em dbt.
- Marts analiticos modelados em dbt.
- DAGs legacy Gold/Mart removidas do fluxo principal e marcadas como deprecated.

Evidence:
- dbt models: `dbt/models/marts/core/*.sql`, `dbt/models/marts/analytics/*.sql`
- DAG principal usa `dbt_run`: `infra/airflow/dags/pipeline_brasileirao.py`
- DAGs legacy: `deprecated_gold_dimensions_load`, `deprecated_gold_facts_load`, `deprecated_mart_build_brasileirao_2024`

## Phase 4 - Data quality formal
Status: Done

Definition of Done:
- Gate de qualidade obrigatorio no orquestrador.
- GE e SQL checks executam em sequencia.
- Falha em quality quebra a pipeline.

Evidence:
- DAGs: `great_expectations_checks`, `data_quality_checks`
- GE suites: `quality/great_expectations/expectations/*.json`
- Encadeamento no pipeline: `infra/airflow/dags/pipeline_brasileirao.py`

## Phase 5 - Orquestracao avancada
Status: Done

Definition of Done:
- DAG unica de orquestracao com grupos claros de etapa.
- Dependencias formais entre ingestao, raw, transformacao e quality.
- Parametros `league_id` e `season` propagados por `conf`.

Evidence:
- DAG: `infra/airflow/dags/pipeline_brasileirao.py`
- `TriggerDagRunOperator(wait_for_completion=True)` em todos os gatilhos

## Phase 6 - Adocao de dbt
Status: Done

Definition of Done:
- Projeto dbt estruturado com `stg -> int -> marts`.
- `dbt run` e `dbt test` integrados no Airflow.
- Documentacao dbt gerada local e no CI.

Evidence:
- Estrutura: `dbt/models/staging`, `dbt/models/intermediate`, `dbt/models/marts`
- DAG: `infra/airflow/dags/dbt_run.py`
- CI: `.github/workflows/ci.yml` (deps/compile/docs artifact)

## Phase 7 - Performance e escalabilidade
Status: Done (baseline)

Definition of Done:
- Indices compostos adicionados para filtros de BI.
- `ANALYZE` automatico apos dbt run.
- Facts com incremental configurado.

Evidence:
- Migration: `db/migrations/20260217122000_optimization_indexes.sql`
- DAG dbt: task `analyze_db` em `infra/airflow/dags/dbt_run.py`
- Incremental: `dbt/models/marts/core/fact_matches.sql`, `dbt/models/marts/core/fact_match_events.sql`

## Phase 8 - BI layer
Status: Done (infra + versionamento)

Definition of Done:
- Metabase sobe no compose e persiste metadados em volume.
- Processo de export/import de dashboards versionado no repo.
- Conexao ao DW documentada.

Evidence:
- Service: `metabase` em `docker-compose.yml`
- Scripts: `bi/metabase/scripts/export_metabase.py`, `bi/metabase/scripts/import_metabase.py`
- Guia: `bi/metabase/README.md`

## Phase 9 - Engenharia de producao
Status: In progress

Definition of Done (target):
- CI completo em PR/push para lint/test/dbt compile.
- Testes de DAG parsing e checks SQL estaveis.
- Contratos e metricas operacionais documentados e auditaveis.

Done now:
- CI workflow existente: `.github/workflows/ci.yml`
- Testes: `tests/test_airflow_dags.py`, `tests/test_dbt_compile.py`, `tests/test_sql_assets.py`
- Contratos: `docs/contracts/data_contracts.md`

Remaining planned:
- Publicacao automatica opcional de dbt docs (ex.: GitHub Pages)
- Expansao de cobertura de testes (dbt test em profile CI com dataset de fixture controlado)
- Hardening final de operacao (playbooks de incidente/rollback e SLO formal)
