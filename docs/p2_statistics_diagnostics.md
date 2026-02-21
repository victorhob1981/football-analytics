# P2.1 Statistics Diagnostics

## Grain canonico de statistics

No projeto atual, o grain canonico de `raw.match_statistics` e:

- `fixture_id`
- `team_id`

Isto representa **1 linha por time por partida** (formato pivotado de metricas).

Evidencias no codigo:
- DDL: `warehouse/ddl/002_raw_statistics.sql` define `PRIMARY KEY (fixture_id, team_id)`.
- Migracoes: `db/migrations/20260217120000_baseline_schema.sql` e `db/migrations/20260217121000_raw_statistics_fixture_fk_not_valid.sql`.
- Mapper: `infra/airflow/dags/common/mappers/statistics_mapper.py` faz dedupe por `["fixture_id", "team_id"]`.
- Loader raw: `infra/airflow/dags/common/services/warehouse_service.py` faz `ON CONFLICT (fixture_id, team_id)`.
- dbt staging: `dbt/models/staging/stg_match_statistics.sql` consome a tabela neste mesmo formato.

Como o modelo e pivotado (colunas como `shots_on_goal`, `fouls`, etc.), **`stat_type` nao faz parte do grain final**.

## Queries de diagnostico

Arquivos:
- `warehouse/queries/fixtures_missing_stats.sql`
- `warehouse/queries/stats_duplicates.sql`
- `warehouse/queries/coverage_by_season.sql`

### Como executar (PowerShell)

```powershell
Get-Content 'warehouse/queries/fixtures_missing_stats.sql' | docker compose exec -T postgres psql -U football -d football_dw
Get-Content 'warehouse/queries/stats_duplicates.sql' | docker compose exec -T postgres psql -U football -d football_dw
Get-Content 'warehouse/queries/coverage_by_season.sql' | docker compose exec -T postgres psql -U football -d football_dw
```

## Interpretacao

- `fixtures_missing_stats.sql`:
  - Cada `fixture_id` retornado e gap de cobertura.
  - Esta lista e a base para backfill em `ingest_statistics_bronze`.

- `stats_duplicates.sql`:
  - Resultado esperado: zero linhas.
  - Se houver linhas, existe quebra do grain `(fixture_id, team_id)`.

- `coverage_by_season.sql`:
  - `pct_with_any_stats`: cobertura minima (ao menos 1 time com stats).
  - `pct_with_two_teams_stats`: cobertura completa por fixture (mandante + visitante).
