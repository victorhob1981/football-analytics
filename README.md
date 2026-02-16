# football-analytics

## Pipeline (local)
Stack: Airflow + MinIO + Postgres via `docker compose`.

## DDLs
- `warehouse/ddl/001_raw_fixtures.sql`
- `warehouse/ddl/010_mart_schema.sql`
- `warehouse/ddl/011_mart_tables.sql`

## Execucao ponta-a-ponta
1. Suba os servicos:
```bash
docker compose up -d
```
2. Airflow UI: `http://localhost:8080` (`admin` / `admin`).
3. Rode os DAGs nesta ordem:
- `ingest_brasileirao_2024_backfill`
- `bronze_to_silver_fixtures_backfill`
- `silver_to_postgres_fixtures`

## Validacao no Airflow UI
- Abra `silver_to_postgres_fixtures` -> task `load_silver_to_postgres` -> Log.
- Verifique linha final com contadores:
  - `lidas`
  - `validas`
  - `inseridas`
  - `atualizadas`
  - `ignoradas`
  - `invalidas_sem_fixture_id`
  - `duplicadas_no_lote`

Re-run seguro: execute novamente `silver_to_postgres_fixtures`; o esperado e `inseridas=0`, `atualizadas=0` e `ignoradas=validas` quando nao houve mudanca de dados.

## MART (gold) no Postgres
1. Aplicar DDLs (PowerShell):
```powershell
Get-Content 'warehouse/ddl/010_mart_schema.sql' | docker compose exec -T postgres psql -U football -d football_dw
Get-Content 'warehouse/ddl/011_mart_tables.sql' | docker compose exec -T postgres psql -U football -d football_dw
```
2. Rodar DAG do mart:
```bash
docker compose exec -T airflow-webserver airflow dags test mart_build_brasileirao_2024 2026-02-16
```
3. Re-run idempotente: rode o mesmo comando novamente e confira no log `inseridas=0` e `atualizadas=0` quando nao houver mudanca na `raw.fixtures`.

## Validacao no Postgres (psql)
Abra shell:
```bash
docker compose exec -it postgres psql -U football -d football_dw
```

1) Total por mes (2024):
```sql
SELECT year, month, COUNT(*) AS fixtures
FROM raw.fixtures
GROUP BY year, month
ORDER BY year, month;
```

2) Top times por gols marcados:
```sql
SELECT team_name, SUM(goals) AS total_goals
FROM (
  SELECT home_team_name AS team_name, COALESCE(home_goals, 0) AS goals FROM raw.fixtures
  UNION ALL
  SELECT away_team_name AS team_name, COALESCE(away_goals, 0) AS goals FROM raw.fixtures
) t
GROUP BY team_name
ORDER BY total_goals DESC
LIMIT 10;
```

3) Sanidade de unicidade:
```sql
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT fixture_id) AS distinct_fixture_id
FROM raw.fixtures;
```

4) MART mensal por time:
```sql
SELECT season, year, month, team_name, matches, goals_for, goals_against, wins, draws, losses
FROM mart.team_match_goals_monthly
ORDER BY year, month, team_name
LIMIT 20;
```

5) MART resumo da liga:
```sql
SELECT league_id, league_name, season, total_matches, total_goals, avg_goals_per_match, first_match_date, last_match_date
FROM mart.league_summary;
```
