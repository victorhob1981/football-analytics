# AGENTS.md — Coding Agent Instructions (football-analytics)

You are a coding agent working inside this repository.

## Mission
Build a professional end-to-end data platform for football analytics (Brazilian leagues), evolving incrementally and safely:
Bronze (MinIO) -> Silver (Parquet) -> Raw (Postgres) -> Mart (Postgres) -> BI (Metabase).
Prefer correctness, idempotency, and reviewable diffs.

## Hard Rules (non-negotiables)
- No hacks, no hidden side effects. Keep changes minimal and explicit.
- Never commit secrets or print tokens/credentials in logs.
- Prefer idempotent pipelines (safe to re-run).
- Prefer schema contracts: explicit columns and type normalization.
- Always provide PowerShell-friendly commands when suggesting local execution (Windows user).
- If a command differs between Bash and PowerShell, provide both OR default to PowerShell.

## Current Stack (as implemented)
- Orchestration: Airflow (Docker)
- Object storage: MinIO (Bronze as JSON payloads, Silver in Parquet)
- Warehouse: Postgres (`football_dw`)
- Layers:
  - `raw.fixtures` in Postgres (loaded from Silver)
  - `mart.team_match_goals_monthly` and `mart.league_summary` (built by Airflow DAG)
- Repo runs locally via `docker compose`.

## Target Architecture (vision)
Long-term target (not all implemented yet):
- Ingestion: fixtures + match events (event-level)
- Data quality gate: Great Expectations
- Transformations: dbt (staging -> intermediate -> marts)
- Dimensional model:
  - facts: fact_matches, fact_match_events
  - dims: dim_team, dim_player, dim_competition, dim_time, dim_venue
- BI: Metabase dashboards powered by mart tables

## Repo Map (expected)
- `infra/airflow/dags/` — Airflow DAGs
- `warehouse/ddl/` — Postgres DDL/migrations
- `pipelines/` or `src/` — Python pipeline logic (if present)
- `README.md` — how to run locally

If actual structure differs, follow the repository conventions you observe.

## Working Style (optimize for token usage)
When implementing a task:
1) Identify exactly which files you will touch.
2) State the minimal plan in 3–6 bullets.
3) Implement with small, readable diffs.
4) Add validation queries and run commands (PowerShell-first).
5) Summarize expected results and how to verify.

Do NOT:
- Dump large unrelated refactors.
- Invent tools/services not present in repo.
- Add heavy dependencies without explicit request.

## Data Contracts
- Use explicit column lists for all loads/transforms.
- Prefer `NOT NULL` + primary keys where correct.
- Prefer `ON CONFLICT ... DO UPDATE` with `IS DISTINCT FROM` for idempotent upserts.

## Airflow Standards
- DAGs must be idempotent.
- Parameterize via Airflow `params` and/or `dag_run.conf`.
- Fail fast with actionable error messages.
- Avoid global runtime state; keep connections inside callables.

## PowerShell-safe command patterns
- Prefer `docker compose exec -T ...`
- Prefer `psql -c "SQL..."` for ad-hoc checks (works in PowerShell)
- For file piped into psql (PowerShell):
  - `Get-Content 'path.sql' | docker compose exec -T postgres psql -U football -d football_dw`

## Validation Checklist (every change)
- Airflow: `airflow dags test <dag_id> <YYYY-MM-DD>` (via docker compose)
- Postgres sanity:
  - row counts
  - uniqueness checks
  - idempotent re-run results (inserts=0, updates=0 when no data changed)

## Project Gotchas (known)
- Windows PowerShell requires careful quoting for JSON `--conf`.
- Postgres schema names used: `raw`, `mart`.
- Fixtures uniqueness key: `fixture_id`.
