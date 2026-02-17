# Roadmap — football-analytics

## Phase 0 — Baseline (done)
- Local stack via docker compose
- Fixtures pipeline: Bronze -> Silver -> Postgres raw
- Mart aggregates in Postgres (monthly team stats + league summary)
- Idempotent re-runs (no duplication)

## Phase 1 — Event-level ingestion (next)
- Ingest match events (shots, cards, fouls, subs, minute, player/team)
- Bronze partitions: competition / season / match_date (and match_id)
- Silver normalization:
  - explicit schema
  - consistent types
  - deduplicate by (match_id, event_id) or a stable surrogate key

## Phase 2 — Data Quality Gate
- Add Great Expectations suite(s):
  - keys not null
  - uniqueness constraints
  - accepted value ranges (goals >= 0, minute within bounds)
  - referential integrity (player belongs to team where possible)
- Integrate validation task in Airflow DAGs; fail pipeline on violations

## Phase 3 — Dimensional Model (Gold)
- Implement tables:
  - facts: fact_matches, fact_match_events
  - dims: dim_team, dim_player, dim_competition, dim_time, dim_venue
- Define grains and surrogate keys
- Add constraints and basic db tests

## Phase 4 — dbt adoption (optional but recommended)
- Move SQL transforms into dbt:
  - stg_* / int_* / marts
- Add dbt tests and docs
- Keep Airflow as orchestrator triggering dbt runs

## Phase 5 — BI (Metabase)
- Create dashboards:
  - performance by round / home-away splits
  - goals by minute buckets
  - cards trends
  - team ranking by points + xG (if available later)

## Definition of Done (for each phase)
- DAG idempotent
- Verification queries included in README/docs
- Clear local run steps (PowerShell-friendly)
- Minimal and reviewable diffs
