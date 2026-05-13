# Architecture — football-analytics

## Goal
A modern data stack platform for football analytics (Brazilian leagues), designed to be reproducible locally and safe to re-run.

## Current Architecture (Implemented)
### Components
- Airflow (orchestration) — Docker
- MinIO (data lake) — Bronze as JSON payloads, Silver as Parquet
- Postgres (`football_dw`) — Raw and Mart schemas

### Data Flow
1) **Ingestion (Bronze)**  
   - Fetch fixtures from API
   - Store raw payloads to MinIO as JSON (partitioned by season/date where applicable)

2) **Transform (Silver)**  
   - Normalize schema and types
   - Produce curated Parquet for downstream loads

3) **Load (Raw in Postgres)**
   - `raw.fixtures` upserted by `fixture_id`
   - Idempotent: re-run does not duplicate rows

4) **Mart (Postgres)**
   - `mart.team_match_goals_monthly`: team-level monthly aggregates incl. points and goal_diff
   - `mart.league_summary`: league-level summary incl. first/last match date, avg goals

## Target Architecture (Planned)
### New Ingestion
- Match events (event-level): shots, fouls, cards, substitutions, minute, player, team

### Data Quality Gate
- Great Expectations validations between Bronze->Silver and Silver->Raw
- Pipeline should fail early on invalid expectations

### Data Modeling
- Dimensional model (Gold):
  - fact_matches
  - fact_match_events
  - dim_team, dim_player, dim_competition, dim_time, dim_venue

### BI
- Metabase dashboards fed from marts / dimensional model

## Non-functional Requirements
- Idempotency on every layer
- Schema contracts (explicit columns)
- Observability: clear logs and verification queries
- Local reproducibility via docker compose
