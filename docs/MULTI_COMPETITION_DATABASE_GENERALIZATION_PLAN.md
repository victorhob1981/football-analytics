# Multi-Competition Database Generalization Plan

Status date: 2026-03-11

## Objective
Generalize the current warehouse so it can support the portfolio scope across multiple domestic leagues and cups, while preserving idempotency, traceability and clean analytical contracts.

Target competition scope:
- Premier League
- La Liga
- Serie A (Italy)
- Bundesliga
- Ligue 1
- UEFA Champions League
- CONMEBOL Libertadores
- Campeonato Brasileiro Serie A
- Campeonato Brasileiro Serie B
- Copa do Brasil

## Current constraint
The current platform already uses competition and season fields in many places, but it was modeled operationally around Brasileirao-first ingestion. That is enough for a first implementation, but not enough for a durable multi-competition historical warehouse.

The goal is not a full rewrite. The goal is to make competition, season and provider identity first-class business keys across `raw` and `mart`.

## Design principles
- Keep provider source ids preserved.
- Separate provider identity from canonical competition identity.
- Distinguish league vs cup vs international competition.
- Distinguish season label from provider season id.
- Make every table safe for multi-competition joins and filters.
- Avoid assuming one competition has one fixed round shape.

## Main database changes
### 1. Add canonical competition catalog
Create control tables that define the competition universe independently from the provider.

Recommended tables:
- `control.competitions`
- `control.competition_provider_map`
- `control.season_catalog`

Suggested columns for `control.competitions`:
- `competition_key` PK, stable text key such as `epl`, `laliga`, `ucl`, `brasileirao_a`
- `competition_name`
- `competition_type` with values like `league`, `cup`, `continental_cup`
- `country_name` nullable
- `confederation_name` nullable
- `tier` nullable
- `is_active`
- `display_priority`

Suggested columns for `control.competition_provider_map`:
- `competition_key`
- `provider`
- `provider_league_id`
- `provider_name`
- unique key on `(provider, provider_league_id)`

Suggested columns for `control.season_catalog`:
- `competition_key`
- `season_label` such as `2024` or `2024_25`
- `season_start_date`
- `season_end_date`
- `is_closed`
- `provider`
- `provider_season_id` nullable
- unique key on `(competition_key, season_label, provider)`

Why:
- this removes the hidden assumption that `league_id` alone is the main business identifier

### 2. Add backfill control tables
Create operational control tables to drive ingestion and validation.

Recommended tables:
- `control.backfill_manifest`
- `control.backfill_runs`
- `control.domain_coverage`

Why:
- the system needs durable operational state for quota-aware historical extraction

### 3. Strengthen provenance in raw tables
Every raw domain table should include enough provenance to trace the row back to the provider and the extraction slice.

Recommended common columns:
- `provider`
- `competition_key`
- `provider_league_id`
- `season_label`
- `provider_season_id` nullable
- `ingested_at`
- `source_run_id`
- `source_entity_type`

Why:
- this makes replay, audit and debugging straightforward

## Raw-layer changes by concern
### 4. Normalize competition identity in `raw.fixtures`
The current fixture grain is close to reusable, but it should stop relying only on `league_id` and `season`.

Recommended additions:
- `competition_key`
- `season_label`
- `provider`
- `provider_season_id`
- `competition_type`
- `stage_name`
- `round_name`
- `group_name` nullable
- `leg` nullable

Keep:
- `fixture_id` as the provider fixture key if the provider is fixed

If multi-provider support remains possible in the future, prefer:
- natural uniqueness on `(provider, fixture_id)`

Why:
- cup and continental competitions do not behave like domestic round-robin leagues

### 5. Review primary and unique keys in raw domain tables
Any table keyed only by `fixture_id`, `team_id` or `player_id` may become fragile when multi-provider or replay scenarios expand.

Recommended pattern:
- preserve provider ids as raw business keys
- use unique constraints that include provider where necessary
- use `ON CONFLICT ... DO UPDATE` with `IS DISTINCT FROM`

Examples:
- `raw.match_statistics`: unique key should remain fixture-team scoped, but provider-aware if fixture ids are not globally guaranteed
- `raw.match_events`: event uniqueness may need `(provider, fixture_id, event_id)` or a derived stable event hash
- `raw.fixture_player_statistics`: prefer `(provider, fixture_id, team_id, player_id, stat_type)` or the existing stable business grain if already stronger

### 6. Make round and stage modeling more flexible
League-centric `round_number` is useful, but not sufficient for cups and continental competitions.

Recommended raw support:
- `stage_id`
- `stage_name`
- `round_id`
- `round_name`
- `round_number` nullable
- `group_name` nullable
- `knockout_leg` nullable

Why:
- Champions League, Libertadores and Copa do Brasil require stage-aware navigation

### 7. Add explicit season semantics
`season` as integer is not enough for cross-year competitions.

Recommended pattern:
- keep `season` integer only where it is already useful and cheap
- add `season_label` as canonical text
- add `season_start_date`
- add `season_end_date`

Examples:
- Brasileirao 2024 -> `season=2024`, `season_label='2024'`
- Champions League 2024/25 -> `season=2024`, `season_label='2024_25'`

Why:
- the frontend and analytics need clean labels without guessing

## Mart-layer changes
### 8. Strengthen `dim_competition`
`mart.dim_competition` should become the central semantic layer for all competitions in scope.

Recommended attributes:
- `competition_sk`
- `competition_key`
- `competition_name`
- `competition_type`
- `country_name`
- `confederation_name`
- `tier`
- `provider`
- `provider_league_id`
- `is_domestic`
- `is_cup`

Why:
- this becomes the stable dimension used by API filters and BI slices

### 9. Add or strengthen a season dimension
If not already explicit enough, add a dimension such as `mart.dim_season`.

Recommended attributes:
- `season_sk`
- `competition_key`
- `season_label`
- `season_start_date`
- `season_end_date`
- `is_closed`
- `provider`
- `provider_season_id`

Why:
- season is a first-class analytical filter and should not stay implicit inside fact tables only

### 10. Revisit fact grain assumptions
Facts must keep working for league and cup competitions.

Recommended fact checks:
- `fact_matches` grain remains one row per match
- `fact_standings_snapshots` must tolerate different competition structures
- player summary marts must filter by competition and season without assuming league-only tables

Potential additions to `fact_matches`:
- `competition_key`
- `season_label`
- `stage_sk`
- `round_sk`
- `is_knockout`
- `leg_number` nullable

### 11. Make player and team summaries competition-aware
Current analytics such as `player_match_summary`, `player_season_summary` and `head_to_head_summary` should expose:
- `competition_key`
- `season_label`
- `competition_name`

Why:
- the API and frontend should not infer competition context from internal ids alone

## Operational schema additions
### 12. Add quality and readiness metadata
Create durable tables for release readiness.

Recommended tables:
- `control.data_release_status`
- `control.coverage_audit`

Useful fields:
- `competition_key`
- `season_label`
- `domain`
- `expected_count`
- `loaded_count`
- `coverage_pct`
- `status`
- `validated_at`

Why:
- the frontend should consume only what the platform has validated

## Migration strategy
Generalization should be incremental, not disruptive.

### Phase 1
Add control tables and season semantics:
- `control.competitions`
- `control.competition_provider_map`
- `control.season_catalog`
- `control.backfill_manifest`
- `control.backfill_runs`

### Phase 2
Add provenance and season-label columns to raw tables without removing existing columns.

### Phase 3
Update loaders to populate the new columns and preserve idempotent upserts.

### Phase 4
Refactor dbt staging models to standardize:
- `competition_key`
- `season_label`
- `provider`
- `provider ids`

### Phase 5
Refactor marts and BFF contracts to use canonical competition and season fields.

### Phase 6
Only after the new path is stable, deprecate old league-specific assumptions.

## Risks to address
- assuming integer `season` is enough for all competitions
- assuming round numbers exist and mean the same thing everywhere
- assuming fixture ids are globally safe without provider context
- assuming standings exist for every competition in the same shape
- assuming deep domains such as events and lineups have uniform coverage

## Concrete database backlog
1. Add a new `control` schema if it does not exist.
2. Create competition catalog and provider mapping tables.
3. Create season catalog with canonical `season_label`.
4. Create backfill manifest and run tracking tables.
5. Add provenance columns to raw tables, starting with `raw.fixtures`.
6. Add `competition_key` and `season_label` propagation to all raw loaders.
7. Refactor dbt staging models to standardize multi-competition semantics.
8. Strengthen `dim_competition` and add `dim_season` if needed.
9. Update marts used by the BFF to expose canonical competition and season fields.
10. Add release-status and coverage tables for frontend-safe publishing.

## Definition of done
The database is ready for multi-competition portfolio use when:
- every raw row is traceable to a provider, competition and season
- `competition_key` and `season_label` exist as stable cross-layer identifiers
- facts and marts can support leagues and cups without special-case logic
- backfill execution is driven by persisted control tables
- quality and coverage status can be queried directly from the warehouse
- the BFF can filter by canonical competition and season without relying on Brasileirao-specific assumptions
