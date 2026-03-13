# Portfolio Backfill Operational Plan

Status date: 2026-03-11

## Objective
Execute a historical backfill for the portfolio with a plan that is:

- compatible with the current Airflow pipeline
- safe for replay in `raw`
- realistic under the SportMonks free-tier daily cap
- ordered by portfolio value, not by alphabetical competition order

The goal is not "one competition per day".
The goal is "large waves with explicit request budgets and no avoidable reingestion".

## Scope

Competitions:

- Premier League
- La Liga
- Serie A
- Bundesliga
- Ligue 1
- Campeonato Brasileiro Serie A
- UEFA Champions League
- CONMEBOL Libertadores
- Copa do Brasil
- Campeonato Brasileiro Serie B

Closed seasons to ingest:

- Europe + Champions League: `2020/21`, `2021/22`, `2022/23`, `2023/24`, `2024/25`
- Brazil + Libertadores + Copa do Brasil: `2021`, `2022`, `2023`, `2024`, `2025`

Do not ingest ongoing seasons such as `2025/26`.

## Current Pipeline Constraints

This plan follows the code that exists today.

### Request model by domain

- `competition_structure`: about `3` requests per competition-season
  - one for league + seasons
  - one for stages
  - one for rounds
- `standings`: about `2` requests per competition-season
  - one to resolve `season_id`
  - one for standings
- `fixtures`: variable cost
  - the current provider uses `/fixtures/between/{from}/{to}` with pagination
  - filtering by competition and season happens locally after the API responses
  - this makes fixtures the most expensive "light" domain
- `match_statistics`: about `1` request per finished fixture
- `match_events`: about `1` request per finished fixture
- `lineups`: about `1` request per finished fixture
- `fixture_player_statistics`: about `1` request per finished fixture
- `head_to_head`: about `1` request per unique team pair in the season scope
- `player_season_statistics`: about `1` request per unique player in scope
  - in a large domestic league this is operationally expensive
  - local baseline from Brasileirao 2024: `846` distinct players in lineups for one season

### Important execution implications

- There is no standalone `teams` ingestion DAG today.
  - team identities come from competition structure, standings and fixtures payloads.
  - therefore `teams` adds no separate API cost in the current architecture.
- `player_season_statistics` should not run before `lineups` for the same competition batch.
  - current player discovery is seeded first from `raw.fixture_lineups`, and only falls back to `raw.match_events`.
- `lineups` and `fixture_player_statistics` currently hit the same underlying provider payload.
  - this does not break correctness, but it does duplicate requests.
- `fixtures` do not use the same cursor-based skip strategy used by fixture-level domains.
  - treat fixture backfill as something to run once per competition-season, not something to replay casually.

## Daily Budget

Provider hard cap:

- `53k requests/day`

Operational budget:

- use `40k requests/day` as the planning ceiling

Reason:

- keep margin for retries
- absorb pagination variance on fixtures
- avoid breaking the day because one domain consumed more than estimated

## Domain Buckets

Logical buckets for planning:

### Light

- competition structure
- teams (implicit, no standalone API cost)
- standings
- fixtures

Operational note:

- `fixtures` stays in the light bucket only for business priority
- in practice it is the most variable-cost light domain and must be budgeted separately

### Medium

- match statistics
- head-to-head
- player season statistics

Operational note:

- `player_season_statistics` is the heaviest part of the medium bucket
- it requires `lineups` first for clean player discovery

### Heavy

- match events
- lineups
- fixture player statistics

Operational note:

- `lineups` is logically heavy, but must be pulled forward before `player_season_statistics`
- because of that, this plan includes a "prep" wave that runs `lineups` ahead of the rest of the heavy payloads

## Competition Groups

To keep each wave inside the `40k/day` operating ceiling, use these groups:

### Group A

- Premier League
- La Liga
- Serie A
- Bundesliga

### Group B

- Ligue 1
- Campeonato Brasileiro Serie A
- UEFA Champions League
- CONMEBOL Libertadores

### Group C

- Copa do Brasil
- Campeonato Brasileiro Serie B

## Operational Waves

### Wave 1 - Structure and Table Coverage

Competitions:

- all 10 competitions

Domains:

- competition structure
- teams (implicit)
- standings

Estimated requests:

- about `250` to `350`

Why first:

- gives season, stage, round and standings context
- very cheap
- unlocks the catalog and coverage checks for the whole portfolio

### Wave 2 - Fixtures Coverage

Competitions:

- all 10 competitions

Domains:

- fixtures

Estimated requests:

- about `5k` to `12k`

Why second:

- all fixture-scoped domains depend on a complete fixture base
- request cost is still manageable, but more variable than other light domains because of paginated date-window scans

Execution rule:

- use competition-season-specific `fixture_windows` whenever possible
- do not rerun this wave unless coverage or quality checks fail

### Wave 3 - Group A Analytical Prep

Competitions:

- Premier League
- La Liga
- Serie A
- Bundesliga

Domains:

- match statistics
- head-to-head
- lineups
- player season statistics

Estimated requests:

- about `34k` to `38k`

Why grouped this way:

- four large domestic leagues fit inside the daily budget when `lineups` is included as the preparation step for player-season stats

### Wave 4 - Group B Analytical Prep

Competitions:

- Ligue 1
- Campeonato Brasileiro Serie A
- UEFA Champions League
- CONMEBOL Libertadores

Domains:

- match statistics
- head-to-head
- lineups
- player season statistics

Estimated requests:

- about `24k` to `30k`

Why grouped this way:

- this preserves portfolio priority
- it stays comfortably below the operating ceiling

### Wave 5 - Group C Analytical Prep

Competitions:

- Copa do Brasil
- Campeonato Brasileiro Serie B

Domains:

- match statistics
- head-to-head
- lineups
- player season statistics

Estimated requests:

- about `10k` to `14k`

Why last in prep:

- lower portfolio priority
- much safer to finish after premium coverage is already loaded

### Wave 6 - Premium Deep Detail

Competitions:

- Group A
- Group B

Domains:

- match events
- fixture player statistics

Estimated requests:

- about `24k` to `28k`

Why this is safe:

- `lineups` was already ingested in Waves 3 and 4
- the remaining heavy domains are now the fixture-level enrichments that add frontend depth

### Wave 7 - Secondary Deep Detail

Competitions:

- Group C

Domains:

- match events
- fixture player statistics

Estimated requests:

- about `4k` to `6k`

Why isolated:

- closes the low-priority tail without pressuring quota

## Recommended Execution Order Inside Each Wave

### Wave 1

1. competition structure
2. standings

### Wave 2

1. fixtures

### Waves 3 to 5

1. match statistics
2. head-to-head
3. lineups
4. player season statistics

Reason:

- `lineups` must exist before `player season statistics` for stable player discovery

### Waves 6 to 7

1. match events
2. fixture player statistics

Reason:

- this is the deepest fixture-level enrichment layer
- by this point the core analytical layer is already loaded

## Replay and Recovery Rules

The `raw` layer is already idempotent, so replay is safe from a duplication perspective.
The goal here is to avoid unnecessary provider calls, not to protect against duplicate rows.

### Safe to replay whole wave

- Waves 3 to 7

Reason:

- fixture-level and numeric-id domains use sync state and idempotent loads

### Replay only if needed

- Wave 2 (`fixtures`)

Reason:

- fixtures still use date-window scans and do not have the same cursor-based optimization as other domains

### Best recovery strategy for partial failure

- rerun only the affected domain DAG
- for fixture-scoped domains, use `mode=backfill` plus explicit `fixture_ids` chunks when resuming

This is already compatible with the current ingestion code and does not require architecture changes.

## Mandatory Operational Rules

- Do not execute backfill waves through the full `pipeline_competition` DAG.
- Trigger only the domain DAGs required by the wave.
- Do not run `player_season_statistics` before `lineups` in the same competition batch.
- Treat `40k/day` as the true budget, not `53k/day`.
- Keep a separate execution ledger by wave, competition and season.
- Validate one wave completely before opening the next.

## Simple Request-Saving Improvements That Do Not Change Architecture

These are process-level improvements that fit the current repository:

1. Use domain DAGs instead of the full orchestrator.
   - avoids rerunning already completed light stages

2. Override `fixture_windows` per competition-season.
   - reduces wasted pagination on global fixture scans

3. Resume fixture-level failures with explicit `fixture_ids`.
   - avoids repeating already successful fixture calls

4. Treat `lineups` as a prep domain, not just a deep domain.
   - this prevents empty or underpopulated `player_season_statistics` runs

## Final Recommended Calendar

Use the following execution calendar as the default portfolio backfill sequence:

1. Wave 1 - structure and standings for all competitions
2. Wave 2 - fixtures for all competitions
3. Wave 3 - Group A analytical prep
4. Wave 4 - Group B analytical prep
5. Wave 5 - Group C analytical prep
6. Wave 6 - premium deep detail
7. Wave 7 - secondary deep detail

This is the cleanest plan under the current codebase because it:

- respects the request budget with realistic buffers
- avoids running player-season stats before player discovery exists
- minimizes redundant provider calls
- preserves replay safety in `raw`
- prioritizes the competitions that matter most for portfolio value
