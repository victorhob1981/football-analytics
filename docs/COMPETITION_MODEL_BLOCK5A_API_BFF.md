# Competition Model Block 5A - API and BFF Contracts

Reference date: 2026-03-25

## Scope closed

- New contract `GET /api/v1/competition-structure`
- New contract `GET /api/v1/group-standings`
- New contract `GET /api/v1/ties`
- New contract `GET /api/v1/team-progression`
- Backward-compatible expansion of `GET /api/v1/standings` with optional `stageId` and `groupId`

## Compatibility decisions

- League flow remains on the existing `/standings` contract.
- Group scoping is mandatory when the resolved stage has `stageFormat=group_table`.
- Structural endpoints are driven by `competitionKey + seasonLabel`.
- No endpoint branches on `competition_key` for structural behavior; the payload is driven by `stage_format`, structure rows and configuration.

## Files implemented

- `api/src/core/context_registry.py`
- `api/src/main.py`
- `api/src/routers/competition_hub.py`
- `api/src/routers/standings.py`
- `api/tests/test_competition_hub_routes.py`
- `api/tests/test_standings_routes.py`
- `docs/BFF_API_CONTRACT.md`

## Validation evidence

### Unit and contract tests

- `python -m pytest api/tests/test_standings_routes.py api/tests/test_competition_hub_routes.py`
- Result: `9 passed`

### Real BFF validation against local database

- `GET /api/v1/competition-structure?competitionKey=libertadores&seasonLabel=2024`
  - status `200`
  - stage formats: `qualification_knockout`, `group_table`, `knockout`
  - group stage returned `8` groups
- `GET /api/v1/competition-structure?competitionKey=champions_league&seasonLabel=2024/2025`
  - status `200`
  - `league_stage.stageFormat = league_table`
  - `league_stage.groups = 0`
- `GET /api/v1/group-standings?competitionKey=libertadores&seasonLabel=2024&stageId=77468966&groupId=23036f64061bf2ff6a088aa90b6659bd`
  - status `200`
  - `groupName = Group A`
  - `rows = 4`
  - leader `Fluminense`
- `GET /api/v1/ties?competitionKey=copa_do_brasil&seasonLabel=2024&stageId=77469788`
  - status `200`
  - `ties = 40`
  - first `resolutionType = single_match`
- `GET /api/v1/team-progression?competitionKey=libertadores&seasonLabel=2024&teamId=1095`
  - status `200`
  - team `Fluminense`
  - `progression` rows `= 3`
- `GET /api/v1/standings?competitionId=390&seasonId=2024&stageId=77468966&groupId=23036f64061bf2ff6a088aa90b6659bd`
  - status `200`
  - `stage.stageFormat = group_table`
  - `rows = 4`
- `GET /api/v1/standings?competitionId=390&seasonId=2024&stageId=77468966`
  - status `400`
  - `code = INVALID_QUERY_PARAM`
- `GET /api/v1/standings?competitionId=8&seasonId=2024`
  - status `200`
  - `rows = 20`
  - top team `Liverpool`

## Notes

- The dbt control models are materialized under schema `mart_control`, not `control`; the BFF query was aligned to the real schema during validation.
- Canonical standings context now expands source competition ids through the existing registry, so `competitionId=390` resolves the Libertadores raw source correctly.
