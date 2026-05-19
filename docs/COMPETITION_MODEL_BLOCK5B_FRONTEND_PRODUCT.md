# Competition Model Block 5B - Frontend and Product

Reference date: 2026-03-25

## Scope closed

- Season hub now reacts to structural competition data instead of assuming a single standings table.
- Group stages consume `GET /api/v1/group-standings`.
- Knockout stages consume `GET /api/v1/ties`.
- League-table stages continue to consume `GET /api/v1/standings`.
- Competition season cards now surface edition-format metadata when structural data exists.

## Structural rendering rules

- Default stage selection is structural:
  - current stage, else
  - main table stage (`group_table` or `league_table`), else
  - knockout, else
  - first mapped stage
- No component branches on `competitionKey` to decide whether to show groups, league phase or knockout.
- Stage rendering is driven by `stageFormat`, `groups[]` and `transitions[]` from `/api/v1/competition-structure`.

## Files implemented

- `frontend/src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/SeasonHubContent.tsx`
- `frontend/src/features/competitions/components/CompetitionHubContent.tsx`
- `frontend/src/features/competitions/hooks/index.ts`
- `frontend/src/features/competitions/hooks/useCompetitionStructure.ts`
- `frontend/src/features/competitions/hooks/useStageTies.ts`
- `frontend/src/features/competitions/queryKeys.ts`
- `frontend/src/features/competitions/services/competition-hub.service.ts`
- `frontend/src/features/competitions/types/index.ts`
- `frontend/src/features/competitions/types/competition-structure.types.ts`
- `frontend/src/features/competitions/utils/competition-structure.ts`
- `frontend/src/features/competitions/utils/competition-structure.test.ts`
- `frontend/src/features/standings/hooks/index.ts`
- `frontend/src/features/standings/hooks/useGroupStandingsTable.ts`
- `frontend/src/features/standings/hooks/useStandingsTable.ts`
- `frontend/src/features/standings/queryKeys.ts`
- `frontend/src/features/standings/services/standings.service.ts`
- `frontend/src/features/standings/types/index.ts`
- `frontend/src/features/standings/types/standings.types.ts`

## Validation evidence

### Frontend validation

- `pnpm --dir frontend typecheck`
- `pnpm --dir frontend test`
- `pnpm --dir frontend lint`
- `pnpm --dir frontend build`

Results:
- `typecheck`: passed
- `vitest`: `29 passed`
- `lint`: passed
- `next build`: passed

### Structural rendering proof

- Utility tests prove format description and default-stage choice from stage structure alone:
  - `qualification_knockout + group_table + knockout -> Fase de grupos + mata-mata`
  - `qualification_knockout + league_table + knockout -> League phase + mata-mata`
  - default stage prioritizes `league_table/group_table` over qualifiers when present

### No render branch by competition key

- Search on `frontend/src` for equality/switch branching on `competitionKey` returned no matches after the block changes.

## Notes

- League pages keep the previous flow when no structural edition is returned by the BFF.
- Structured competition pages now preserve `stageId/groupId` through the season hub query string so navigation between tabs does not lose the selected phase context.
