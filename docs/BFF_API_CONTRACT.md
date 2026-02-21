# BFF API Contract (Frontend Integration)

Reference date: 2026-02-21  
Scope: official BFF contract for frontend integration, aligned with:
- `docs/FRONTEND_ARCHITECTURE_BLUEPRINT.md`
- `docs/FRONTEND_MANUAL_POSSIBILIDADES.md`
- `frontend/src/config/metrics.registry.ts`
- `frontend/src/config/ranking.registry.ts`

## 1) Global Rules

## 1.1 Base path
- Official base path: `/api/v1`

## 1.2 Standard success response

```json
{
  "data": {},
  "meta": {
    "pagination": {
      "page": 1,
      "pageSize": 20,
      "totalCount": 200,
      "totalPages": 10,
      "hasNextPage": true,
      "hasPreviousPage": false
    },
    "coverage": {
      "status": "complete",
      "percentage": 100,
      "label": "Complete coverage"
    },
    "requestId": "req_01H...",
    "generatedAt": "2026-02-21T13:30:00Z"
  }
}
```

Type contract:
- `ApiResponse<T>`: `{ data: T; meta?: ApiResponseMeta }`
- `Pagination`: `page`, `pageSize`, `totalCount`, optional `totalPages`, `hasNextPage`, `hasPreviousPage`
- `CoverageState`:
  - `status`: `complete | partial | empty | unknown`
  - `percentage?`: `0..100`
  - `label?`: string

Coverage rule:
- `meta.coverage` must be returned when data quality/completeness is relevant.
- Expected in: `/players`, `/players/{playerId}`, `/rankings/{rankingType}`, `/matches/{matchId}`, `/insights`.
- Optional in `/matches`.

## 1.3 Standard error response

```json
{
  "message": "Invalid ranking type.",
  "code": "INVALID_RANKING_TYPE",
  "status": 400,
  "details": {
    "rankingType": "unknown-ranking"
  }
}
```

Type contract:
- `ApiError`: `message` (required), `code?`, `status?`, `details?`
- `HttpError`: same shape, with `status` required

## 1.4 Global filters and time range

Global filters:
- `competitionId` (string)
- `seasonId` (string)
- `roundId` (string)
- `venue` (`home | away | all`)

Time range (mutually exclusive):
- Option A: `lastN` (int > 0)
- Option B: `dateStart` + `dateEnd` (`YYYY-MM-DD`)

Validation:
1. `lastN` cannot coexist with `dateStart/dateEnd`.
2. `dateStart` without `dateEnd` (or vice-versa) is invalid.
3. Invalid combinations return `400 INVALID_TIME_RANGE`.

Migration compatibility:
- BFF may accept legacy aliases `dateRangeStart/dateRangeEnd`.
- Official contract is `dateStart/dateEnd`.

---

## 2) Endpoints

## 2.1 GET `/players`

Purpose:
- List players with aggregated stats in the selected scope.

Query params:
- Global + time: `competitionId`, `seasonId`, `roundId`, `venue`, `lastN` or `dateStart/dateEnd`
- Local:
  - `search` (string)
  - `teamId` (string)
  - `position` (string)
  - `minMinutes` (int >= 0)
- Pagination:
  - `page` (default `1`)
  - `pageSize` (default `20`, recommended max `100`)
- Sort:
  - `sortBy` (`playerName | minutesPlayed | goals | assists | rating`)
  - `sortDirection` (`asc | desc`, default `desc`)

Request example:
```http
GET /api/v1/players?competitionId=648&seasonId=2024&lastN=5&page=1&pageSize=20&sortBy=goals&sortDirection=desc
```

Response `data` shape:
- `{ items: PlayerListItem[] }`
- `PlayerListItem`:
  - required: `playerId`, `playerName`
  - optional: `teamId`, `teamName`, `position`, `nationality`, `matchesPlayed`, `minutesPlayed`, `goals`, `assists`, `shotsTotal`, `passAccuracyPct`, `yellowCards`, `redCards`, `rating`

Rules:
- `meta.pagination` required.
- `meta.coverage` expected.

---

## 2.2 GET `/players/{playerId}`

Purpose:
- Player profile, summary, and recent matches.

Path params:
- `playerId` (required)

Query params:
- Global + time: `competitionId`, `seasonId`, `roundId`, `venue`, `lastN` or `dateStart/dateEnd`
- Local:
  - `includeRecentMatches` (`true|false`, default `true`)
  - `recentMatchesLimit` (int, default `10`)
- Pagination: N/A (resource detail)
- Sort: N/A for top-level profile (recent matches default `playedAt desc`)

Request example:
```http
GET /api/v1/players/p-1?competitionId=648&seasonId=2024&dateStart=2026-02-01&dateEnd=2026-02-20&includeRecentMatches=true
```

Response `data` shape:
- `PlayerProfile`
  - `player`
  - `summary`
  - `recentMatches?`

Rules:
- `404 PLAYER_NOT_FOUND` if missing.
- `meta.coverage` expected.

---

## 2.3 GET `/rankings/{rankingType}`

Purpose:
- Generic ranking endpoint driven by `rankingType`.

Path params:
- `rankingType` (required)

`rankingType` must be compatible with frontend registry (`frontend/src/config/ranking.registry.ts`).

Current required values:
- `player-goals`
- `player-assists`
- `player-shots-total`
- `player-shots-on-target`
- `player-pass-accuracy`
- `player-rating`
- `player-yellow-cards`
- `team-possession`
- `team-pass-accuracy`

Query params:
- Global + time: `competitionId`, `seasonId`, `roundId`, `venue`, `lastN` or `dateStart/dateEnd`
- Local:
  - `search` (string)
  - `minSampleValue` (int >= 0)
  - `freshnessClass` (`season | fast`)
- Pagination:
  - `page` (default `1`)
  - `pageSize` (default `20`)
- Sort:
  - `sortDirection` (`asc | desc`, default from ranking definition)

Request example:
```http
GET /api/v1/rankings/player-goals?competitionId=648&seasonId=2024&page=1&pageSize=50&sortDirection=desc
```

Response `data` shape:
- `RankingTableData`
  - `rankingId` (string)
  - `metricKey` (string)
  - `rows` (`RankingTableRow[]`)
  - `updatedAt?` (ISO datetime)

`RankingTableRow` minimum:
- `entityId` (required)
- optional: `entityName`, `rank`, `metricValue`, and extra ranking columns

Rules:
- `400 INVALID_RANKING_TYPE` if unsupported.
- `meta.pagination` required.
- `meta.coverage` expected.

---

## 2.4 GET `/matches`

Purpose:
- Match list with summary context.

Query params:
- Global + time: `competitionId`, `seasonId`, `roundId`, `venue`, `lastN` or `dateStart/dateEnd`
- Local:
  - `search` (string)
  - `status` (`scheduled | live | finished | cancelled` or provider-compatible values)
- Pagination:
  - `page` (default `1`)
  - `pageSize` (default `20`)
- Sort:
  - `sortBy` (`kickoffAt | status | homeTeamName | awayTeamName`)
  - `sortDirection` (`asc | desc`, default `desc`)

Request example:
```http
GET /api/v1/matches?competitionId=648&seasonId=2024&status=finished&page=1&pageSize=20&sortBy=kickoffAt&sortDirection=desc
```

Response `data` shape:
- `{ items: MatchListItem[] }`
- `MatchListItem`:
  - required: `matchId`
  - optional: `fixtureId`, `competitionId`, `competitionName`, `seasonId`, `roundId`, `kickoffAt`, `status`, `venueName`, `homeTeamId`, `homeTeamName`, `awayTeamId`, `awayTeamName`, `homeScore`, `awayScore`

Rules:
- `meta.pagination` required.
- `meta.coverage` optional.

---

## 2.5 GET `/matches/{matchId}`

Purpose:
- Match center blocks (header + timeline + lineups + player stats).

Path params:
- `matchId` (required)

Query params:
- Global + time: `competitionId`, `seasonId`, `roundId`, `venue`, `lastN` or `dateStart/dateEnd`
- Local:
  - `includeTimeline` (`true|false`, default `true`)
  - `includeLineups` (`true|false`, default `true`)
  - `includePlayerStats` (`true|false`, default `true`)
- Pagination: N/A
- Sort: N/A at top-level (timeline should be chronological)

Request example:
```http
GET /api/v1/matches/m-1001?competitionId=648&seasonId=2024&lastN=10&includeTimeline=true&includeLineups=true&includePlayerStats=true
```

Response `data` shape:
- `MatchCenterData`
  - `match` (required)
  - `timeline?`
  - `lineups?`
  - `playerStats?`

Rules:
- `404 MATCH_NOT_FOUND` if missing.
- `meta.coverage` expected.

---

## 2.6 GET `/insights`

Purpose:
- Return insight feed for a given context.

Query params:
- Context:
  - `entityType` (required): `player | team | match | competition | global`
  - `entityId` (required when `entityType != global`)
- Global + time: `competitionId`, `seasonId`, `roundId`, `venue`, `lastN` or `dateStart/dateEnd`
- Local:
  - `severity` (`info | warning | critical`)
- Pagination (optional):
  - `page`, `pageSize`
- Sort (optional):
  - `sortBy` (`severity | referencePeriod`)
  - `sortDirection` (`asc | desc`)
  - default recommended: highest severity first

Request example:
```http
GET /api/v1/insights?entityType=player&entityId=p-1&competitionId=648&seasonId=2024&lastN=5
```

Response `data` shape:
- `InsightObject[]`
- `InsightObject`:
  - `insight_id`
  - `severity`
  - `explanation`
  - `evidences` (`Record<string, number>`)
  - `reference_period`
  - `data_source` (`string[]`)

Rules:
- `400 INVALID_INSIGHT_CONTEXT` for invalid context.
- `meta.coverage` expected.
- `meta.pagination` required only when pagination is requested/supported.

---

## 3) Recommended Error Codes

| HTTP | code | When to use |
|---|---|---|
| 400 | `INVALID_QUERY_PARAM` | invalid param type/enum/range |
| 400 | `INVALID_TIME_RANGE` | invalid `lastN` vs `dateStart/dateEnd` combination |
| 400 | `INVALID_RANKING_TYPE` | unsupported `rankingType` |
| 400 | `INVALID_INSIGHT_CONTEXT` | invalid `entityType/entityId` combination |
| 404 | `PLAYER_NOT_FOUND` | unknown player |
| 404 | `MATCH_NOT_FOUND` | unknown match |
| 429 | `RATE_LIMITED` | rate limit exceeded |
| 500 | `INTERNAL_ERROR` | unexpected BFF error |

---

## 4) Frontend Compatibility Checklist

- Frontend base url + path must target `/api/v1`.
- `rankingType` must stay compatible with `ranking.registry.ts`.
- Success responses must follow `ApiResponse<T>`.
- Coverage must be present when applicable to support `loading/empty/partial/error` UX states.
- Errors must follow `ApiError` standard so frontend can render predictable fallback messages.

