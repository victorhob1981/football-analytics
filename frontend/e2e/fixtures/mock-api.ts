import type { Page, Route } from "@playwright/test";

type JsonResponse = {
  status?: number;
  body: unknown;
};

const DEFAULT_META = {
  coverage: {
    status: "complete",
    percentage: 100,
  },
};

const PLAYERS_LIST_ITEMS = [
  {
    playerId: "p-1",
    playerName: "Arrascaeta",
    teamId: "flamengo",
    teamName: "Flamengo",
    nationality: "Uruguai",
    position: "CAM",
    matchesPlayed: 10,
    minutesPlayed: 900,
    goals: 6,
    assists: 4,
    rating: 7.8,
  },
  {
    playerId: "p-2",
    playerName: "Pedro",
    teamId: "flamengo",
    teamName: "Flamengo",
    nationality: "Brasil",
    position: "FW",
    matchesPlayed: 11,
    minutesPlayed: 980,
    goals: 9,
    assists: 2,
    rating: 8.1,
  },
  {
    playerId: "p-3",
    playerName: "Gerson",
    teamId: "flamengo",
    teamName: "Flamengo",
    nationality: "Brasil",
    position: "CM",
    matchesPlayed: 12,
    minutesPlayed: 1050,
    goals: 3,
    assists: 5,
    rating: 7.5,
  },
  {
    playerId: "p-4",
    playerName: "Vini Jr",
    teamId: "real-madrid",
    teamName: "Real Madrid",
    nationality: "Brasil",
    position: "LW",
    matchesPlayed: 8,
    minutesPlayed: 720,
    goals: 7,
    assists: 3,
    rating: 8.6,
  },
];

const TEAMS_LIST_ITEMS = [
  {
    teamId: "flamengo",
    teamName: "Flamengo",
    competitionId: "71",
    competitionName: "Brasileirao",
    seasonId: "2024",
    seasonLabel: "2024",
    position: 1,
    matchesPlayed: 12,
    wins: 9,
    draws: 2,
    losses: 1,
    goalsFor: 24,
    goalsAgainst: 9,
    goalDiff: 15,
    points: 29,
  },
  {
    teamId: "palmeiras",
    teamName: "Palmeiras",
    competitionId: "71",
    competitionName: "Brasileirao",
    seasonId: "2024",
    seasonLabel: "2024",
    position: 2,
    matchesPlayed: 12,
    wins: 8,
    draws: 3,
    losses: 1,
    goalsFor: 20,
    goalsAgainst: 8,
    goalDiff: 12,
    points: 27,
  },
];

const MATCHES_LIST_ITEMS = [
  {
    matchId: "m-1",
    fixtureId: "m-1",
    competitionId: "71",
    competitionName: "Brasileirao",
    seasonId: "2024",
    roundId: "12",
    kickoffAt: "2026-03-20T22:00:00Z",
    status: "FT",
    venueName: "Maracana",
    homeTeamId: "flamengo",
    homeTeamName: "Flamengo",
    awayTeamId: "palmeiras",
    awayTeamName: "Palmeiras",
    homeScore: 2,
    awayScore: 1,
  },
  {
    matchId: "m-2",
    fixtureId: "m-2",
    competitionId: "71",
    competitionName: "Brasileirao",
    seasonId: "2024",
    roundId: "12",
    kickoffAt: "2026-03-19T22:00:00Z",
    status: "FT",
    venueName: "Mineirao",
    homeTeamId: "atletico-mg",
    homeTeamName: "Atletico-MG",
    awayTeamId: "sao-paulo",
    awayTeamName: "Sao Paulo",
    homeScore: 1,
    awayScore: 1,
  },
];

function toJson(route: Route, response: JsonResponse): Promise<void> {
  return route.fulfill({
    status: response.status ?? 200,
    contentType: "application/json",
    body: JSON.stringify(response.body),
  });
}

function normalizeApiPath(pathname: string): string {
  return pathname.replace(/^\/api\/v1(?=\/)/, "/api");
}

function buildInsightsResponse() {
  return {
    data: [
      {
        insight_id: "insight-1",
        severity: "info",
        explanation: "Indicador de teste para fluxo E2E.",
        evidences: {
          sample_value: 42,
        },
        reference_period: "2026-01",
        data_source: ["mart.player_match_summary"],
      },
    ],
    meta: DEFAULT_META,
  };
}

function buildPlayersListResponse() {
  return {
    data: {
      items: PLAYERS_LIST_ITEMS,
    },
    meta: DEFAULT_META,
  };
}

function buildTeamsListResponse() {
  return {
    data: {
      items: TEAMS_LIST_ITEMS,
    },
    meta: DEFAULT_META,
  };
}

function buildMatchesListResponse() {
  return {
    data: {
      items: MATCHES_LIST_ITEMS,
    },
    meta: DEFAULT_META,
  };
}

function buildRankingResponse(pathname: string) {
  const rankingId = pathname.split("/").filter(Boolean).slice(-2).join("-");

  return {
    data: {
      rankingId,
      metricKey: "goals",
      rows: [
        {
          entityId: "p-1",
          entityName: "Arrascaeta",
          rank: 1,
          metricValue: 6,
          goals: 6,
        },
      ],
      updatedAt: "2026-02-21T10:00:00Z",
    },
    meta: DEFAULT_META,
  };
}

function buildPlayerProfileResponse(playerId: string) {
  const player =
    PLAYERS_LIST_ITEMS.find((candidate) => candidate.playerId === playerId) ??
    PLAYERS_LIST_ITEMS[0];

  return {
    data: {
      player: {
        playerId: player.playerId,
        playerName: player.playerName,
        teamId: player.teamId,
        teamName: player.teamName,
        position: player.position,
        nationality: player.nationality,
      },
      summary: {
        matchesPlayed: player.matchesPlayed,
        minutesPlayed: player.minutesPlayed,
        goals: player.goals,
        assists: player.assists,
        shotsTotal: 18,
        shotsOnTarget: 8,
        passAccuracyPct: 85.2,
        yellowCards: 2,
        redCards: 0,
        rating: player.rating,
      },
      recentMatches: [],
    },
    meta: DEFAULT_META,
  };
}

export async function installApiMocks(page: Page): Promise<void> {
  await page.route("**/api/**", async (route) => {
    const requestUrl = new URL(route.request().url());
    const pathname = normalizeApiPath(requestUrl.pathname);

    if (pathname === "/api/insights") {
      await toJson(route, { body: buildInsightsResponse() });
      return;
    }

    if (pathname === "/api/players") {
      await toJson(route, { body: buildPlayersListResponse() });
      return;
    }

    if (pathname === "/api/teams") {
      await toJson(route, { body: buildTeamsListResponse() });
      return;
    }

    if (pathname === "/api/matches") {
      await toJson(route, { body: buildMatchesListResponse() });
      return;
    }

    if (pathname.startsWith("/api/players/")) {
      const playerId = pathname.split("/").filter(Boolean).at(-1) ?? "p-1";
      await toJson(route, { body: buildPlayerProfileResponse(playerId) });
      return;
    }

    if (pathname.startsWith("/api/rankings/")) {
      await toJson(route, { body: buildRankingResponse(pathname) });
      return;
    }

    await toJson(route, {
      body: {
        data: [],
        meta: DEFAULT_META,
      },
    });
  });
}
