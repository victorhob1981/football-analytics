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

function toJson(route: Route, response: JsonResponse): Promise<void> {
  return route.fulfill({
    status: response.status ?? 200,
    contentType: "application/json",
    body: JSON.stringify(response.body),
  });
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
      items: [
        {
          playerId: "p-1",
          playerName: "Arrascaeta",
          teamName: "Flamengo",
          nationality: "Uruguai",
          position: "Midfielder",
          minutesPlayed: 900,
          goals: 6,
          assists: 4,
          rating: 7.8,
        },
      ],
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
  return {
    data: {
      player: {
        playerId,
        playerName: "Arrascaeta",
        teamName: "Flamengo",
        position: "Midfielder",
      },
      summary: {
        matchesPlayed: 10,
        minutesPlayed: 900,
        goals: 6,
        assists: 4,
        shotsTotal: 18,
        shotsOnTarget: 8,
        passAccuracyPct: 85.2,
        yellowCards: 2,
        redCards: 0,
        rating: 7.8,
      },
      recentMatches: [],
    },
    meta: DEFAULT_META,
  };
}

export async function installApiMocks(page: Page): Promise<void> {
  await page.route("**/api/**", async (route) => {
    const requestUrl = new URL(route.request().url());
    const pathname = requestUrl.pathname;

    if (pathname === "/api/insights") {
      await toJson(route, { body: buildInsightsResponse() });
      return;
    }

    if (pathname === "/api/players") {
      await toJson(route, { body: buildPlayersListResponse() });
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
