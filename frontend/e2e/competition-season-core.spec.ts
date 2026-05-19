import { expect, test, type Page, type Route } from "@playwright/test";

type ApiPayload = {
  data: unknown;
  meta?: Record<string, unknown>;
};

function json(route: Route, body: ApiPayload): Promise<void> {
  return route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify(body),
  });
}

function noContent(route: Route, status = 404): Promise<void> {
  return route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify({ data: null }),
  });
}

function buildRankingResponse(pathname: string) {
  const rankingId = pathname.split("/").filter(Boolean).at(-1) ?? "player-goals";
  const isPlayerRanking = rankingId.startsWith("player");

  return {
    data: {
      rankingId,
      metricKey: isPlayerRanking ? "goals" : "team_possession_pct",
      rows: [
        {
          entityId: isPlayerRanking ? "p-1" : "40",
          entityName: isPlayerRanking ? "Mohamed Salah" : "Liverpool",
          teamId: "40",
          teamName: "Liverpool",
          rank: 1,
          metricValue: isPlayerRanking ? 28 : 61.5,
          position: isPlayerRanking ? "FW" : undefined,
        },
      ],
      updatedAt: "2026-03-21T13:31:34Z",
    },
    meta: {
      coverage: {
        status: "complete",
        percentage: 100,
        label: "Ranking coverage",
      },
    },
  };
}

function buildLeagueMatchesResponse(roundId: string) {
  return {
    data: {
      items: [
        {
          matchId: "19135048",
          fixtureId: "19135048",
          competitionId: "8",
          competitionName: "Premier League",
          seasonId: "2024",
          roundId,
          kickoffAt: "2025-05-25T15:00:00Z",
          status: "FT",
          venueName: "Anfield",
          homeTeamId: "40",
          homeTeamName: "Liverpool",
          awayTeamId: "50",
          awayTeamName: "Crystal Palace",
          homeScore: 2,
          awayScore: 1,
        },
      ],
    },
    meta: {
      coverage: {
        status: "complete",
        percentage: 100,
        label: "Match list coverage",
      },
    },
  };
}

function buildLeagueStandingsResponse(roundId: string) {
  return {
    data: {
      competition: {
        competitionId: "8",
        competitionKey: "premier_league",
        competitionName: "Premier League",
        seasonId: "2024",
        seasonLabel: "2024/2025",
        providerSeasonId: "23614",
      },
      stage: {
        stageId: "77471288",
        stageName: "Regular Season",
        expectedTeams: 20,
      },
      selectedRound: {
        roundId,
        providerRoundId: roundId === "29" ? "339264" : "339273",
        roundName: roundId,
        label: `Rodada ${roundId}`,
        startingAt: roundId === "29" ? "2025-02-19" : "2025-05-25",
        endingAt: roundId === "29" ? "2025-04-16" : "2025-05-25",
        isCurrent: roundId === "38",
      },
      currentRound: {
        roundId: "38",
        providerRoundId: "339273",
        roundName: "38",
        label: "Rodada 38",
        startingAt: "2025-05-25",
        endingAt: "2025-05-25",
        isCurrent: true,
      },
      rounds: [
        {
          roundId: "29",
          providerRoundId: "339264",
          roundName: "29",
          label: "Rodada 29",
          startingAt: "2025-02-19",
          endingAt: "2025-04-16",
          isCurrent: false,
        },
        {
          roundId: "38",
          providerRoundId: "339273",
          roundName: "38",
          label: "Rodada 38",
          startingAt: "2025-05-25",
          endingAt: "2025-05-25",
          isCurrent: true,
        },
      ],
      rows: [
        {
          position: 1,
          teamId: "40",
          teamName: "Liverpool",
          matchesPlayed: roundId === "29" ? 29 : 38,
          wins: roundId === "29" ? 21 : 25,
          draws: roundId === "29" ? 7 : 9,
          losses: roundId === "29" ? 1 : 4,
          goalsFor: roundId === "29" ? 69 : 86,
          goalsAgainst: roundId === "29" ? 27 : 41,
          goalDiff: roundId === "29" ? 42 : 45,
          points: roundId === "29" ? 70 : 84,
        },
        {
          position: 2,
          teamId: "42",
          teamName: "Arsenal",
          matchesPlayed: roundId === "29" ? 29 : 38,
          wins: roundId === "29" ? 18 : 22,
          draws: roundId === "29" ? 8 : 8,
          losses: roundId === "29" ? 3 : 8,
          goalsFor: roundId === "29" ? 54 : 72,
          goalsAgainst: roundId === "29" ? 24 : 37,
          goalDiff: roundId === "29" ? 30 : 35,
          points: roundId === "29" ? 62 : 74,
        },
      ],
      updatedAt: "2026-03-21T13:31:34Z",
    },
    meta: {
      coverage: {
        status: "partial",
        percentage: 10,
        label: "Standings coverage",
      },
    },
  };
}

function buildCupStructureResponse() {
  return {
    data: {
      competition: {
        competitionKey: "copa_do_brasil",
        competitionName: "Copa do Brasil",
        seasonLabel: "2024",
        formatFamily: "knockout",
        seasonFormatCode: "cup_knockout",
        participantScope: "club",
      },
      stages: [
        {
          stageId: "quarter",
          stageName: "Quartas de final",
          stageCode: "quarter",
          stageFormat: "knockout",
          stageOrder: 1,
          standingsContextMode: null,
          bracketContextMode: null,
          groupMode: null,
          eliminationMode: null,
          isCurrent: false,
          groups: [],
          transitions: [],
        },
        {
          stageId: "semi",
          stageName: "Semifinal",
          stageCode: "semi",
          stageFormat: "knockout",
          stageOrder: 2,
          standingsContextMode: null,
          bracketContextMode: null,
          groupMode: null,
          eliminationMode: null,
          isCurrent: false,
          groups: [],
          transitions: [],
        },
        {
          stageId: "final",
          stageName: "Final",
          stageCode: "final",
          stageFormat: "knockout",
          stageOrder: 3,
          standingsContextMode: null,
          bracketContextMode: null,
          groupMode: null,
          eliminationMode: null,
          isCurrent: true,
          groups: [],
          transitions: [],
        },
      ],
    },
  };
}

function buildCupTiesResponse(stageId: string) {
  if (stageId === "quarter") {
    return {
      data: {
        competition: {
          competitionKey: "copa_do_brasil",
          competitionName: "Copa do Brasil",
          seasonLabel: "2024",
          formatFamily: "knockout",
          seasonFormatCode: "cup_knockout",
          participantScope: "club",
        },
        stage: {
          stageId: "quarter",
          stageName: "Quartas de final",
          stageFormat: "knockout",
          stageOrder: 1,
          isCurrent: false,
        },
        ties: [
          {
            tieId: "quarter-1",
            tieOrder: 1,
            homeTeamId: "40",
            homeTeamName: "Flamengo",
            awayTeamId: "42",
            awayTeamName: "Palmeiras",
            matchCount: 2,
            firstLegAt: "2024-08-21",
            lastLegAt: "2024-08-29",
            homeGoals: 3,
            awayGoals: 2,
            winnerTeamId: "40",
            winnerTeamName: "Flamengo",
            resolutionType: "aggregate",
            hasExtraTimeMatch: false,
            hasPenaltiesMatch: false,
          },
        ],
      },
    };
  }

  if (stageId === "semi") {
    return {
      data: {
        competition: {
          competitionKey: "copa_do_brasil",
          competitionName: "Copa do Brasil",
          seasonLabel: "2024",
          formatFamily: "knockout",
          seasonFormatCode: "cup_knockout",
          participantScope: "club",
        },
        stage: {
          stageId: "semi",
          stageName: "Semifinal",
          stageFormat: "knockout",
          stageOrder: 2,
          isCurrent: false,
        },
        ties: [
          {
            tieId: "semi-1",
            tieOrder: 1,
            homeTeamId: "40",
            homeTeamName: "Flamengo",
            awayTeamId: "43",
            awayTeamName: "Bahia",
            matchCount: 2,
            firstLegAt: "2024-09-18",
            lastLegAt: "2024-09-26",
            homeGoals: 4,
            awayGoals: 1,
            winnerTeamId: "40",
            winnerTeamName: "Flamengo",
            resolutionType: "aggregate",
            hasExtraTimeMatch: false,
            hasPenaltiesMatch: false,
          },
        ],
      },
    };
  }

  return {
    data: {
      competition: {
        competitionKey: "copa_do_brasil",
        competitionName: "Copa do Brasil",
        seasonLabel: "2024",
        formatFamily: "knockout",
        seasonFormatCode: "cup_knockout",
        participantScope: "club",
      },
      stage: {
        stageId: "final",
        stageName: "Final",
        stageFormat: "knockout",
        stageOrder: 3,
        isCurrent: true,
      },
      ties: [
        {
          tieId: "final-1",
          tieOrder: 1,
          homeTeamId: "40",
          homeTeamName: "Flamengo",
          awayTeamId: "42",
          awayTeamName: "Palmeiras",
          matchCount: 2,
          firstLegAt: "2024-10-12",
          lastLegAt: "2024-10-19",
          homeGoals: 3,
          awayGoals: 1,
          winnerTeamId: "40",
          winnerTeamName: "Flamengo",
          resolutionType: "aggregate",
          hasExtraTimeMatch: false,
          hasPenaltiesMatch: false,
        },
      ],
    },
  };
}

function buildCupMatchesResponse() {
  return {
    data: {
      items: [
        {
          matchId: "cdp-final-1",
          fixtureId: "cdp-final-1",
          competitionId: "732",
          competitionName: "Copa do Brasil",
          seasonId: "2024",
          roundId: "final",
          kickoffAt: "2024-10-19T20:00:00Z",
          status: "FT",
          venueName: "Maracana",
          homeTeamId: "40",
          homeTeamName: "Flamengo",
          awayTeamId: "42",
          awayTeamName: "Palmeiras",
          homeScore: 2,
          awayScore: 0,
        },
      ],
    },
    meta: {
      coverage: {
        status: "complete",
        percentage: 100,
        label: "Match list coverage",
      },
    },
  };
}

function buildHybridStructureResponse() {
  return {
    data: {
      competition: {
        competitionKey: "champions_league",
        competitionName: "UEFA Champions League",
        seasonLabel: "2024/2025",
        formatFamily: "hybrid",
        seasonFormatCode: "league_phase_knockout",
        participantScope: "club",
      },
      stages: [
        {
          stageId: "league_phase",
          stageName: "League Phase",
          stageCode: "league_phase",
          stageFormat: "league_table",
          stageOrder: 1,
          standingsContextMode: null,
          bracketContextMode: null,
          groupMode: null,
          eliminationMode: null,
          isCurrent: false,
          groups: [],
          transitions: [],
        },
        {
          stageId: "round_of_16",
          stageName: "Round of 16",
          stageCode: "round_of_16",
          stageFormat: "knockout",
          stageOrder: 2,
          standingsContextMode: null,
          bracketContextMode: null,
          groupMode: null,
          eliminationMode: null,
          isCurrent: false,
          groups: [],
          transitions: [],
        },
        {
          stageId: "ucl_final",
          stageName: "Final",
          stageCode: "final",
          stageFormat: "knockout",
          stageOrder: 3,
          standingsContextMode: null,
          bracketContextMode: null,
          groupMode: null,
          eliminationMode: null,
          isCurrent: true,
          groups: [],
          transitions: [],
        },
      ],
    },
  };
}

function buildHybridStandingsResponse() {
  return {
    data: {
      competition: {
        competitionId: "2",
        competitionKey: "champions_league",
        competitionName: "UEFA Champions League",
        seasonId: "2024",
        seasonLabel: "2024/2025",
      },
      stage: {
        stageId: "league_phase",
        stageName: "League Phase",
        expectedTeams: 36,
      },
      selectedRound: {
        roundId: "8",
        label: "Rodada 8",
        startingAt: "2025-01-29",
        endingAt: "2025-01-29",
        isCurrent: true,
      },
      currentRound: {
        roundId: "8",
        label: "Rodada 8",
        startingAt: "2025-01-29",
        endingAt: "2025-01-29",
        isCurrent: true,
      },
      rounds: [],
      rows: [
        {
          position: 1,
          teamId: "40",
          teamName: "Liverpool",
          matchesPlayed: 8,
          wins: 7,
          draws: 0,
          losses: 1,
          goalsFor: 19,
          goalsAgainst: 6,
          goalDiff: 13,
          points: 21,
        },
        {
          position: 2,
          teamId: "50",
          teamName: "Barcelona",
          matchesPlayed: 8,
          wins: 6,
          draws: 1,
          losses: 1,
          goalsFor: 17,
          goalsAgainst: 8,
          goalDiff: 9,
          points: 19,
        },
      ],
    },
    meta: {
      coverage: {
        status: "complete",
        percentage: 100,
        label: "Standings coverage",
      },
    },
  };
}

function buildHybridTiesResponse(stageId: string) {
  if (stageId === "round_of_16") {
    return {
      data: {
        competition: {
          competitionKey: "champions_league",
          competitionName: "UEFA Champions League",
          seasonLabel: "2024/2025",
          formatFamily: "hybrid",
          seasonFormatCode: "league_phase_knockout",
          participantScope: "club",
        },
        stage: {
          stageId: "round_of_16",
          stageName: "Round of 16",
          stageFormat: "knockout",
          stageOrder: 2,
          isCurrent: false,
        },
        ties: [
          {
            tieId: "ucl-ro16-1",
            tieOrder: 1,
            homeTeamId: "40",
            homeTeamName: "Liverpool",
            awayTeamId: "60",
            awayTeamName: "Inter",
            matchCount: 2,
            firstLegAt: "2025-02-18",
            lastLegAt: "2025-02-26",
            homeGoals: 4,
            awayGoals: 2,
            winnerTeamId: "40",
            winnerTeamName: "Liverpool",
            resolutionType: "aggregate",
            hasExtraTimeMatch: false,
            hasPenaltiesMatch: false,
          },
        ],
      },
    };
  }

  return {
    data: {
      competition: {
        competitionKey: "champions_league",
        competitionName: "UEFA Champions League",
        seasonLabel: "2024/2025",
        formatFamily: "hybrid",
        seasonFormatCode: "league_phase_knockout",
        participantScope: "club",
      },
      stage: {
        stageId: "ucl_final",
        stageName: "Final",
        stageFormat: "knockout",
        stageOrder: 3,
        isCurrent: true,
      },
      ties: [
        {
          tieId: "ucl-final-1",
          tieOrder: 1,
          homeTeamId: "40",
          homeTeamName: "Liverpool",
          awayTeamId: "50",
          awayTeamName: "Barcelona",
          matchCount: 1,
          firstLegAt: "2025-05-31",
          lastLegAt: "2025-05-31",
          homeGoals: 2,
          awayGoals: 1,
          winnerTeamId: "40",
          winnerTeamName: "Liverpool",
          resolutionType: "single_match",
          hasExtraTimeMatch: false,
          hasPenaltiesMatch: false,
        },
      ],
    },
  };
}

async function setupSeasonSurfaceRoutes(page: Page) {
  await page.route("**/api/v1/teams**", async (route) => {
    await json(route, { data: { items: [] } });
  });

  await page.route("**/api/v1/rankings/**", async (route) => {
    const url = new URL(route.request().url());
    await json(route, buildRankingResponse(url.pathname));
  });

  await page.route("**/api/v1/competition-analytics**", async (route) => {
    const url = new URL(route.request().url());
    const competitionKey = url.searchParams.get("competitionKey");
    const seasonLabel = url.searchParams.get("seasonLabel");

    await json(route, {
      data: {
        competition: {
          competitionKey,
          competitionName: competitionKey,
          seasonLabel,
          formatFamily: competitionKey === "champions_league" ? "hybrid" : "knockout",
          seasonFormatCode: "test_format",
          participantScope: "club",
        },
        seasonSummary: {
          matchCount: 10,
          totalStages: competitionKey === "champions_league" ? 3 : 3,
          tableStages: competitionKey === "champions_league" ? 1 : 0,
          knockoutStages: 2,
          groupCount: 0,
          tieCount: 2,
          averageGoals: 2.8,
        },
        stageAnalytics: [],
        seasonComparisons: [],
      },
    });
  });

  await page.route("**/api/v1/competition-structure**", async (route) => {
    const url = new URL(route.request().url());
    const competitionKey = url.searchParams.get("competitionKey");

    if (competitionKey === "copa_do_brasil") {
      await json(route, buildCupStructureResponse());
      return;
    }

    if (competitionKey === "champions_league") {
      await json(route, buildHybridStructureResponse());
      return;
    }

    await noContent(route);
  });

  await page.route("**/api/v1/ties**", async (route) => {
    const url = new URL(route.request().url());
    const competitionKey = url.searchParams.get("competitionKey");
    const stageId = url.searchParams.get("stageId") ?? "";

    if (competitionKey === "copa_do_brasil") {
      await json(route, buildCupTiesResponse(stageId));
      return;
    }

    if (competitionKey === "champions_league") {
      await json(route, buildHybridTiesResponse(stageId));
      return;
    }

    await noContent(route);
  });

  await page.route("**/api/v1/group-standings**", async (route) => {
    await noContent(route);
  });

  await page.route("**/api/v1/standings**", async (route) => {
    const url = new URL(route.request().url());
    const competitionId = url.searchParams.get("competitionId");
    const competitionKey = url.searchParams.get("competitionKey");
    const roundId = url.searchParams.get("roundId") ?? "38";

    if (competitionId === "8" || competitionKey === "premier_league") {
      await json(route, buildLeagueStandingsResponse(roundId));
      return;
    }

    await json(route, buildHybridStandingsResponse());
  });

  await page.route("**/api/v1/matches**", async (route) => {
    const url = new URL(route.request().url());
    const competitionId = url.searchParams.get("competitionId");
    const roundId = url.searchParams.get("roundId") ?? "38";

    if (competitionId === "732") {
      await json(route, buildCupMatchesResponse());
      return;
    }

    await json(route, buildLeagueMatchesResponse(roundId));
  });
}

test.describe("Fluxo critico: competition season surface orientada por tipo", () => {
  test("liga preserva rota, compatibilidade de tab e deep links legados", async ({ page }) => {
    const seasonHubHref = "/competitions/premier_league/seasons/2024%2F2025";
    const seasonNav = page.getByLabel("Navegacao da edicao");

    await setupSeasonSurfaceRoutes(page);

    await page.goto("/competitions");

    await expect(page.getByRole("heading", { name: "Análise de Competições" })).toBeVisible();

    await page.locator(`a[href="${seasonHubHref}"]`).first().click();

    await expect(page).toHaveURL(/\/competitions\/premier_league\/seasons\/2024%2F2025$/);
    await expect(page.getByRole("heading", { name: "Premier League 2024/2025" })).toBeVisible();
    await expect(page.getByText("Liga encerrada", { exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Classificacao final" }).first()).toBeVisible();
    await expect(page.getByRole("link", { name: "Liverpool" }).first()).toBeVisible();
    await expect(page.locator("#global-filter-competition-id")).toBeEnabled();

    await seasonNav.getByRole("link", { name: "Classificacao final" }).click();

    await expect(page).toHaveURL(/tab=standings/);
    await expect(page.getByRole("heading", { name: "Classificacao final" }).first()).toBeVisible();
    await expect(page.getByText("Dados parciais.")).toBeVisible();

    await page.locator("#global-filter-round-id").fill("29");

    await expect.poll(() => page.url()).toContain("tab=standings");
    await expect.poll(() => page.url()).toContain("roundId=29");
    await expect.poll(() => page.url()).not.toContain("competitionId=");
    await expect.poll(() => page.url()).not.toContain("seasonId=");

    await seasonNav.getByRole("link", { name: "Partidas marcantes" }).click();

    await expect(page).toHaveURL(/tab=calendar/);
    await expect(page).toHaveURL(/roundId=29/);
    await expect(page.getByRole("heading", { name: "Partidas marcantes da temporada" })).toBeVisible();

    await seasonNav.getByRole("link", { name: "Destaques da edicao" }).click();

    await expect(page).toHaveURL(/tab=rankings/);
    await expect(page).toHaveURL(/roundId=29/);
    await expect(page.getByRole("heading", { name: "Artilharia" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Posse de bola" })).toBeVisible();

    await expect(page.locator('a[href^="/matches?"]').first()).toHaveAttribute("href", /competitionId=8/);
    await expect(page.locator('a[href^="/matches?"]').first()).toHaveAttribute("href", /seasonId=2024/);
    await expect(page.locator('a[href^="/matches?"]').first()).toHaveAttribute("href", /roundId=29/);
    await expect(page.locator('a[href^="/rankings/player-goals?"]').first()).toHaveAttribute("href", /competitionId=8/);
    await expect(page.locator('a[href^="/rankings/player-goals?"]').first()).toHaveAttribute("href", /seasonId=2024/);
    await expect(page.locator('a[href^="/rankings/player-goals?"]').first()).toHaveAttribute("href", /roundId=29/);
  });

  test("copa abre variante de chaveamento com caminho do campeao", async ({ page }) => {
    await setupSeasonSurfaceRoutes(page);

    await page.goto("/competitions/copa_do_brasil/seasons/2024");

    await expect(page.getByRole("heading", { name: "Copa do Brasil 2024" })).toBeVisible();
    await expect(page.getByText("Copa encerrada", { exact: true })).toBeVisible();
    await expect(page.getByLabel("Navegacao da edicao").getByRole("link", { name: "Chaveamento" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Flamengo", exact: true })).toBeVisible();

    await page.getByLabel("Navegacao da edicao").getByRole("link", { name: "Chaveamento" }).click();

    await expect(page).toHaveURL(/tab=standings/);
    await expect(page.getByRole("heading", { name: "Chaveamento finalizado" })).toBeVisible();
    await expect(page.getByText("Palmeiras").first()).toBeVisible();
  });

  test("hibrida separa fase classificatoria e mata-mata na mesma edicao", async ({ page }) => {
    await setupSeasonSurfaceRoutes(page);

    await page.goto("/competitions/champions_league/seasons/2024%2F2025");

    await expect(page.getByRole("heading", { name: "UEFA Champions League 2024/2025" })).toBeVisible();
    await expect(page.getByText("Edicao hibrida encerrada", { exact: true })).toBeVisible();
    await expect(page.getByLabel("Navegacao da edicao").getByRole("link", { name: "Fase classificatoria" })).toBeVisible();
    await expect(page.getByLabel("Navegacao da edicao").getByRole("link", { name: "Mata-mata" })).toBeVisible();

    await page.getByLabel("Navegacao da edicao").getByRole("link", { name: "Fase classificatoria" }).click();

    await expect(page).toHaveURL(/tab=standings/);
    await expect(page.getByRole("heading", { name: "League Phase" })).toBeVisible();
    await expect(page.getByRole("link", { name: "Liverpool" }).first()).toBeVisible();

    await page.getByLabel("Navegacao da edicao").getByRole("link", { name: "Mata-mata" }).click();

    await expect(page).toHaveURL(/tab=calendar/);
    await expect(page.getByRole("heading", { name: "Mata-mata finalizado" })).toBeVisible();
    await expect(page.getByText("Final", { exact: true })).toBeVisible();
  });
});
