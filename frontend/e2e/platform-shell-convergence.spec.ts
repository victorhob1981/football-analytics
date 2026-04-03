import { expect, test, type Page, type Route } from "@playwright/test";

type ApiPayload = {
  data: unknown;
  meta?: Record<string, unknown>;
};

function json(route: Route, body: ApiPayload, status = 200): Promise<void> {
  return route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify(body),
  });
}

const COMPLETE_COVERAGE = {
  coverage: {
    status: "complete",
    percentage: 100,
    label: "Cobertura completa",
  },
};

const PARTIAL_COVERAGE = {
  coverage: {
    status: "partial",
    percentage: 62.5,
    label: "Cobertura parcial",
  },
};

const MATCH_ITEM = {
  matchId: "19135048",
  fixtureId: "19135048",
  competitionId: "8",
  competitionName: "Premier League",
  seasonId: "2024",
  roundId: "29",
  kickoffAt: "2025-04-16T19:00:00Z",
  status: "FT",
  venueName: "Anfield",
  homeTeamId: "40",
  homeTeamName: "Liverpool",
  awayTeamId: "50",
  awayTeamName: "Crystal Palace",
  homeScore: 2,
  awayScore: 1,
};

async function installPlatformShellMocks(page: Page) {
  await page.route("**/api/v1/**", async (route) => {
    const requestUrl = new URL(route.request().url());
    const pathname = requestUrl.pathname;

    if (pathname === "/api/v1/standings") {
      await json(route, {
        data: {
          competition: {
            competitionId: "8",
            competitionKey: "premier_league",
            competitionName: "Premier League",
            seasonId: "2024",
            seasonLabel: "2024/2025",
          },
          stage: {
            stageId: "regular-season",
            stageName: "Regular Season",
            expectedTeams: 20,
          },
          selectedRound: {
            roundId: "29",
            providerRoundId: "339264",
            roundName: "29",
            label: "Rodada 29",
            isCurrent: false,
          },
          currentRound: {
            roundId: "38",
            providerRoundId: "339273",
            roundName: "38",
            label: "Rodada 38",
            isCurrent: true,
          },
          rounds: [
            {
              roundId: "29",
              providerRoundId: "339264",
              roundName: "29",
              label: "Rodada 29",
              isCurrent: false,
            },
            {
              roundId: "38",
              providerRoundId: "339273",
              roundName: "38",
              label: "Rodada 38",
              isCurrent: true,
            },
          ],
          rows: [
            {
              position: 1,
              teamId: "40",
              teamName: "Liverpool",
              matchesPlayed: 29,
              wins: 21,
              draws: 7,
              losses: 1,
              goalsFor: 69,
              goalsAgainst: 27,
              goalDiff: 42,
              points: 70,
            },
          ],
          updatedAt: "2026-03-21T18:00:00Z",
        },
        meta: PARTIAL_COVERAGE,
      });
      return;
    }

    if (pathname === "/api/v1/players") {
      await json(route, {
        data: {
          items: [
            {
              playerId: "306",
              playerName: "Mohamed Salah",
              teamId: "40",
              teamName: "Liverpool",
              nationality: "Egypt",
              position: "RW",
              matchesPlayed: 29,
              minutesPlayed: 2520,
              goals: 23,
              assists: 14,
              rating: 8.2,
            },
          ],
        },
        meta: COMPLETE_COVERAGE,
      });
      return;
    }

    if (pathname === "/api/v1/players/306") {
      await json(route, {
        data: {
          player: {
            playerId: "306",
            playerName: "Mohamed Salah",
            teamId: "40",
            teamName: "Liverpool",
            position: "RW",
            nationality: "Egypt",
          },
          summary: {
            matchesPlayed: 29,
            minutesPlayed: 2520,
            goals: 23,
            assists: 14,
            shotsTotal: 98,
            shotsOnTarget: 44,
            passAccuracyPct: 82.4,
            yellowCards: 1,
            redCards: 0,
            rating: 8.2,
          },
          recentMatches: [
            {
              fixtureId: "19135048",
              playedAt: "2025-04-16T19:00:00Z",
              opponentName: "Crystal Palace",
              minutesPlayed: 90,
              goals: 1,
              assists: 1,
              rating: 8.4,
            },
          ],
        },
        meta: COMPLETE_COVERAGE,
      });
      return;
    }

    if (pathname === "/api/v1/players/306/contexts") {
      await json(route, {
        data: {
          defaultContext: {
            competitionId: "8",
            competitionKey: "premier_league",
            competitionName: "Premier League",
            seasonId: "2024",
            seasonLabel: "2024/2025",
          },
          availableContexts: [
            {
              competitionId: "8",
              competitionKey: "premier_league",
              competitionName: "Premier League",
              seasonId: "2024",
              seasonLabel: "2024/2025",
            },
          ],
        },
      });
      return;
    }

    if (pathname === "/api/v1/teams/40") {
      await json(route, {
        data: {
          team: {
            teamId: "40",
            teamName: "Liverpool",
            competitionId: "8",
            competitionName: "Premier League",
            seasonId: "2024",
            seasonLabel: "2024/2025",
          },
          summary: {
            matchesPlayed: 29,
            wins: 21,
            draws: 7,
            losses: 1,
            goalsFor: 69,
            goalsAgainst: 27,
            goalDiff: 42,
            points: 70,
          },
          standing: {
            position: 1,
            totalTeams: 20,
          },
          form: ["win", "win", "draw", "win", "win"],
          recentMatches: [
            {
              matchId: "19135048",
              playedAt: "2025-04-16T19:00:00Z",
              opponentTeamId: "50",
              opponentName: "Crystal Palace",
              venue: "home",
              goalsFor: 2,
              goalsAgainst: 1,
              result: "win",
            },
          ],
        },
        meta: COMPLETE_COVERAGE,
      });
      return;
    }

    if (pathname === "/api/v1/teams/40/contexts") {
      await json(route, {
        data: {
          defaultContext: {
            competitionId: "8",
            competitionKey: "premier_league",
            competitionName: "Premier League",
            seasonId: "2024",
            seasonLabel: "2024/2025",
          },
          availableContexts: [
            {
              competitionId: "8",
              competitionKey: "premier_league",
              competitionName: "Premier League",
              seasonId: "2024",
              seasonLabel: "2024/2025",
            },
          ],
        },
      });
      return;
    }

    if (pathname === "/api/v1/matches") {
      await json(route, {
        data: {
          items: [MATCH_ITEM],
        },
        meta: COMPLETE_COVERAGE,
      });
      return;
    }

    if (pathname === "/api/v1/matches/19135048") {
      await json(route, {
        data: {
          match: MATCH_ITEM,
          timeline: [
            {
              eventId: "goal-1",
              minute: 27,
              type: "goal",
              detail: "Gol",
              teamId: "40",
              teamName: "Liverpool",
              playerId: "306",
              playerName: "Mohamed Salah",
            },
          ],
          lineups: [
            {
              playerId: "306",
              playerName: "Mohamed Salah",
              teamId: "40",
              teamName: "Liverpool",
              position: "RW",
              shirtNumber: 11,
              isStarter: true,
            },
          ],
          playerStats: [
            {
              playerId: "306",
              playerName: "Mohamed Salah",
              teamId: "40",
              teamName: "Liverpool",
              minutesPlayed: 90,
              goals: 1,
              assists: 1,
              shotsTotal: 5,
              rating: 8.4,
            },
          ],
        },
        meta: COMPLETE_COVERAGE,
      });
      return;
    }

    if (pathname.startsWith("/api/v1/rankings/")) {
      await json(route, {
        data: {
          rankingId: "player-goals",
          metricKey: "goals",
          rows: [
            {
              entityId: "306",
              entityName: "Mohamed Salah",
              teamId: "40",
              teamName: "Liverpool",
              rank: 1,
              metricValue: 23,
              position: "RW",
            },
          ],
          updatedAt: "2026-03-21T18:00:00Z",
        },
        meta: COMPLETE_COVERAGE,
      });
      return;
    }

    if (pathname === "/api/v1/insights") {
      await json(route, {
        data: [],
        meta: COMPLETE_COVERAGE,
      });
      return;
    }

    await json(route, {
      data: [],
      meta: COMPLETE_COVERAGE,
    });
  });
}

test.describe("Convergencia estrutural da shell global", () => {
  test("preserva recorte explicito entre season hub, players, team, matches, match center e rankings", async ({
    page,
  }) => {
    await installPlatformShellMocks(page);

    await page.goto(
      "/competitions/premier_league/seasons/2024%2F2025?tab=standings&roundId=29&venue=home&lastN=5",
    );

    await expect(
      page.getByRole("heading", { name: "Premier League 2024/2025" }),
    ).toBeVisible();
    await expect(
      page.getByRole("region", { name: "Contexto da página" }),
    ).toBeVisible();

    const mainNav = page.getByRole("navigation", { name: "Navegação principal" });
    await expect(mainNav.getByRole("link", { name: "Partidas" })).toHaveAttribute(
      "href",
      "/matches?competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5",
    );
    await expect(mainNav.getByRole("link", { name: "Rankings" })).toHaveAttribute(
      "href",
      "/rankings/player-goals?competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5",
    );
    await expect(mainNav.getByRole("link", { name: "Jogadores" })).toHaveAttribute(
      "href",
      "/players?competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5",
    );

    const frameNav = page.getByRole("navigation", {
      name: "Atalhos da página",
    });
    await expect(frameNav.getByRole("link", { name: "Tabela" })).toHaveAttribute(
      "href",
      "/competitions/premier_league/seasons/2024%2F2025?roundId=29&venue=home&lastN=5&tab=standings",
    );
    await frameNav.getByRole("link", { name: "Jogadores" }).click();

    await expect(page).toHaveURL(
      "/players?competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5",
    );
    await expect(page.getByRole("heading", { exact: true, name: "Jogadores" })).toBeVisible();
    await page.getByRole("link", { name: "Mohamed Salah" }).first().click();

    await expect(page).toHaveURL(
      "/competitions/premier_league/seasons/2024%2F2025/players/306?roundId=29&venue=home&lastN=5",
    );
    await expect(page.getByRole("heading", { name: "Mohamed Salah" })).toBeVisible();

    await page.getByRole("link", { name: "Liverpool" }).first().click();

    await expect(page).toHaveURL(
      "/competitions/premier_league/seasons/2024%2F2025/teams/40?roundId=29&venue=home&lastN=5",
    );
    await expect(page.getByRole("heading", { exact: true, name: "Liverpool" })).toBeVisible();

    const openMatchesLink = page.getByRole("link", { name: "Abrir partidas" });
    await expect(openMatchesLink).toHaveAttribute(
      "href",
      "/matches?competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5",
    );
    await openMatchesLink.click();

    await expect(page).toHaveURL(
      "/matches?competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5",
    );
    await expect(page.getByRole("heading", { exact: true, name: "Partidas" })).toBeVisible();

    const matchCenterLink = page.getByRole("link", {
      name: "Abrir match center de Liverpool vs Crystal Palace",
    });
    await expect(matchCenterLink).toHaveAttribute(
      "href",
      "/matches/19135048?competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5",
    );
    await matchCenterLink.click();

    await expect(page).toHaveURL(
      "/matches/19135048?competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5",
    );
    await expect(
      page.getByRole("heading", { name: "Liverpool vs Crystal Palace" }),
    ).toBeVisible();

    await page.getByRole("navigation", { name: "Navegação principal" }).getByRole("link", {
      name: "Rankings",
    }).click();

    await expect(page).toHaveURL(
      "/rankings/player-goals?competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5",
    );
    await expect(page.getByRole("heading", { name: "Artilharia" })).toBeVisible();
  });

  test("rotas legadas relevantes aterrissam na shell convergida", async ({ page }) => {
    await installPlatformShellMocks(page);

    await page.goto("/competition/8");
    await expect(page).toHaveURL("/competitions/premier_league");
    await expect(page.getByRole("heading", { name: "Premier League" })).toBeVisible();

    await page.goto("/clubs/40");
    await expect(page).toHaveURL(
      "/competitions/premier_league/seasons/2024%2F2025/teams/40",
    );
    await expect(page.getByRole("heading", { name: "Liverpool" })).toBeVisible();
    await expect(
      page.getByRole("region", { name: "Contexto da página" }),
    ).toBeVisible();
  });
});
