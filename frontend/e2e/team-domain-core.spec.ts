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

const TEAM_CONTEXT_QUERY = "competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5";
const TEAM_CANONICAL_URL =
  "/competitions/premier_league/seasons/2024%2F2025/teams/40?roundId=29&venue=home&lastN=5";

async function installTeamDomainMocks(page: Page) {
  await page.route("**/api/v1/**", async (route) => {
    const requestUrl = new URL(route.request().url());
    const pathname = requestUrl.pathname;

    if (pathname === "/api/v1/teams") {
      await json(route, {
        data: {
          items: [
            {
              teamId: "40",
              teamName: "Liverpool",
              competitionId: "8",
              seasonId: "2024",
              position: 1,
              totalTeams: 20,
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
        },
        meta: {
          coverage: {
            status: "complete",
            percentage: 100,
            label: "Teams list coverage",
          },
          pagination: {
            page: 1,
            pageSize: 40,
            totalCount: 1,
            totalPages: 1,
            hasNextPage: false,
            hasPreviousPage: false,
          },
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
          squad: [
            {
              playerId: "306",
              playerName: "Mohamed Salah",
              positionName: "RW",
              shirtNumber: 11,
              appearances: 29,
              starts: 28,
              minutesPlayed: 2520,
              averageMinutes: 86.9,
              lastAppearanceAt: "2025-04-16T19:00:00Z",
            },
            {
              playerId: "307",
              playerName: "Virgil van Dijk",
              positionName: "CB",
              shirtNumber: 4,
              appearances: 29,
              starts: 29,
              minutesPlayed: 2610,
              averageMinutes: 90,
              lastAppearanceAt: "2025-04-16T19:00:00Z",
            },
          ],
          stats: {
            pointsPerMatch: 2.41,
            winRatePct: 72.41,
            goalsForPerMatch: 2.38,
            goalsAgainstPerMatch: 0.93,
            cleanSheets: 12,
            failedToScore: 2,
            trend: [
              {
                periodKey: "2025-04",
                label: "04/2025",
                matches: 4,
                wins: 3,
                draws: 1,
                losses: 0,
                goalsFor: 9,
                goalsAgainst: 3,
                goalDiff: 6,
                points: 10,
              },
            ],
          },
          sectionCoverage: {
            overview: {
              status: "complete",
              percentage: 100,
              label: "Team overview coverage",
            },
            squad: {
              status: "partial",
              percentage: 82.5,
              label: "Squad coverage",
            },
            stats: {
              status: "complete",
              percentage: 100,
              label: "Team stats coverage",
            },
          },
        },
        meta: {
          coverage: {
            status: "partial",
            percentage: 94.17,
            label: "Team profile coverage",
          },
        },
      });
      return;
    }

    if (pathname === "/api/v1/matches") {
      await json(route, {
        data: {
          items: [
            {
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
      });
      return;
    }

    if (pathname === "/api/v1/matches/19135048") {
      await json(route, {
        data: {
          match: {
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
          },
          timeline: [
            {
              eventId: "goal-1",
              minute: 27,
              type: "goal",
              detail: "Gol de pé esquerdo",
              teamId: "40",
              teamName: "Liverpool",
              playerId: "306",
              playerName: "Mohamed Salah",
            },
          ],
          lineups: [],
          teamStats: [],
          playerStats: [],
          sectionCoverage: {
            timeline: {
              status: "complete",
              percentage: 100,
              label: "Timeline",
            },
            lineups: {
              status: "empty",
              percentage: 0,
              label: "Lineups",
            },
            teamStats: {
              status: "empty",
              percentage: 0,
              label: "Team stats",
            },
            playerStats: {
              status: "empty",
              percentage: 0,
              label: "Player stats",
            },
          },
        },
        meta: {
          coverage: {
            status: "partial",
            percentage: 55,
            label: "Match center sections coverage",
          },
        },
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
            passesAttempted: 812,
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
        meta: {
          coverage: {
            status: "complete",
            percentage: 100,
            label: "Player profile coverage",
          },
        },
      });
      return;
    }

    if (pathname === "/api/v1/rankings/player-goals") {
      await json(route, {
        data: {
          rankingId: "player-goals",
          metricKey: "goals",
          title: "Artilharia",
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
        },
        meta: {
          coverage: {
            status: "complete",
            percentage: 100,
            label: "Rankings coverage",
          },
        },
      });
      return;
    }

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
            label: "Rodada 29",
            isCurrent: false,
          },
          currentRound: {
            roundId: "38",
            label: "Rodada 38",
            isCurrent: true,
          },
          rounds: [
            {
              roundId: "29",
              label: "Rodada 29",
              isCurrent: false,
            },
          ],
          rows: [
            {
              position: 1,
              teamId: "40",
              teamName: "Liverpool",
              points: 70,
              goalDiff: 42,
              goalsFor: 69,
              matchesPlayed: 29,
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
      });
      return;
    }

    if (pathname === "/api/v1/insights") {
      await json(route, {
        data: [],
        meta: {
          coverage: {
            status: "complete",
            percentage: 100,
            label: "Insights coverage",
          },
        },
      });
      return;
    }

    await json(route, {
      data: [],
      meta: {
        coverage: {
          status: "complete",
          percentage: 100,
          label: "Default coverage",
        },
      },
    });
  });
}

test.describe("Domínio teams", () => {
  test("fecha lista, profile, seções e navegação contextual do time", async ({ page }) => {
    await installTeamDomainMocks(page);

    await page.goto(`/teams?${TEAM_CONTEXT_QUERY}`);

    await expect(
      page.getByRole("heading", { name: "Times em Premier League 2024/2025" }),
    ).toBeVisible();
    const teamProfileLink = page.getByRole("link", { name: "Abrir perfil de Liverpool" });
    await expect(teamProfileLink).toHaveAttribute(
      "href",
      "/competitions/premier_league/seasons/2024%2F2025/teams/40?roundId=29&venue=home&lastN=5",
    );

    await teamProfileLink.click();
    await expect(page).toHaveURL(TEAM_CANONICAL_URL);
    await expect(page.getByRole("heading", { exact: true, name: "Liverpool" })).toBeVisible();

    await page.getByRole("link", { name: /Elenco/i }).click();
    await expect(page).toHaveURL(/tab=squad/);
    await expect(page.getByRole("heading", { name: "Elenco por minutos jogados" })).toBeVisible();
    const playerLink = page.getByRole("link", { name: "Mohamed Salah" }).first();
    await expect(playerLink).toHaveAttribute(
      "href",
      "/competitions/premier_league/seasons/2024%2F2025/players/306?roundId=29&venue=home&lastN=5",
    );
    await playerLink.click();

    await expect(page).toHaveURL(
      "/competitions/premier_league/seasons/2024%2F2025/players/306?roundId=29&venue=home&lastN=5",
    );
    await expect(page.getByRole("heading", { name: "Mohamed Salah" })).toBeVisible();

    await page.goto(TEAM_CANONICAL_URL);
    await page
      .getByRole("navigation", { name: "Team profile tabs" })
      .getByRole("link", { name: /Partidas/i })
      .click();
    await expect(page).toHaveURL(/tab=matches/);
    await expect(page.getByRole("heading", { name: "Calendário e resultados do clube" })).toBeVisible();
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
    await expect(page.getByRole("heading", { name: "Liverpool vs Crystal Palace" })).toBeVisible();

    await page.goto(TEAM_CANONICAL_URL);
    await page
      .getByRole("navigation", { name: "Team profile tabs" })
      .getByRole("link", { name: /Stats/i })
      .click();
    await expect(page).toHaveURL(/tab=stats/);
    await expect(page.getByRole("heading", { name: "Tendência e métricas agregadas" })).toBeVisible();
    await expect(page.getByText("04/2025")).toBeVisible();

    await page.goto(TEAM_CANONICAL_URL);
    await page.getByRole("link", { name: "Temporada" }).click();
    await expect(page).toHaveURL(
      /\/competitions\/premier_league\/seasons\/2024%2F2025\?(?:tab=standings&roundId=29&venue=home&lastN=5|roundId=29&venue=home&lastN=5&tab=standings)/,
    );
    await expect(
      page.getByRole("heading", { name: "Premier League 2024/2025" }),
    ).toBeVisible();

    await page.goto(TEAM_CANONICAL_URL);
    await page.getByRole("link", { name: "Rankings" }).first().click();
    await expect(page).toHaveURL(
      "/rankings/player-goals?competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5",
    );
    await expect(page.getByRole("heading", { name: "Artilharia" })).toBeVisible();
  });
});
