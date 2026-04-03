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

const PLAYER_CONTEXT_QUERY = "competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5";
const PLAYER_CANONICAL_URL =
  "/competitions/premier_league/seasons/2024%2F2025/players/306?roundId=29&venue=home&lastN=5";

async function installPlayerDomainMocks(page: Page) {
  await page.route("**/api/v1/**", async (route) => {
    const requestUrl = new URL(route.request().url());
    const pathname = requestUrl.pathname;

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
              shotsTotal: 98,
              yellowCards: 1,
              redCards: 0,
              rating: 8.2,
            },
          ],
        },
        meta: {
          coverage: {
            status: "complete",
            percentage: 100,
            label: "Players list coverage",
          },
          pagination: {
            page: 1,
            pageSize: 20,
            totalCount: 1,
            totalPages: 1,
            hasNextPage: false,
            hasPreviousPage: false,
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
            lastMatchAt: "2025-04-16T19:00:00Z",
          },
          summary: {
            matchesPlayed: 29,
            minutesPlayed: 2520,
            goals: 23,
            assists: 14,
            shotsTotal: 98,
            shotsOnTarget: 44,
            passesAttempted: 812,
            passAccuracyPct: null,
            yellowCards: 1,
            redCards: 0,
            rating: 8.2,
          },
          recentMatches: [
            {
              fixtureId: "19135048",
              matchId: "19135048",
              playedAt: "2025-04-16T19:00:00Z",
              competitionId: "8",
              competitionName: "Premier League",
              seasonId: "2024",
              roundId: "29",
              teamId: "40",
              teamName: "Liverpool",
              opponentTeamId: "50",
              opponentName: "Crystal Palace",
              venue: "home",
              goalsFor: 2,
              goalsAgainst: 1,
              result: "win",
              minutesPlayed: 90,
              goals: 1,
              assists: 1,
              shotsTotal: 5,
              shotsOnTarget: 3,
              passesAttempted: 28,
              rating: 8.9,
            },
          ],
          history: [
            {
              competitionId: "8",
              competitionKey: "premier_league",
              competitionName: "Premier League",
              seasonId: "2024",
              seasonLabel: "2024/2025",
              teamId: "40",
              teamName: "Liverpool",
              matchesPlayed: 29,
              minutesPlayed: 2520,
              goals: 23,
              assists: 14,
              rating: 8.2,
              lastMatchAt: "2025-04-16T19:00:00Z",
            },
            {
              competitionId: "2",
              competitionKey: "champions_league",
              competitionName: "UEFA Champions League",
              seasonId: "2024",
              seasonLabel: "2024/2025",
              teamId: "40",
              teamName: "Liverpool",
              matchesPlayed: 9,
              minutesPlayed: 780,
              goals: 5,
              assists: 3,
              rating: 8.0,
              lastMatchAt: "2025-03-01T19:00:00Z",
            },
          ],
          stats: {
            minutesPerMatch: 86.9,
            goalsPer90: 0.82,
            assistsPer90: 0.5,
            goalContributionsPer90: 1.32,
            shotsPer90: 3.5,
            shotsOnTargetPer90: 1.57,
            shotsOnTargetPct: 44.9,
            passesAttemptedPer90: 29,
            yellowCardsPer90: 0.04,
            redCardsPer90: 0,
            trend: [
              {
                periodKey: "2025-04",
                label: "04/2025",
                matchesPlayed: 4,
                minutesPlayed: 360,
                goals: 3,
                assists: 2,
                shotsTotal: 16,
                shotsOnTarget: 7,
                passesAttempted: 112,
                rating: 8.7,
              },
            ],
          },
          sectionCoverage: {
            overview: {
              status: "partial",
              percentage: 87.5,
              label: "Player overview coverage",
            },
            history: {
              status: "complete",
              percentage: 100,
              label: "Player history coverage",
            },
            matches: {
              status: "partial",
              percentage: 87.5,
              label: "Player matches coverage",
            },
            stats: {
              status: "partial",
              percentage: 87.5,
              label: "Player stats coverage",
            },
          },
        },
        meta: {
          coverage: {
            status: "partial",
            percentage: 90.63,
            label: "Player profile coverage",
          },
        },
      });
      return;
    }

    if (pathname === "/api/v1/insights") {
      await json(route, {
        data: [
          {
            insight_id: "player-form-1",
            severity: "info",
            explanation: "Participação ofensiva acima da média recente do recorte.",
            evidences: {
              goals: 23,
              assists: 14,
            },
            reference_period: "Últimos 29 jogos",
            data_source: ["mart.player_match_summary"],
          },
        ],
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
              detail: "Gol",
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
              status: "complete",
              percentage: 100,
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
            status: "complete",
            percentage: 100,
            label: "Team profile coverage",
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
            label: "Standings coverage",
          },
        },
      });
      return;
    }

    if (pathname.startsWith("/api/v1/rankings/")) {
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
          updatedAt: "2026-03-22T12:00:00Z",
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

test.describe("Domínio players", () => {
  test("fecha lista, profile, seções e navegação contextual do jogador", async ({ page }) => {
    await installPlayerDomainMocks(page);

    await page.goto(`/players?${PLAYER_CONTEXT_QUERY}`);

    await expect(page.getByRole("heading", { exact: true, name: "Jogadores" })).toBeVisible();
    await expect(page.getByText("Egypt").first()).toBeVisible();
    const playerProfileLink = page.getByRole("link", { name: "Mohamed Salah" }).first();
    await expect(playerProfileLink).toHaveAttribute("href", PLAYER_CANONICAL_URL);

    await playerProfileLink.click();
    await expect(page).toHaveURL(PLAYER_CANONICAL_URL);
    await expect(page.getByRole("heading", { exact: true, name: "Mohamed Salah" })).toBeVisible();
    await expect(page.getByText("Egypt").first()).toBeVisible();

    await page
      .getByRole("navigation", { name: "Player profile tabs" })
      .getByRole("link", { name: /History/i })
      .click();
    await expect(page).toHaveURL(/tab=history/);
    await expect(page.getByRole("heading", { name: "Participação por contexto" })).toBeVisible();

    await page
      .getByRole("navigation", { name: "Player profile tabs" })
      .getByRole("link", { name: /Matches/i })
      .click();
    await expect(page).toHaveURL(/tab=matches/);
    await expect(page.getByRole("heading", { name: "Match log do jogador" })).toBeVisible();
    const matchCenterLink = page.getByRole("link", {
      name: "Abrir match center de Liverpool contra Crystal Palace",
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

    await page.goto(PLAYER_CANONICAL_URL);
    await page
      .getByRole("navigation", { name: "Player profile tabs" })
      .getByRole("link", { name: /Stats/i })
      .click();
    await expect(page).toHaveURL(/tab=stats/);
    await expect(page.getByRole("heading", { name: "Produção agregada e tendência" })).toBeVisible();
    await expect(page.getByText("04/2025")).toBeVisible();

    await page.goto(PLAYER_CANONICAL_URL);
    await page.getByRole("link", { exact: true, name: "Time" }).click();
    await expect(page).toHaveURL(
      "/competitions/premier_league/seasons/2024%2F2025/teams/40?roundId=29&venue=home&lastN=5",
    );
    await expect(page.getByRole("heading", { exact: true, name: "Liverpool" })).toBeVisible();

    await page.goto(PLAYER_CANONICAL_URL);
    await page.getByRole("link", { exact: true, name: "Temporada" }).click();
    await expect(page).toHaveURL(
      "/competitions/premier_league/seasons/2024%2F2025?roundId=29&venue=home&lastN=5",
    );
    await expect(page.getByRole("heading", { name: "Premier League 2024/2025" })).toBeVisible();
  });
});
