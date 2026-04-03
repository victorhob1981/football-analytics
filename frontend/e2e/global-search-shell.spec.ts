import { expect, test, type Route } from "@playwright/test";

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

function buildEmptyGroups() {
  return [
    { type: "competition", total: 0, items: [] },
    { type: "team", total: 0, items: [] },
    { type: "player", total: 0, items: [] },
    { type: "match", total: 0, items: [] },
  ];
}

function buildSearchPayload(query: string): ApiPayload {
  const normalizedQuery = query.trim().toLowerCase();

  if (normalizedQuery === "prem" || normalizedQuery === "slow") {
    return {
      data: {
        query: normalizedQuery,
        totalResults: 1,
        groups: [
          {
            type: "competition",
            total: 1,
            items: [
              {
                competitionId: "8",
                competitionKey: "premier_league",
                competitionName: "Premier League",
              },
            ],
          },
          ...buildEmptyGroups().slice(1),
        ],
      },
      meta: {
        coverage: {
          status: "complete",
          percentage: 100,
          label: "Global search navigability coverage",
        },
      },
    };
  }

  if (normalizedQuery === "salah") {
    return {
      data: {
        query: "salah",
        totalResults: 1,
        groups: [
          buildEmptyGroups()[0],
          buildEmptyGroups()[1],
          {
            type: "player",
            total: 1,
            items: [
              {
                playerId: "306",
                playerName: "Mohamed Salah",
                teamId: "40",
                teamName: "Liverpool",
                position: "RW",
                defaultContext: {
                  competitionId: "8",
                  competitionKey: "premier_league",
                  competitionName: "Premier League",
                  seasonId: "2024",
                  seasonLabel: "2024/2025",
                },
              },
            ],
          },
          buildEmptyGroups()[3],
        ],
      },
      meta: {
        coverage: {
          status: "complete",
          percentage: 100,
          label: "Global search navigability coverage",
        },
      },
    };
  }

  if (normalizedQuery === "liv") {
    return {
      data: {
        query: "liv",
        totalResults: 1,
        groups: [
          buildEmptyGroups()[0],
          {
            type: "team",
            total: 1,
            items: [
              {
                teamId: "40",
                teamName: "Liverpool",
                defaultContext: {
                  competitionId: "8",
                  competitionKey: "premier_league",
                  competitionName: "Premier League",
                  seasonId: "2024",
                  seasonLabel: "2024/2025",
                },
              },
            ],
          },
          buildEmptyGroups()[2],
          buildEmptyGroups()[3],
        ],
      },
      meta: {
        coverage: {
          status: "complete",
          percentage: 100,
          label: "Global search navigability coverage",
        },
      },
    };
  }

  if (normalizedQuery === "flamengo" || normalizedQuery === "fla") {
    return {
      data: {
        query: normalizedQuery,
        totalResults: 1,
        groups: [
          buildEmptyGroups()[0],
          {
            type: "team",
            total: 1,
            items: [
              {
                teamId: "20",
                teamName: "Flamengo",
                defaultContext: {
                  competitionId: "71",
                  competitionKey: "brasileirao_a",
                  competitionName: "Campeonato Brasileiro Serie A",
                  seasonId: "2025",
                  seasonLabel: "2025",
                },
              },
            ],
          },
          buildEmptyGroups()[2],
          buildEmptyGroups()[3],
        ],
      },
      meta: {
        coverage: {
          status: "complete",
          percentage: 100,
          label: "Global search navigability coverage",
        },
      },
    };
  }

  if (normalizedQuery === "19135048") {
    return {
      data: {
        query: "19135048",
        totalResults: 1,
        groups: [
          buildEmptyGroups()[0],
          buildEmptyGroups()[1],
          buildEmptyGroups()[2],
          {
            type: "match",
            total: 1,
            items: [
              {
                matchId: "19135048",
                competitionId: "8",
                competitionName: "Premier League",
                seasonId: "2024",
                roundId: "38",
                kickoffAt: "2025-05-25T15:00:00Z",
                status: "FT",
                homeTeamId: "40",
                homeTeamName: "Liverpool",
                awayTeamId: "50",
                awayTeamName: "Crystal Palace",
                homeScore: 2,
                awayScore: 1,
                defaultContext: {
                  competitionId: "8",
                  competitionKey: "premier_league",
                  competitionName: "Premier League",
                  seasonId: "2024",
                  seasonLabel: "2024/2025",
                },
              },
            ],
          },
        ],
      },
      meta: {
        coverage: {
          status: "complete",
          percentage: 100,
          label: "Global search navigability coverage",
        },
      },
    };
  }

  if (normalizedQuery === "partial") {
    return {
      data: {
        query: "partial",
        totalResults: 1,
        groups: [
          buildEmptyGroups()[0],
          {
            type: "team",
            total: 1,
            items: [
              {
                teamId: "40",
                teamName: "Liverpool",
                defaultContext: {
                  competitionId: "8",
                  competitionKey: "premier_league",
                  competitionName: "Premier League",
                  seasonId: "2024",
                  seasonLabel: "2024/2025",
                },
              },
            ],
          },
          buildEmptyGroups()[2],
          buildEmptyGroups()[3],
        ],
      },
      meta: {
        coverage: {
          status: "partial",
          percentage: 50,
          label: "Global search navigability coverage",
        },
      },
    };
  }

  return {
    data: {
      query,
      totalResults: 0,
      groups: buildEmptyGroups(),
    },
    meta: {
      coverage: {
        status: "complete",
        percentage: 100,
        label: "Global search navigability coverage",
      },
    },
  };
}

test.describe("Shell global + busca global", () => {
  test("navega com seguranca para competition, player, team e match", async ({ page }) => {
    await page.route("**/api/v1/search**", async (route) => {
      const url = new URL(route.request().url());
      const query = url.searchParams.get("q") ?? "";
      await json(route, buildSearchPayload(query));
    });

    await page.route("**/api/v1/players/306/contexts**", async (route) => {
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
    });

    await page.route("**/api/v1/players/306?**", async (route) => {
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
            matchesPlayed: 38,
            minutesPlayed: 3200,
            goals: 28,
            assists: 18,
            shotsTotal: 120,
            shotsOnTarget: 58,
            passesAttempted: 1480,
            yellowCards: 1,
            redCards: 0,
            rating: 8.2,
          },
          recentMatches: [
            {
              fixtureId: "19135048",
              playedAt: "2025-05-25T15:00:00Z",
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
    });

    await page.route("**/api/v1/teams/40/contexts**", async (route) => {
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
    });

    await page.route("**/api/v1/teams/40?**", async (route) => {
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
            matchesPlayed: 38,
            wins: 25,
            draws: 9,
            losses: 4,
            goalsFor: 86,
            goalsAgainst: 41,
            goalDiff: 45,
            points: 84,
          },
          standing: {
            position: 1,
            totalTeams: 20,
          },
          form: ["win", "win", "draw", "win", "win"],
          recentMatches: [
            {
              matchId: "19135048",
              playedAt: "2025-05-25T15:00:00Z",
              opponentTeamId: "50",
              opponentName: "Crystal Palace",
              venue: "home",
              goalsFor: 2,
              goalsAgainst: 1,
              result: "win",
            },
          ],
        },
        meta: {
          coverage: {
            status: "complete",
            percentage: 100,
            label: "Team profile coverage",
          },
        },
      });
    });

    await page.route("**/api/v1/teams/20/contexts**", async (route) => {
      await json(route, {
        data: {
          defaultContext: {
            competitionId: "71",
            competitionKey: "brasileirao_a",
            competitionName: "Campeonato Brasileiro Serie A",
            seasonId: "2025",
            seasonLabel: "2025",
          },
          availableContexts: [
            {
              competitionId: "71",
              competitionKey: "brasileirao_a",
              competitionName: "Campeonato Brasileiro Serie A",
              seasonId: "2025",
              seasonLabel: "2025",
            },
          ],
        },
      });
    });

    await page.route("**/api/v1/teams/20?**", async (route) => {
      await json(route, {
        data: {
          team: {
            teamId: "20",
            teamName: "Flamengo",
            competitionId: "71",
            competitionName: "Campeonato Brasileiro Serie A",
            seasonId: "2025",
            seasonLabel: "2025",
          },
          summary: {
            matchesPlayed: 20,
            wins: 13,
            draws: 4,
            losses: 3,
            goalsFor: 34,
            goalsAgainst: 15,
            goalDiff: 19,
            points: 43,
          },
          standing: {
            position: 1,
            totalTeams: 20,
          },
          form: ["win", "win", "draw", "win", "loss"],
          recentMatches: [
            {
              matchId: "201",
              playedAt: "2025-05-25T15:00:00Z",
              opponentTeamId: "33",
              opponentName: "Palmeiras",
              venue: "home",
              goalsFor: 2,
              goalsAgainst: 1,
              result: "win",
            },
          ],
        },
        meta: {
          coverage: {
            status: "complete",
            percentage: 100,
            label: "Team profile coverage",
          },
        },
      });
    });

    await page.route("**/api/v1/matches/19135048?**", async (route) => {
      await json(route, {
        data: {
          match: {
            matchId: "19135048",
            fixtureId: "19135048",
            competitionId: "8",
            competitionName: "Premier League",
            seasonId: "2024",
            roundId: "38",
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
          timeline: [
            {
              eventId: "1",
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
        meta: {
          coverage: {
            status: "complete",
            percentage: 100,
            label: "Match center sections coverage",
          },
        },
      });
    });

    await page.goto("/competitions");

    const openSearchButton = page.getByRole("button", {
      name: /Buscar competicoes, partidas, times ou jogadores/i,
    });
    await expect(openSearchButton).toBeVisible();

    await openSearchButton.click();
    const searchDialog = page.getByRole("dialog");
    await expect(searchDialog).toBeVisible();

    const searchInput = searchDialog.locator("#global-search-input");
    await searchInput.fill("prem");
    await expect(searchDialog.getByText("Competições", { exact: true })).toBeVisible();
    await searchDialog.getByRole("link", { name: /Premier League/i }).click();

    await expect(page).toHaveURL(/\/competitions\/premier_league$/);
    await expect(page.getByRole("heading", { name: /Premier League/i })).toBeVisible();

    await page
      .getByRole("button", { name: /Buscar competicoes, partidas, times ou jogadores/i })
      .click();
    const reopenedDialog = page.getByRole("dialog");
    await reopenedDialog.locator("#global-search-input").fill("salah");
    await expect(reopenedDialog.getByText("Jogadores", { exact: true })).toBeVisible();
    const playerResultLink = reopenedDialog.getByRole("link", { name: /Mohamed Salah/i });
    await expect(playerResultLink).toHaveAttribute("href", /competitionId=8/);
    await expect(playerResultLink).toHaveAttribute("href", /seasonId=2024/);
    await playerResultLink.click();

    await expect(page).toHaveURL(
      /\/competitions\/premier_league\/seasons\/2024%2F2025\/players\/306$/,
    );
    await expect(page.getByRole("heading", { name: "Mohamed Salah" })).toBeVisible();

    await page
      .getByRole("button", { name: /Buscar competicoes, partidas, times ou jogadores/i })
      .click();
    const teamDialog = page.getByRole("dialog");
    await teamDialog.locator("#global-search-input").fill("liv");
    await expect(teamDialog.getByText("Times", { exact: true })).toBeVisible();
    const teamResultLink = teamDialog.getByRole("link", { name: /^Liverpool/i });
    await expect(teamResultLink).toHaveAttribute("href", /competitionId=8/);
    await expect(teamResultLink).toHaveAttribute("href", /seasonId=2024/);
    await teamResultLink.click();

    await expect(page).toHaveURL(
      /\/competitions\/premier_league\/seasons\/2024%2F2025\/teams\/40$/,
    );
    await expect(page.getByRole("heading", { name: "Liverpool" })).toBeVisible();

    await page
      .getByRole("button", { name: /Buscar competicoes, partidas, times ou jogadores/i })
      .click();
    const brazilDialog = page.getByRole("dialog");
    await brazilDialog.locator("#global-search-input").fill("flamengo");
    await expect(brazilDialog.getByText("Times", { exact: true })).toBeVisible();
    const brazilTeamResultLink = brazilDialog.getByRole("link", { name: /^Flamengo/i });
    await expect(brazilTeamResultLink).toHaveAttribute("href", /\/teams\/20\?/);
    await expect(brazilTeamResultLink).toHaveAttribute("href", /competitionId=71/);
    await expect(brazilTeamResultLink).toHaveAttribute("href", /seasonId=2025/);
    await brazilTeamResultLink.click();

    await expect(page).toHaveURL(/\/competitions\/brasileirao_a\/seasons\/2025\/teams\/20$/);
    await expect(page.getByRole("heading", { name: "Flamengo" })).toBeVisible();

    await page
      .getByRole("button", { name: /Buscar competicoes, partidas, times ou jogadores/i })
      .click();
    const matchDialog = page.getByRole("dialog");
    await matchDialog.locator("#global-search-input").fill("19135048");
    await expect(matchDialog.getByText("Partidas", { exact: true })).toBeVisible();
    const matchResultLink = matchDialog.getByRole("link", { name: /Liverpool vs Crystal Palace/i });
    await expect(matchResultLink).toHaveAttribute("href", /\/matches\/19135048\?/);
    await expect(matchResultLink).toHaveAttribute("href", /competitionId=8/);
    await expect(matchResultLink).toHaveAttribute("href", /seasonId=2024/);
    await matchResultLink.click();

    await expect(page).toHaveURL(/\/matches\/19135048\?competitionId=8&seasonId=2024$/);
    await expect(page.getByRole("heading", { name: /Liverpool vs Crystal Palace/i })).toBeVisible();
    await expect(page.getByRole("dialog")).toHaveCount(0);
  });

  test("mostra loading, partial, empty e error no overlay", async ({ page }) => {
    await page.route("**/api/v1/search**", async (route) => {
      const url = new URL(route.request().url());
      const query = url.searchParams.get("q") ?? "";

      if (query.trim().toLowerCase() === "error") {
        await json(
          route,
          {
            message: "Falha controlada na busca global.",
            code: "INTERNAL_ERROR",
            status: 500,
          } as unknown as ApiPayload,
          500,
        );
        return;
      }

      if (query.trim().toLowerCase() === "slow") {
        await new Promise((resolve) => setTimeout(resolve, 350));
      }

      await json(route, buildSearchPayload(query));
    });

    await page.goto("/competitions");

    await page
      .getByRole("button", { name: /Buscar competicoes, partidas, times ou jogadores/i })
      .click();

    const searchDialog = page.getByRole("dialog");
    const searchInput = searchDialog.locator("#global-search-input");

    await searchInput.fill("slow");
    await expect(searchDialog.getByRole("status", { name: "Carregando" }).first()).toBeVisible();
    await expect(searchDialog.getByRole("link", { name: /Premier League/i })).toBeVisible();

    await searchInput.fill("partial");
    await expect(searchDialog.getByText("Resultados parciais.")).toBeVisible();
    await expect(
      searchDialog.getByText(
        "Alguns resultados podem aparecer em quantidade reduzida neste momento.",
      ),
    ).toBeVisible();
    await expect(searchDialog.getByText(/Cobertura atual:/)).toHaveCount(0);

    await searchInput.fill("zzz");
    await expect(searchDialog.getByText("Sem resultados")).toBeVisible();

    await searchInput.fill("error");
    await expect(searchDialog.getByText("Busca indisponivel")).toBeVisible();
    await expect(
      searchDialog.getByText(
        "Nao foi possivel carregar os resultados agora. Tente novamente em instantes.",
      ),
    ).toBeVisible();
    await expect(searchDialog.getByText("Busca global do app atual.")).toHaveCount(0);
  });
});
