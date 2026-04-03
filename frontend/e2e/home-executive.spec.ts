import { expect, test, type Page, type Route } from "@playwright/test";

function json(route: Route, body: unknown, status = 200): Promise<void> {
  return route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify(body),
  });
}

async function installHomeMocks(page: Page) {
  await page.route("**/api/v1/**", async (route) => {
    const requestUrl = new URL(route.request().url());
    const pathname = requestUrl.pathname;

    if (pathname === "/api/v1/home") {
      await json(route, {
        data: {
          archiveSummary: {
            competitions: 10,
            seasons: 50,
            matches: 15265,
            players: 21654,
          },
          competitions: [
            {
              competitionId: "71",
              competitionKey: "brasileirao_a",
              competitionName: "Campeonato Brasileiro Série A",
              assetId: "648",
              matchesCount: 1900,
              seasonsCount: 5,
              range: {
                fromSeasonId: "2021",
                fromSeasonLabel: "2021",
                toSeasonId: "2025",
                toSeasonLabel: "2025",
              },
              latestContext: {
                competitionId: "71",
                competitionKey: "brasileirao_a",
                competitionName: "Campeonato Brasileiro Série A",
                seasonId: "2025",
                seasonLabel: "2025",
              },
              coverage: {
                status: "partial",
                percentage: 99.79,
                label: "Competition depth coverage",
              },
            },
            {
              competitionId: "651",
              competitionKey: "brasileirao_b",
              competitionName: "Campeonato Brasileiro Série B",
              assetId: "651",
              matchesCount: 1894,
              seasonsCount: 5,
              range: {
                fromSeasonId: "2021",
                fromSeasonLabel: "2021",
                toSeasonId: "2025",
                toSeasonLabel: "2025",
              },
              latestContext: {
                competitionId: "651",
                competitionKey: "brasileirao_b",
                competitionName: "Campeonato Brasileiro Série B",
                seasonId: "2025",
                seasonLabel: "2025",
              },
              coverage: {
                status: "partial",
                percentage: 99.68,
                label: "Competition depth coverage",
              },
            },
            {
              competitionId: "8",
              competitionKey: "premier_league",
              competitionName: "Premier League",
              assetId: "8",
              matchesCount: 1900,
              seasonsCount: 5,
              range: {
                fromSeasonId: "2020",
                fromSeasonLabel: "2020/2021",
                toSeasonId: "2024",
                toSeasonLabel: "2024/2025",
              },
              latestContext: {
                competitionId: "8",
                competitionKey: "premier_league",
                competitionName: "Premier League",
                seasonId: "2024",
                seasonLabel: "2024/2025",
              },
              coverage: {
                status: "partial",
                percentage: 99.89,
                label: "Competition depth coverage",
              },
            },
            {
              competitionId: "390",
              competitionKey: "libertadores",
              competitionName: "Copa Libertadores da América",
              assetId: "1122",
              matchesCount: 775,
              seasonsCount: 5,
              range: {
                fromSeasonId: "2021",
                fromSeasonLabel: "2021",
                toSeasonId: "2025",
                toSeasonLabel: "2025",
              },
              latestContext: {
                competitionId: "390",
                competitionKey: "libertadores",
                competitionName: "Copa Libertadores da América",
                seasonId: "2025",
                seasonLabel: "2025",
              },
              coverage: {
                status: "complete",
                percentage: 100,
                label: "Competition depth coverage",
              },
            },
            {
              competitionId: "2",
              competitionKey: "champions_league",
              competitionName: "UEFA Champions League",
              assetId: "2",
              matchesCount: 1103,
              seasonsCount: 5,
              range: {
                fromSeasonId: "2020",
                fromSeasonLabel: "2020/2021",
                toSeasonId: "2024",
                toSeasonLabel: "2024/2025",
              },
              latestContext: {
                competitionId: "2",
                competitionKey: "champions_league",
                competitionName: "UEFA Champions League",
                seasonId: "2024",
                seasonLabel: "2024/2025",
              },
              coverage: {
                status: "complete",
                percentage: 100,
                label: "Competition depth coverage",
              },
            },
          ],
          editorialHighlights: [
            {
              id: "highlight-1",
              eyebrow: "Curadoria de dados reais",
              competitionLabel: "UEFA Champions League · 2024/2025",
              title: "Raphinha: o pico recente de UEFA Champions League",
              description:
                "14 jogos, 13 gols, 11 assistências, rating 8.34 por FC Barcelona no recorte selecionado.",
              playerId: "160258",
              playerName: "Raphinha",
              teamId: "83",
              teamName: "FC Barcelona",
              imageAssetId: "160258",
              context: {
                competitionId: "2",
                competitionKey: "champions_league",
                competitionName: "UEFA Champions League",
                seasonId: "2024",
                seasonLabel: "2024/2025",
              },
              metrics: {
                matchesPlayed: 14,
                goals: 13,
                assists: 11,
                rating: 8.34,
              },
            },
            {
              id: "highlight-2",
              eyebrow: "Curadoria de dados reais",
              competitionLabel: "Copa Libertadores da América · 2024",
              title: "Pedrinho: o pico recente de Copa Libertadores da América",
              description:
                "6 jogos, 2 gols, 1 assistência, rating 8.52 por Atlético Mineiro no recorte selecionado.",
              playerId: "524329",
              playerName: "Pedrinho",
              teamId: "3427",
              teamName: "Atlético Mineiro",
              imageAssetId: "524329",
              context: {
                competitionId: "390",
                competitionKey: "libertadores",
                competitionName: "Copa Libertadores da América",
                seasonId: "2024",
                seasonLabel: "2024",
              },
              metrics: {
                matchesPlayed: 6,
                goals: 2,
                assists: 1,
                rating: 8.52,
              },
            },
          ],
        },
        meta: {
          coverage: {
            status: "complete",
            percentage: 100,
            label: "Home coverage",
          },
        },
      });
      return;
    }

    if (pathname === "/api/v1/search") {
      await json(route, {
        data: {
          query: "raph",
          totalResults: 1,
          groups: [
            {
              type: "player",
              total: 1,
              items: [
                {
                  playerId: "160258",
                  playerName: "Raphinha",
                  teamId: "83",
                  teamName: "FC Barcelona",
                  position: "RW",
                  defaultContext: {
                    competitionId: "2",
                    competitionKey: "champions_league",
                    competitionName: "UEFA Champions League",
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
            label: "Search coverage",
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

async function installPartialHomeMocks(page: Page) {
  await page.route("**/api/v1/**", async (route) => {
    const requestUrl = new URL(route.request().url());
    const pathname = requestUrl.pathname;

    if (pathname === "/api/v1/home") {
      await json(route, {
        data: {
          archiveSummary: {
            competitions: 10,
            seasons: 50,
            matches: 15265,
            players: 21654,
          },
          competitions: [
            {
              competitionId: "8",
              competitionKey: "premier_league",
              competitionName: "Premier League",
              assetId: "8",
              matchesCount: 1900,
              seasonsCount: 5,
              range: {
                fromSeasonId: "2020",
                fromSeasonLabel: "2020/2021",
                toSeasonId: "2024",
                toSeasonLabel: "2024/2025",
              },
              latestContext: {
                competitionId: "8",
                competitionKey: "premier_league",
                competitionName: "Premier League",
                seasonId: "2024",
                seasonLabel: "2024/2025",
              },
              coverage: {
                status: "partial",
                percentage: 74.5,
                label: "Competition depth coverage",
              },
            },
          ],
          editorialHighlights: [],
        },
        meta: {
          coverage: {
            status: "partial",
            percentage: 66.67,
            label: "Home coverage",
          },
        },
      });
      return;
    }

    if (pathname === "/api/v1/search") {
      await json(route, {
        data: {
          query: "raph",
          totalResults: 0,
          groups: [],
        },
        meta: {
          coverage: {
            status: "complete",
            percentage: 100,
            label: "Search coverage",
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

test.describe("Home executiva", () => {
  test("renderiza a nova home e navega pelos blocos principais", async ({ page }) => {
    await installHomeMocks(page);

    await page.goto("/");

    await expect(page.getByText("Explore a História do")).toBeVisible();
    await expect(page.getByRole("heading", { name: "Competições Nacionais" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Continentais" })).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Curadoria de Temporadas em Destaque" }),
    ).toBeVisible();
    await expect(page.getByRole("link", { name: /Premier League/i })).toBeVisible();
    await expect(page.getByText("UEFA Champions League").first()).toBeVisible();

    await page.getByRole("button", { name: /02 Busca Global/i }).click();
    await expect(page.getByRole("dialog")).toBeVisible();
    await expect(page.locator("#global-search-input")).toBeVisible();
    await page.keyboard.press("Escape");

    await page
      .getByRole("link", { name: /Premier League/i })
      .first()
      .click();
    await expect(page).toHaveURL("/competitions/premier_league/seasons/2024%2F2025");
  });

  test("expõe partial coverage e empty state editorial quando o contrato vier incompleto", async ({
    page,
  }) => {
    await installPartialHomeMocks(page);

    await page.goto("/");

    await expect(page.getByText("Dados parciais.")).toBeVisible();
    await expect(page.getByRole("heading", { name: "Competições Nacionais" })).toBeVisible();
    await expect(page.getByText("Curadoria indisponível")).toBeVisible();
  });
});
