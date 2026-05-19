import { expect, test, type Page, type Route } from "@playwright/test";

function json(route: Route, body: unknown, status = 200): Promise<void> {
  return route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify(body),
  });
}

async function installResolverSurfaceMocks(page: Page) {
  await page.route("**/api/v1/**", async (route) => {
    const requestUrl = new URL(route.request().url());
    const pathname = requestUrl.pathname;

    if (pathname === "/api/v1/players/306/contexts") {
      await new Promise((resolve) => {
        setTimeout(resolve, 700);
      });

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
            yellowCards: 1,
            redCards: 0,
            rating: 8.2,
          },
          recentMatches: [],
          history: [],
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
            trend: [],
          },
          sectionCoverage: {
            overview: { status: "complete", percentage: 100, label: "Player overview coverage" },
            history: { status: "empty", percentage: 0, label: "Player history coverage" },
            matches: { status: "empty", percentage: 0, label: "Player matches coverage" },
            stats: { status: "complete", percentage: 100, label: "Player stats coverage" },
          },
        },
        meta: {
          coverage: {
            status: "partial",
            percentage: 50,
            label: "Player profile coverage",
          },
        },
      });
      return;
    }

    if (pathname === "/api/v1/teams/40/contexts") {
      await json(
        route,
        {
          message: "Falha simulada na resolução de contexto do time",
          code: "TEAM_CONTEXT_ERROR",
          status: 500,
        },
        500,
      );
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

test.describe("Superfícies globais de sistema", () => {
  test("fecha o not-found da plataforma com framing e saídas úteis", async ({ page }) => {
    await page.goto("/rota-inexistente");

    await expect(
      page.getByRole("heading", { name: "Área não encontrada" }),
    ).toBeVisible();
    await expect(page.getByRole("region", { name: "Contexto da página" })).toBeVisible();
    await expect(page.getByRole("link", { name: "Voltar ao início" })).toHaveAttribute(
      "href",
      "/",
    );
    await expect(page.getByRole("link", { name: "Abrir competições" })).toBeVisible();
    await expect(page.getByRole("link", { name: "Abrir partidas" })).toBeVisible();

    await page.goto("/competitions/premier_league/seasons/2024%2F2025/rota-inexistente");
    await expect(
      page.getByRole("heading", { name: "Área não encontrada" }),
    ).toBeVisible();
  });

  test("preserva query nas reentradas legadas e absorve rotas auxiliares na mesma casca", async ({
    page,
  }) => {
    await page.goto("/competition/8?roundId=29&venue=home&lastN=5");
    await expect(page).toHaveURL("/competitions/premier_league?roundId=29&venue=home&lastN=5");

    await page.goto("/clubs?competitionId=8&seasonId=2024&roundId=29");
    await expect(page).toHaveURL("/competitions?competitionId=8&seasonId=2024&roundId=29");

    await page.goto("/market?competitionId=8&seasonId=2024&roundId=29");
    await expect(
      page.getByRole("heading", { name: "Mercado indisponível" }),
    ).toBeVisible();
    await expect(page.getByRole("region", { name: "Contexto da página" })).toBeVisible();
    await expect(page.getByRole("link", { name: "Abrir jogadores" })).toHaveAttribute(
      "href",
      "/players?competitionId=8&seasonId=2024&roundId=29",
    );

    await page.goto("/coaches/123");
    await expect(
      page.getByRole("heading", { name: "Perfil de técnico indisponível" }),
    ).toBeVisible();
    await expect(page.getByText("Coach ID: 123")).toHaveCount(0);
  });

  test("usa superfícies da shell para loading e erro dos resolvers curtos", async ({ page }) => {
    await installResolverSurfaceMocks(page);

    const playerNavigation = page.goto("/players/306?roundId=29&venue=home&lastN=5");

    await expect(page.getByRole("heading", { name: "Preparando jogador" }).first()).toBeVisible();

    await playerNavigation;

    await expect(page).toHaveURL(
      "/competitions/premier_league/seasons/2024%2F2025/players/306?roundId=29&venue=home&lastN=5",
    );
    await expect(page.getByRole("heading", { name: "Mohamed Salah" })).toBeVisible();

    await page.goto("/teams/40?roundId=29");

    await expect(
      page.getByRole("heading", { name: "Nao foi possivel abrir este time" }),
    ).toBeVisible();
    await expect(page.getByRole("link", { name: "Voltar para times" })).toHaveAttribute(
      "href",
      "/teams?roundId=29",
    );
    await expect(page.getByRole("link", { name: "Abrir competições" })).toHaveAttribute(
      "href",
      "/competitions?roundId=29",
    );
    await expect(page.getByText("Team ID: 40")).toHaveCount(0);
    await expect(page.getByText("/api/v1/teams/40/contexts")).toHaveCount(0);
  });
});
