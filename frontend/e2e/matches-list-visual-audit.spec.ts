import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

import type { Page, Route } from "@playwright/test";
import { expect, test } from "@playwright/test";

const OUTPUT_DIR = path.resolve(__dirname, "..", "test-results", "matches-list-visual-audit");

const MATCHES_REFERENCE_FILE = path.resolve(
  __dirname,
  "..",
  "..",
  "reference design",
  "stitch_home_football_analytics",
  "season_hub_calendar_bloco_2",
  "code.html",
);

const DEFAULT_META = {
  coverage: {
    status: "complete",
    percentage: 100,
    label: "Cobertura Total",
  },
};

const MATCHES_LIST_ITEMS = [
  {
    matchId: "match-liverpool-man-city",
    competitionId: "8",
    competitionName: "Premier League",
    seasonId: "2024",
    roundId: "28",
    kickoffAt: "2025-03-09T15:00:00Z",
    status: "finished",
    venueName: "Anfield",
    homeTeamId: "liverpool",
    homeTeamName: "Liverpool",
    awayTeamId: "man-city",
    awayTeamName: "Manchester City",
    homeScore: 2,
    awayScore: 0,
  },
  {
    matchId: "match-arsenal-chelsea",
    competitionId: "8",
    competitionName: "Premier League",
    seasonId: "2024",
    roundId: "28",
    kickoffAt: "2025-03-09T18:30:00Z",
    status: "live",
    venueName: "Emirates Stadium",
    homeTeamId: "arsenal-fc",
    homeTeamName: "Arsenal FC",
    awayTeamId: "chelsea",
    awayTeamName: "Chelsea",
    homeScore: 1,
    awayScore: 1,
  },
  {
    matchId: "match-astonvilla-spurs",
    competitionId: "8",
    competitionName: "Premier League",
    seasonId: "2024",
    roundId: "29",
    kickoffAt: "2025-03-10T14:00:00Z",
    status: "scheduled",
    venueName: "Villa Park",
    homeTeamId: "aston-villa",
    homeTeamName: "Aston Villa",
    awayTeamId: "spurs",
    awayTeamName: "Tottenham",
    homeScore: null,
    awayScore: null,
  },
  {
    matchId: "match-brighton-manutd",
    competitionId: "8",
    competitionName: "Premier League",
    seasonId: "2024",
    roundId: "29",
    kickoffAt: "2025-03-10T17:00:00Z",
    status: "cancelled",
    venueName: "Amex Stadium",
    homeTeamId: "brighton",
    homeTeamName: "Brighton",
    awayTeamId: "man-utd",
    awayTeamName: "Manchester United",
    homeScore: null,
    awayScore: null,
  },
];

function ensureOutputDir() {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

function normalizeApiPath(pathname: string): string {
  return pathname.replace(/^\/api\/v1(?=\/)/, "/api");
}

async function installMatchesAuditMocks(page: Page) {
  await page.route("**/api/v1/**", async (route: Route) => {
    const requestUrl = new URL(route.request().url());
    const pathname = normalizeApiPath(requestUrl.pathname);

    if (pathname === "/api/matches") {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          data: {
            items: MATCHES_LIST_ITEMS,
          },
          meta: DEFAULT_META,
        }),
      });
      return;
    }

    if (pathname.startsWith("/api/matches/")) {
      const matchId = pathname.split("/").filter(Boolean).at(-1) ?? "match-arsenal-chelsea";
      const match =
        MATCHES_LIST_ITEMS.find((candidate) => candidate.matchId === matchId) ??
        MATCHES_LIST_ITEMS[0];

      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          data: {
            match,
            timeline: [],
            lineups: [],
            playerStats: [],
          },
          meta: DEFAULT_META,
        }),
      });
      return;
    }

    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        data: [],
        meta: DEFAULT_META,
      }),
    });
  });
}

test.describe("Auditoria visual da lista de partidas", () => {
  test("gera screenshots da implementacao e da referencia", async ({ page }) => {
    ensureOutputDir();
    await installMatchesAuditMocks(page);
    await page.setViewportSize({ width: 1600, height: 2000 });

    await page.goto("/matches?competitionId=8&seasonId=2024&roundId=28");
    await expect(page.getByRole("heading", { exact: true, name: "Partidas" })).toBeVisible();
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(800);

    await page
      .locator("main")
      .first()
      .screenshot({
        animations: "disabled",
        path: path.join(OUTPUT_DIR, "matches-list-implementation.png"),
      });

    await page.goto(pathToFileURL(MATCHES_REFERENCE_FILE).href);
    await page.waitForLoadState("load");
    await page.waitForTimeout(1200);

    await page
      .locator("main")
      .first()
      .screenshot({
        animations: "disabled",
        path: path.join(OUTPUT_DIR, "matches-list-reference.png"),
      });
  });

  test("preserva contexto ao abrir o match center a partir da lista", async ({ page }) => {
    await installMatchesAuditMocks(page);

    await page.goto("/matches?competitionId=8&seasonId=2024&roundId=28");
    await expect(page.getByRole("heading", { exact: true, name: "Partidas" })).toBeVisible();

    await page
      .getByRole("link", {
        name: "Abrir match center de Arsenal FC vs Chelsea",
      })
      .click();

    await expect(page).toHaveURL(
      /\/matches\/match-arsenal-chelsea\?competitionId=8&seasonId=2024/,
    );
    await expect(
      page.getByRole("heading", { exact: true, name: "Arsenal FC vs Chelsea" }),
    ).toBeVisible();
  });
});
