import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

import type { Page, Route } from "@playwright/test";
import { expect, test } from "@playwright/test";

const OUTPUT_DIR = path.resolve(
  __dirname,
  "..",
  "test-results",
  "match-center-rankings-visual-audit",
);

const MATCH_CENTER_REFERENCE_FILE = path.resolve(
  __dirname,
  "..",
  "..",
  "reference design",
  "stitch_home_football_analytics",
  "match_center_summary_bloco_2",
  "code.html",
);

const RANKINGS_REFERENCE_FILE = path.resolve(
  __dirname,
  "..",
  "..",
  "reference design",
  "stitch_home_football_analytics",
  "season_hub_rankings_bloco_5_pt_br",
  "code.html",
);

const DEFAULT_META = {
  coverage: {
    status: "complete",
    percentage: 100,
    label: "Cobertura Total",
  },
};

const MATCH_CENTER_PAYLOAD = {
  match: {
    matchId: "match-arsenal-chelsea",
    competitionId: "8",
    competitionName: "Premier League",
    seasonId: "2024/2025",
    roundId: "28",
    kickoffAt: "2025-03-09T18:30:00Z",
    status: "finished",
    venueName: "Emirates Stadium",
    homeTeamId: "arsenal-fc",
    homeTeamName: "Arsenal FC",
    awayTeamId: "chelsea",
    awayTeamName: "Chelsea",
    homeScore: 2,
    awayScore: 1,
  },
  timeline: [
    {
      eventId: "goal-1",
      minute: 11,
      type: "goal",
      detail: "Gol em transicao rapida",
      teamId: "arsenal-fc",
      teamName: "Arsenal FC",
      playerId: "p-saka",
      playerName: "Bukayo Saka",
    },
    {
      eventId: "yellow-1",
      minute: 27,
      type: "yellow-card",
      detail: "Falta taticamente dura",
      teamId: "chelsea",
      teamName: "Chelsea",
      playerId: "p-enzo",
      playerName: "Enzo Fernandez",
    },
    {
      eventId: "goal-2",
      minute: 44,
      type: "goal",
      detail: "Finalizacao cruzada",
      teamId: "chelsea",
      teamName: "Chelsea",
      playerId: "p-palmer",
      playerName: "Cole Palmer",
    },
    {
      eventId: "goal-3",
      minute: 78,
      type: "goal",
      detail: "Rebatida na pequena area",
      teamId: "arsenal-fc",
      teamName: "Arsenal FC",
      playerId: "p-odegaard",
      playerName: "Martin Odegaard",
    },
  ],
  lineups: [
    {
      playerId: "p-saka",
      playerName: "Bukayo Saka",
      teamId: "arsenal-fc",
      teamName: "Arsenal FC",
      position: "RW",
      shirtNumber: 7,
      isStarter: true,
    },
    {
      playerId: "p-rice",
      playerName: "Declan Rice",
      teamId: "arsenal-fc",
      teamName: "Arsenal FC",
      position: "CM",
      shirtNumber: 41,
      isStarter: true,
    },
    {
      playerId: "p-palmer",
      playerName: "Cole Palmer",
      teamId: "chelsea",
      teamName: "Chelsea",
      position: "AM",
      shirtNumber: 20,
      isStarter: true,
    },
    {
      playerId: "p-caicedo",
      playerName: "Moises Caicedo",
      teamId: "chelsea",
      teamName: "Chelsea",
      position: "CM",
      shirtNumber: 25,
      isStarter: true,
    },
  ],
  playerStats: [
    {
      playerId: "p-saka",
      playerName: "Bukayo Saka",
      teamId: "arsenal-fc",
      teamName: "Arsenal FC",
      minutesPlayed: 90,
      goals: 1,
      assists: 0,
      shotsTotal: 4,
      passesCompleted: 31,
      rating: 8.1,
    },
    {
      playerId: "p-odegaard",
      playerName: "Martin Odegaard",
      teamId: "arsenal-fc",
      teamName: "Arsenal FC",
      minutesPlayed: 90,
      goals: 1,
      assists: 1,
      shotsTotal: 3,
      passesCompleted: 44,
      rating: 8.4,
    },
    {
      playerId: "p-palmer",
      playerName: "Cole Palmer",
      teamId: "chelsea",
      teamName: "Chelsea",
      minutesPlayed: 90,
      goals: 1,
      assists: 0,
      shotsTotal: 5,
      passesCompleted: 36,
      rating: 7.9,
    },
  ],
};

const RANKINGS_PAYLOAD = {
  rankingId: "rankings-player-goals",
  metricKey: "goals",
  rows: [
    {
      entityId: "p-haaland",
      entityName: "Erling Haaland",
      teamName: "Manchester City",
      rank: 1,
      metricValue: 12,
      goals: 12,
    },
    {
      entityId: "p-salah",
      entityName: "Mohamed Salah",
      teamName: "Liverpool FC",
      rank: 2,
      metricValue: 10,
      goals: 10,
    },
    {
      entityId: "p-palmer",
      entityName: "Cole Palmer",
      teamName: "Chelsea FC",
      rank: 3,
      metricValue: 8,
      goals: 8,
    },
    {
      entityId: "p-saka",
      entityName: "Bukayo Saka",
      teamName: "Arsenal FC",
      rank: 4,
      metricValue: 7,
      goals: 7,
    },
  ],
  updatedAt: "2026-03-21T09:00:00Z",
};

function ensureOutputDir() {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

function normalizeApiPath(pathname: string): string {
  return pathname.replace(/^\/api\/v1(?=\/)/, "/api");
}

async function installAuditMocks(page: Page) {
  await page.route("**/api/**", async (route: Route) => {
    const requestUrl = new URL(route.request().url());
    const pathname = normalizeApiPath(requestUrl.pathname);

    if (pathname === "/api/matches/match-arsenal-chelsea") {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          data: MATCH_CENTER_PAYLOAD,
          meta: DEFAULT_META,
        }),
      });
      return;
    }

    if (pathname.startsWith("/api/rankings/")) {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          data: RANKINGS_PAYLOAD,
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

async function createComparisonSheet(
  page: Page,
  title: string,
  implementationPath: string,
  referencePath: string,
  outputFileName: string,
) {
  const implementationDataUrl = `data:image/png;base64,${fs.readFileSync(implementationPath).toString("base64")}`;
  const referenceDataUrl = `data:image/png;base64,${fs.readFileSync(referencePath).toString("base64")}`;
  const comparisonHtml = `
    <html lang="pt-BR">
      <head>
        <meta charset="utf-8" />
        <title>${title}</title>
        <style>
          body {
            margin: 0;
            font-family: Inter, Arial, sans-serif;
            background: #eef5ff;
            color: #111c2d;
            padding: 24px;
          }
          h1 {
            margin: 0 0 16px;
            font-size: 24px;
          }
          .grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
          }
          .panel {
            background: rgba(255, 255, 255, 0.9);
            border-radius: 20px;
            padding: 16px;
            box-shadow: 0 20px 50px -40px rgba(17, 28, 45, 0.45);
          }
          .label {
            margin: 0 0 10px;
            font-size: 12px;
            font-weight: 700;
            letter-spacing: 0.18em;
            text-transform: uppercase;
            color: #57657a;
          }
          img {
            display: block;
            width: 100%;
            border-radius: 16px;
          }
        </style>
      </head>
      <body>
        <h1>${title}</h1>
        <div class="grid">
          <section class="panel">
            <p class="label">Implementacao</p>
            <img src="${implementationDataUrl}" alt="Implementacao" />
          </section>
          <section class="panel">
            <p class="label">Referencia</p>
            <img src="${referenceDataUrl}" alt="Referencia" />
          </section>
        </div>
      </body>
    </html>
  `;

  await page.setViewportSize({ width: 1800, height: 1200 });
  await page.goto(`data:text/html,${encodeURIComponent(comparisonHtml)}`);
  await page.waitForLoadState("load");
  await page.waitForTimeout(300);
  await page.screenshot({
    animations: "disabled",
    fullPage: true,
    path: path.join(OUTPUT_DIR, outputFileName),
  });
}

test.describe("Auditoria visual: Match center e Rankings", () => {
  test("gera screenshots de implementacao e referencia dos dois recortes", async ({ page }) => {
    ensureOutputDir();
    await installAuditMocks(page);
    await page.setViewportSize({ width: 1600, height: 1900 });

    await page.goto("/matches/match-arsenal-chelsea?competitionId=8&seasonId=2024/2025");
    await expect(
      page.getByRole("heading", { exact: true, name: "Arsenal FC vs Chelsea" }),
    ).toBeVisible();
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(800);
    await page
      .locator("main")
      .first()
      .screenshot({
        animations: "disabled",
        path: path.join(OUTPUT_DIR, "match-center-implementation.png"),
      });

    await page.goto(pathToFileURL(MATCH_CENTER_REFERENCE_FILE).href);
    await page.waitForLoadState("load");
    await page.waitForTimeout(1200);
    await page
      .locator("main")
      .first()
      .screenshot({
        animations: "disabled",
        path: path.join(OUTPUT_DIR, "match-center-reference.png"),
      });

    await createComparisonSheet(
      page,
      "Match center summary/header",
      path.join(OUTPUT_DIR, "match-center-implementation.png"),
      path.join(OUTPUT_DIR, "match-center-reference.png"),
      "match-center-comparison.png",
    );

    await installAuditMocks(page);
    await page.setViewportSize({ width: 1600, height: 2200 });
    await page.goto("/rankings/player-goals?competitionId=8&seasonId=2024/2025&lastN=5");
    await expect(page.getByRole("heading", { exact: true, name: "Artilharia" })).toBeVisible();
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(800);
    await page
      .locator("main")
      .first()
      .screenshot({
        animations: "disabled",
        path: path.join(OUTPUT_DIR, "rankings-implementation.png"),
      });

    await page.goto(pathToFileURL(RANKINGS_REFERENCE_FILE).href);
    await page.waitForLoadState("load");
    await page.waitForTimeout(1200);
    await page
      .locator("main")
      .first()
      .screenshot({
        animations: "disabled",
        path: path.join(OUTPUT_DIR, "rankings-reference.png"),
      });

    await createComparisonSheet(
      page,
      "Rankings",
      path.join(OUTPUT_DIR, "rankings-implementation.png"),
      path.join(OUTPUT_DIR, "rankings-reference.png"),
      "rankings-comparison.png",
    );
  });
});
