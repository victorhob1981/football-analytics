import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

import type { Page, Route } from "@playwright/test";
import { expect, test } from "@playwright/test";

const OUTPUT_DIR = path.resolve(__dirname, "..", "test-results", "profile-visual-audit");

const DEFAULT_META = {
  coverage: {
    status: "complete",
    percentage: 100,
    label: "Cobertura Total",
  },
};

const TEAM_REFERENCE_FILE = path.resolve(
  __dirname,
  "..",
  "..",
  "reference design",
  "stitch_home_football_analytics",
  "perfil_do_clube_vis_o_geral_bloco_8_pt_br",
  "code.html",
);

const PLAYER_REFERENCE_FILE = path.resolve(
  __dirname,
  "..",
  "..",
  "reference design",
  "stitch_home_football_analytics",
  "perfil_do_jogador_partidas_bloco_9_pt_br",
  "code.html",
);

function ensureOutputDir() {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

async function installProfileAuditMocks(page: Page) {
  await page.route("**/api/v1/**", async (route: Route) => {
    const requestUrl = new URL(route.request().url());
    const pathname = requestUrl.pathname;

    if (pathname === "/api/v1/teams/arsenal-fc") {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          data: {
            team: {
              teamId: "arsenal-fc",
              teamName: "Arsenal FC",
              competitionId: "8",
              competitionName: "Premier League",
              seasonId: "2024/2025",
              seasonLabel: "2024/2025",
            },
            summary: {
              matchesPlayed: 24,
              wins: 16,
              draws: 6,
              losses: 2,
              goalsFor: 54,
              goalsAgainst: 22,
              goalDiff: 32,
              points: 54,
            },
            standing: {
              position: 2,
              totalTeams: 20,
            },
            form: ["win", "win", "draw", "win", "loss"],
            recentMatches: [
              {
                matchId: "tm-1",
                playedAt: "2025-02-22T15:00:00Z",
                opponentName: "Liverpool",
                venue: "home",
                goalsFor: 3,
                goalsAgainst: 1,
                result: "win",
              },
              {
                matchId: "tm-2",
                playedAt: "2025-02-15T15:00:00Z",
                opponentName: "West Ham",
                venue: "away",
                goalsFor: 6,
                goalsAgainst: 0,
                result: "win",
              },
              {
                matchId: "tm-3",
                playedAt: "2025-02-08T15:00:00Z",
                opponentName: "Manchester City",
                venue: "home",
                goalsFor: 0,
                goalsAgainst: 0,
                result: "draw",
              },
              {
                matchId: "tm-4",
                playedAt: "2025-02-01T15:00:00Z",
                opponentName: "Everton",
                venue: "home",
                goalsFor: 2,
                goalsAgainst: 1,
                result: "win",
              },
              {
                matchId: "tm-5",
                playedAt: "2025-01-25T15:00:00Z",
                opponentName: "Aston Villa",
                venue: "away",
                goalsFor: 0,
                goalsAgainst: 1,
                result: "loss",
              },
            ],
          },
          meta: DEFAULT_META,
        }),
      });
      return;
    }

    if (pathname === "/api/v1/matches" && requestUrl.searchParams.get("teamId") === "arsenal-fc") {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          data: {
            items: [
              {
                matchId: "tm-1",
                competitionId: "8",
                competitionName: "Premier League",
                seasonId: "2024/2025",
                roundId: "24",
                kickoffAt: "2025-02-22T15:00:00Z",
                status: "FT",
                homeTeamId: "arsenal-fc",
                homeTeamName: "Arsenal FC",
                awayTeamId: "liverpool",
                awayTeamName: "Liverpool",
                homeScore: 3,
                awayScore: 1,
              },
              {
                matchId: "tm-2",
                competitionId: "8",
                competitionName: "Premier League",
                seasonId: "2024/2025",
                roundId: "23",
                kickoffAt: "2025-02-15T15:00:00Z",
                status: "FT",
                homeTeamId: "west-ham",
                homeTeamName: "West Ham",
                awayTeamId: "arsenal-fc",
                awayTeamName: "Arsenal FC",
                homeScore: 0,
                awayScore: 6,
              },
              {
                matchId: "tm-3",
                competitionId: "8",
                competitionName: "Premier League",
                seasonId: "2024/2025",
                roundId: "22",
                kickoffAt: "2025-02-08T15:00:00Z",
                status: "FT",
                homeTeamId: "arsenal-fc",
                homeTeamName: "Arsenal FC",
                awayTeamId: "man-city",
                awayTeamName: "Manchester City",
                homeScore: 0,
                awayScore: 0,
              },
              {
                matchId: "tm-4",
                competitionId: "8",
                competitionName: "Premier League",
                seasonId: "2024/2025",
                roundId: "21",
                kickoffAt: "2025-02-01T15:00:00Z",
                status: "FT",
                homeTeamId: "arsenal-fc",
                homeTeamName: "Arsenal FC",
                awayTeamId: "everton",
                awayTeamName: "Everton",
                homeScore: 2,
                awayScore: 1,
              },
              {
                matchId: "tm-5",
                competitionId: "8",
                competitionName: "Premier League",
                seasonId: "2024/2025",
                roundId: "20",
                kickoffAt: "2025-01-25T15:00:00Z",
                status: "FT",
                homeTeamId: "aston-villa",
                homeTeamName: "Aston Villa",
                awayTeamId: "arsenal-fc",
                awayTeamName: "Arsenal FC",
                homeScore: 1,
                awayScore: 0,
              },
            ],
          },
          meta: DEFAULT_META,
        }),
      });
      return;
    }

    if (pathname === "/api/v1/players/erling-haaland") {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          data: {
            player: {
              playerId: "erling-haaland",
              playerName: "Erling Haaland",
              teamId: "man-city",
              teamName: "Manchester City",
              position: "FW",
              nationality: "Noruega",
            },
            summary: {
              matchesPlayed: 10,
              minutesPlayed: 868,
              goals: 12,
              assists: 3,
              shotsTotal: 31,
              shotsOnTarget: 18,
              passAccuracyPct: 81.4,
              yellowCards: 1,
              redCards: 0,
              rating: 8.9,
            },
            recentMatches: [
              {
                fixtureId: "pm-1",
                playedAt: "2025-02-22T15:00:00Z",
                opponentName: "Arsenal (F)",
                minutesPlayed: 90,
                goals: 2,
                assists: 0,
                rating: 9.2,
              },
              {
                fixtureId: "pm-2",
                playedAt: "2025-02-15T15:00:00Z",
                opponentName: "Inter (C)",
                minutesPlayed: 78,
                goals: 1,
                assists: 1,
                rating: 8.4,
              },
              {
                fixtureId: "pm-3",
                playedAt: "2025-02-08T15:00:00Z",
                opponentName: "Chelsea (C)",
                minutesPlayed: 90,
                goals: 1,
                assists: 0,
                rating: 7.8,
              },
              {
                fixtureId: "pm-4",
                playedAt: "2025-02-01T15:00:00Z",
                opponentName: "Liverpool (F)",
                minutesPlayed: 86,
                goals: 0,
                assists: 1,
                rating: 7.5,
              },
              {
                fixtureId: "pm-5",
                playedAt: "2025-01-25T15:00:00Z",
                opponentName: "Newcastle (C)",
                minutesPlayed: 88,
                goals: 3,
                assists: 0,
                rating: 9.6,
              },
            ],
          },
          meta: DEFAULT_META,
        }),
      });
      return;
    }

    if (pathname === "/api/v1/insights") {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          data: [
            {
              insight_id: "insight-haaland-1",
              severity: "info",
              explanation: "Conversao de finalizacoes acima da media recente do recorte.",
              evidences: {
                goals: 12,
                shots_on_target: 18,
              },
              reference_period: "Ultimos 10 jogos",
              data_source: ["mart.player_match_summary"],
            },
            {
              insight_id: "insight-haaland-2",
              severity: "warning",
              explanation: "Volume ofensivo alto com dependencia forte de finalizacao central.",
              evidences: {
                shots_total: 31,
                pass_accuracy_pct: 81.4,
              },
              reference_period: "Ultimos 10 jogos",
              data_source: ["mart.player_match_summary"],
            },
          ],
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

test.describe("Auditoria visual de profiles", () => {
  test("gera screenshots de implementacao e referencia para team/player profile", async ({
    page,
  }) => {
    ensureOutputDir();
    await installProfileAuditMocks(page);
    await page.setViewportSize({ width: 1440, height: 2200 });

    await page.goto("/competitions/premier_league/seasons/2024%2F2025/teams/arsenal-fc");
    await expect(page.getByRole("heading", { name: "Arsenal FC" })).toBeVisible();
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(800);
    await page.locator("main").first().screenshot({
      path: path.join(OUTPUT_DIR, "team-implementation.png"),
      animations: "disabled",
    });

    await page.goto(pathToFileURL(TEAM_REFERENCE_FILE).href);
    await page.waitForLoadState("load");
    await page.waitForTimeout(1200);
    await page.locator("main").first().screenshot({
      path: path.join(OUTPUT_DIR, "team-reference.png"),
      animations: "disabled",
    });

    await page.goto("/competitions/premier_league/seasons/2024%2F2025/players/erling-haaland");
    await expect(page.getByRole("heading", { name: "Erling Haaland" })).toBeVisible();
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(800);
    await page.locator("main").first().screenshot({
      path: path.join(OUTPUT_DIR, "player-implementation.png"),
      animations: "disabled",
    });

    await page.goto(pathToFileURL(PLAYER_REFERENCE_FILE).href);
    await page.waitForLoadState("load");
    await page.waitForTimeout(1200);
    await page.locator("main").first().screenshot({
      path: path.join(OUTPUT_DIR, "player-reference.png"),
      animations: "disabled",
    });
  });
});
