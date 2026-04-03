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

const LIST_CONTEXT_QUERY = "competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5";

const MATCH_CENTER_PAYLOAD = {
  match: {
    matchId: "match-arsenal-chelsea",
    fixtureId: "match-arsenal-chelsea",
    competitionId: "8",
    competitionName: "Premier League",
    seasonId: "2024",
    roundId: "29",
    kickoffAt: "2025-03-09T18:30:00Z",
    status: "FT",
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
      formationField: "attack",
      formationPosition: 1,
      shirtNumber: 7,
      isStarter: true,
      minutesPlayed: 90,
    },
    {
      playerId: "p-rice",
      playerName: "Declan Rice",
      teamId: "arsenal-fc",
      teamName: "Arsenal FC",
      position: "CM",
      formationField: "midfield",
      formationPosition: 2,
      shirtNumber: 41,
      isStarter: true,
      minutesPlayed: 90,
    },
    {
      playerId: "p-palmer",
      playerName: "Cole Palmer",
      teamId: "chelsea",
      teamName: "Chelsea",
      position: "AM",
      formationField: "attack",
      formationPosition: 1,
      shirtNumber: 20,
      isStarter: true,
      minutesPlayed: 90,
    },
    {
      playerId: "p-enzo",
      playerName: "Enzo Fernandez",
      teamId: "chelsea",
      teamName: "Chelsea",
      position: "CM",
      formationField: "midfield",
      formationPosition: 4,
      shirtNumber: 8,
      isStarter: false,
      minutesPlayed: 30,
    },
  ],
  playerStats: [
    {
      playerId: "p-saka",
      playerName: "Bukayo Saka",
      teamId: "arsenal-fc",
      teamName: "Arsenal FC",
      positionName: "RW",
      isStarter: true,
      minutesPlayed: 90,
      goals: 1,
      assists: 0,
      shotsTotal: 4,
      shotsOnGoal: 2,
      passesTotal: 31,
      keyPasses: 3,
      yellowCards: 0,
      redCards: 0,
      rating: 8.1,
      xg: 0.74,
    },
    {
      playerId: "p-odegaard",
      playerName: "Martin Odegaard",
      teamId: "arsenal-fc",
      teamName: "Arsenal FC",
      positionName: "AM",
      isStarter: true,
      minutesPlayed: 90,
      goals: 1,
      assists: 1,
      shotsTotal: 3,
      shotsOnGoal: 1,
      passesTotal: 44,
      keyPasses: 4,
      yellowCards: 0,
      redCards: 0,
      rating: 8.4,
      xg: 0.41,
    },
    {
      playerId: "p-palmer",
      playerName: "Cole Palmer",
      teamId: "chelsea",
      teamName: "Chelsea",
      positionName: "AM",
      isStarter: true,
      minutesPlayed: 90,
      goals: 1,
      assists: 0,
      shotsTotal: 5,
      shotsOnGoal: 3,
      passesTotal: 36,
      keyPasses: 2,
      yellowCards: 0,
      redCards: 0,
      rating: 7.9,
      xg: 0.63,
    },
  ],
  teamStats: [
    {
      teamId: "arsenal-fc",
      teamName: "Arsenal FC",
      totalShots: 15,
      shotsOnGoal: 6,
      possessionPct: 58,
      totalPasses: 510,
      passesAccurate: 452,
      passAccuracyPct: 88.6,
      corners: 7,
      fouls: 12,
      yellowCards: 2,
      redCards: 0,
      goalkeeperSaves: 3,
    },
    {
      teamId: "chelsea",
      teamName: "Chelsea",
      totalShots: 9,
      shotsOnGoal: 3,
      possessionPct: 42,
      totalPasses: 436,
      passesAccurate: 372,
      passAccuracyPct: 85.3,
      corners: 4,
      fouls: 16,
      yellowCards: 4,
      redCards: 1,
      goalkeeperSaves: 4,
    },
  ],
  sectionCoverage: {
    timeline: {
      status: "complete",
      percentage: 100,
      label: "Timeline",
    },
    lineups: {
      status: "partial",
      percentage: 59.09,
      label: "Lineups",
    },
    teamStats: {
      status: "complete",
      percentage: 100,
      label: "Team stats",
    },
    playerStats: {
      status: "partial",
      percentage: 66.67,
      label: "Player stats",
    },
  },
};

function buildMatchesResponse(): ApiPayload {
  return {
    data: {
      items: [
        {
          matchId: "match-arsenal-chelsea",
          fixtureId: "match-arsenal-chelsea",
          competitionId: "8",
          competitionName: "Premier League",
          seasonId: "2024",
          roundId: "29",
          kickoffAt: "2025-03-09T18:30:00Z",
          status: "FT",
          venueName: "Emirates Stadium",
          homeTeamId: "arsenal-fc",
          homeTeamName: "Arsenal FC",
          awayTeamId: "chelsea",
          awayTeamName: "Chelsea",
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
      pagination: {
        page: 1,
        pageSize: 20,
        totalCount: 1,
        totalPages: 1,
        hasNextPage: false,
        hasPreviousPage: false,
      },
    },
  };
}

async function installMatchCenterMocks(page: Page) {
  await page.route("**/api/v1/matches?**", async (route) => {
    await json(route, buildMatchesResponse());
  });

  await page.route("**/api/v1/matches/match-arsenal-chelsea?**", async (route) => {
    await json(route, {
      data: MATCH_CENTER_PAYLOAD,
      meta: {
        coverage: {
          status: "partial",
          percentage: 75.25,
          label: "Match center sections coverage",
        },
      },
    });
  });

  await page.route("**/api/v1/players/p-saka?**", async (route) => {
    await json(route, {
      data: {
        player: {
          playerId: "p-saka",
          playerName: "Bukayo Saka",
          teamId: "arsenal-fc",
          teamName: "Arsenal FC",
          position: "RW",
          nationality: "Inglaterra",
        },
        summary: {
          matchesPlayed: 28,
          minutesPlayed: 2450,
          goals: 14,
          assists: 9,
          shotsTotal: 82,
          shotsOnTarget: 37,
          passesAttempted: 1020,
          yellowCards: 2,
          redCards: 0,
          rating: 7.9,
        },
        recentMatches: [
          {
            fixtureId: "match-arsenal-chelsea",
            playedAt: "2025-03-09T18:30:00Z",
            opponentName: "Chelsea",
            minutesPlayed: 90,
            goals: 1,
            assists: 0,
            rating: 8.1,
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

  await page.route("**/api/v1/insights?**", async (route) => {
    await json(route, {
      data: [
        {
          insight_id: "player-form-1",
          severity: "info",
          explanation: "Saka sustentou volume ofensivo acima da media no recorte.",
          reference_period: "ultimas 5 partidas",
          evidences: {
            goals: 3,
            shots_total: 14,
          },
          data_source: ["player_match_summary"],
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
  });
}

test.describe("Fluxo matches -> match center", () => {
  test("entra pela lista, preserva contexto, cobre o detalhe e navega por links canonicos", async ({
    page,
  }) => {
    await installMatchCenterMocks(page);

    await page.goto(`/matches?${LIST_CONTEXT_QUERY}`);

    await expect(page.getByRole("heading", { exact: true, name: "Partidas" })).toBeVisible();

    const matchCenterLink = page.getByRole("link", {
      name: /Abrir match center de Arsenal FC vs Chelsea/i,
    });
    await expect(matchCenterLink).toHaveAttribute(
      "href",
      new RegExp(`/matches/match-arsenal-chelsea\\?${LIST_CONTEXT_QUERY.replace(/\?/g, "\\?")}`),
    );
    await matchCenterLink.click();

    await expect(page).toHaveURL(new RegExp(`/matches/match-arsenal-chelsea\\?${LIST_CONTEXT_QUERY}`));
    await expect(
      page.getByRole("heading", { exact: true, name: "Arsenal FC vs Chelsea" }),
    ).toBeVisible();
    await expect(page.locator("#global-filter-competition-id")).toBeDisabled();
    await expect(page.locator("#global-filter-season-id")).toBeDisabled();
    await expect(page.locator("#global-filter-competition-id")).toHaveValue("8");
    await expect(page.locator("#global-filter-season-id")).toHaveValue("2024");
    await expect(page.getByText("Algumas areas ainda estao incompletas")).toBeVisible();

    const homeTeamLink = page.getByRole("link", { name: "Arsenal FC" }).first();
    await expect(homeTeamLink).toHaveAttribute(
      "href",
      /\/competitions\/premier_league\/seasons\/2024%2F2025\/teams\/arsenal-fc\?roundId=29&venue=home&lastN=5/,
    );
    await expect(page.getByRole("link", { name: "Voltar para temporada" })).toHaveAttribute(
      "href",
      /\/competitions\/premier_league\/seasons\/2024%2F2025\?roundId=29&venue=home&lastN=5/,
    );
    await expect(
      page.getByLabel("Atalhos da página").getByRole("link", { name: "Rankings" }),
    ).toHaveAttribute(
      "href",
      /\/competitions\/premier_league\/seasons\/2024%2F2025\?roundId=29&venue=home&lastN=5&tab=rankings/,
    );

    const matchCenterTabs = page.getByLabel("Match center tabs");

    await matchCenterTabs.getByRole("link", { exact: true, name: "Linha do tempo" }).click();
    await expect(page).toHaveURL(/tab=timeline/);
    await expect(page.getByRole("heading", { name: "Linha do tempo da partida" })).toBeVisible();
    await expect(page.getByText("Bukayo Saka · Gol em transicao rapida")).toBeVisible();
    await expect(
      page.getByRole("link", { name: "Bukayo Saka · Gol em transicao rapida" }),
    ).toHaveAttribute(
      "href",
      /\/competitions\/premier_league\/seasons\/2024%2F2025\/players\/p-saka\?roundId=29&venue=home&lastN=5/,
    );

    await matchCenterTabs.getByRole("link", { exact: true, name: "Escalações" }).click();
    await expect(page).toHaveURL(/tab=lineups/);
    await expect(page.getByRole("heading", { name: "Escalacoes da partida" })).toBeVisible();
    await expect(page.getByText("Slots de formacao").first()).toBeVisible();
    const lineupPlayerLink = page.getByRole("link", { name: "Bukayo Saka" }).first();
    await expect(lineupPlayerLink).toHaveAttribute(
      "href",
      /\/competitions\/premier_league\/seasons\/2024%2F2025\/players\/p-saka\?roundId=29&venue=home&lastN=5/,
    );

    await matchCenterTabs.getByRole("link", { exact: true, name: "Times" }).click();
    await expect(page).toHaveURL(/tab=team-stats/);
    await expect(page.getByRole("heading", { name: "Comparativo dos times" })).toBeVisible();
    await expect(page.getByText("Passes totais")).toBeVisible();
    await expect(page.getByText("88,6%")).toBeVisible();
    await expect(page.getByText("85,3%")).toBeVisible();

    await matchCenterTabs.getByRole("link", { exact: true, name: "Jogadores" }).click();
    await expect(page).toHaveURL(/tab=player-stats/);
    await expect(page.getByRole("heading", { name: "Atuacao individual" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "No alvo" })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "KP" })).toBeVisible();

    await page.getByRole("link", { name: "Bukayo Saka" }).first().click();

    await expect(page).toHaveURL(
      /\/competitions\/premier_league\/seasons\/2024%2F2025\/players\/p-saka\?roundId=29&venue=home&lastN=5/,
    );
    await expect(page.getByRole("heading", { name: "Bukayo Saka" })).toBeVisible();
  });
});
