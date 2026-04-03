import { describe, expect, it } from "vitest";

import {
  appendFilterQueryString,
  buildPassthroughSearchParamsQueryString,
  buildCompetitionHubPath,
  buildContextFilterQueryString,
  buildCanonicalPlayerPath,
  buildFilterQueryString,
  buildMatchCenterPath,
  buildMatchesPath,
  buildPlayersPath,
  buildTeamsPath,
  buildRankingPath,
  buildSeasonHubPath,
  buildSeasonHubTabPath,
  buildCanonicalTeamPath,
  buildPlayerResolverPath,
  getContextQueryKeysToLockForPath,
  getContextQueryKeysToOmitForPath,
  resolveCompetitionSeasonContext,
  resolveCompetitionSeasonContextFromSearchParams,
} from "@/shared/utils/context-routing";
import { getCompetitionByKey } from "@/config/competitions.registry";
import { listSeasonsForCompetition } from "@/config/seasons.registry";

describe("context-routing", () => {
  it("resolve contexto quando recebe ids conhecidos", () => {
    expect(
      resolveCompetitionSeasonContext({
        competitionId: "71",
        seasonId: "2024",
      }),
    ).toEqual({
      competitionId: "71",
      competitionKey: "brasileirao_a",
      competitionName: "Campeonato Brasileiro Série A",
      seasonId: "2024",
      seasonLabel: "2024",
    });
  });

  it("resolve contexto anual dinamico fora do catalogo estatico", () => {
    expect(
      resolveCompetitionSeasonContext({
        competitionId: "71",
        seasonId: "2025",
      }),
    ).toEqual({
      competitionId: "71",
      competitionKey: "brasileirao_a",
      competitionName: "Campeonato Brasileiro Série A",
      seasonId: "2025",
      seasonLabel: "2025",
    });
  });

  it("retorna null quando competitionId e competitionKey conflitam", () => {
    expect(
      resolveCompetitionSeasonContext({
        competitionId: "71",
        competitionKey: "premier_league",
        seasonId: "2024",
      }),
    ).toBeNull();
  });

  it("resolve contexto a partir de search params", () => {
    const searchParams = new URLSearchParams({
      competitionId: "8",
      seasonId: "2024",
    });

    expect(resolveCompetitionSeasonContextFromSearchParams(searchParams)).toEqual({
      competitionId: "8",
      competitionKey: "premier_league",
      competitionName: "Premier League",
      seasonId: "2024",
      seasonLabel: "2024/2025",
    });
  });

  it("gera rotas canonicas com encode seguro de seasonLabel", () => {
    const context = resolveCompetitionSeasonContext({
      competitionId: "8",
      seasonId: "2024",
    });

    expect(context).not.toBeNull();
    expect(buildCanonicalPlayerPath(context!, "99")).toBe(
      "/competitions/premier_league/seasons/2024%2F2025/players/99",
    );
    expect(buildCanonicalTeamPath(context!, "33")).toBe(
      "/competitions/premier_league/seasons/2024%2F2025/teams/33",
    );
    expect(buildSeasonHubPath(context!)).toBe("/competitions/premier_league/seasons/2024%2F2025");
    expect(buildSeasonHubTabPath(context!, "rankings")).toBe(
      "/competitions/premier_league/seasons/2024%2F2025?tab=rankings",
    );
    expect(
      buildSeasonHubTabPath(context!, "calendar", {
        roundId: "29",
        venue: "home",
        lastN: 5,
      }),
    ).toBe("/competitions/premier_league/seasons/2024%2F2025?roundId=29&venue=home&lastN=5");
  });

  it("anexa contexto quando monta rota resolver", () => {
    expect(
      buildPlayerResolverPath("123", {
        competitionId: "71",
        seasonId: "2024",
      }),
    ).toBe("/players/123?competitionId=71&seasonId=2024");
    expect(
      buildMatchCenterPath("19135048", {
        competitionId: "8",
        seasonId: "2024",
        roundId: "29",
      }),
    ).toBe("/matches/19135048?competitionId=8&seasonId=2024&roundId=29");
    expect(
      buildPlayersPath({
        competitionId: "71",
        seasonId: "2024",
        venue: "away",
      }),
    ).toBe("/players?competitionId=71&seasonId=2024&venue=away");
    expect(
      buildTeamsPath({
        competitionId: "71",
        seasonId: "2024",
        lastN: 5,
      }),
    ).toBe("/teams?competitionId=71&seasonId=2024&lastN=5");
    expect(
      buildMatchesPath({
        competitionId: "71",
        seasonId: "2024",
        dateRangeStart: "2025-01-01",
        dateRangeEnd: "2025-01-31",
      }),
    ).toBe("/matches?competitionId=71&seasonId=2024&dateRangeStart=2025-01-01&dateRangeEnd=2025-01-31");
    expect(
      buildRankingPath("player-goals", {
        competitionId: "71",
        seasonId: "2024",
        lastN: 8,
      }),
    ).toBe("/rankings/player-goals?competitionId=71&seasonId=2024&lastN=8");
    expect(
      buildRankingPath("player-goals", {
        competitionId: "390",
        seasonId: "2024",
        stageId: "77468966",
        stageFormat: "group_table",
      }),
    ).toBe("/rankings/player-goals?competitionId=390&seasonId=2024&stageId=77468966&stageFormat=group_table");
  });

  it("gera rota do competition hub e query de contexto para telas nao canonicas", () => {
    expect(buildCompetitionHubPath("brasileirao_a")).toBe("/competitions/brasileirao_a");
    expect(
      buildContextFilterQueryString({
        competitionId: "71",
        seasonId: "2024",
      }),
    ).toBe("?competitionId=71&seasonId=2024");
  });

  it("filtra temporadas compativeis com o calendario da competicao", () => {
    const brasileirao = getCompetitionByKey("brasileirao_a");
    const premierLeague = getCompetitionByKey("premier_league");
    const primeiraLiga = getCompetitionByKey("primeira_liga");

    expect(listSeasonsForCompetition(brasileirao).map((season) => season.id)).toEqual([
      "2025",
      "2024",
      "2023",
      "2022",
      "2021",
    ]);
    expect(listSeasonsForCompetition(premierLeague).map((season) => season.id)).toEqual([
      "2024/2025",
      "2023/2024",
      "2022/2023",
      "2021/2022",
    ]);
    expect(listSeasonsForCompetition(primeiraLiga).map((season) => season.id)).toEqual([
      "2024/2025",
      "2023/2024",
    ]);
  });

  it("resolve seasonLabel de catalogo para a rota canonica exibida", () => {
    expect(
      resolveCompetitionSeasonContext({
        competitionKey: "champions_league",
        seasonLabel: "2024_25",
      }),
    ).toEqual({
      competitionId: "2",
      competitionKey: "champions_league",
      competitionName: "UEFA Champions League",
      seasonId: "2024",
      seasonLabel: "2024/2025",
    });
  });

  it("resolve seasonLabel anual dinamico vindo da rota canonica", () => {
    expect(
      resolveCompetitionSeasonContext({
        competitionKey: "brasileirao_a",
        seasonLabel: "2025",
      }),
    ).toEqual({
      competitionId: "71",
      competitionKey: "brasileirao_a",
      competitionName: "Campeonato Brasileiro Série A",
      seasonId: "2025",
      seasonLabel: "2025",
    });
  });

  it("resolve seasonLabel percent-encoded vindo da rota canonica", () => {
    expect(
      resolveCompetitionSeasonContext({
        competitionKey: "premier_league",
        seasonLabel: "2024%2F2025",
      }),
    ).toEqual({
      competitionId: "8",
      competitionKey: "premier_league",
      competitionName: "Premier League",
      seasonId: "2024",
      seasonLabel: "2024/2025",
    });
  });

  it("resolve temporada cruzada dinamica fora do catalogo estatico", () => {
    expect(
      resolveCompetitionSeasonContext({
        competitionKey: "premier_league",
        seasonLabel: "2025%2F2026",
      }),
    ).toEqual({
      competitionId: "8",
      competitionKey: "premier_league",
      competitionName: "Premier League",
      seasonId: "2025",
      seasonLabel: "2025/2026",
    });
  });

  it("recusa temporada fora do escopo parcial da primeira liga", () => {
    expect(
      resolveCompetitionSeasonContext({
        competitionId: "462",
        seasonId: "2022",
      }),
    ).toBeNull();
  });

  it("recusa temporada anual sem ingestao no catalogo canonico", () => {
    expect(
      resolveCompetitionSeasonContext({
        competitionId: "71",
        seasonId: "2020",
      }),
    ).toBeNull();
  });

  it("omite query de contexto quando a rota ja carrega contexto canonico no path", () => {
    expect(getContextQueryKeysToOmitForPath("/matches/m-1")).toEqual([]);
    expect(getContextQueryKeysToOmitForPath("/competitions/brasileirao_a")).toEqual([
      "competitionId",
    ]);
    expect(
      getContextQueryKeysToOmitForPath("/competitions/brasileirao_a/seasons/2024/teams/1"),
    ).toEqual(["competitionId", "seasonId"]);
  });

  it("trava apenas o contexto que precisa permanecer canonico por rota", () => {
    expect(getContextQueryKeysToLockForPath("/matches/m-1")).toEqual([
      "competitionId",
      "seasonId",
    ]);
    expect(getContextQueryKeysToLockForPath("/competitions/brasileirao_a")).toEqual([]);
    expect(
      getContextQueryKeysToLockForPath("/competitions/brasileirao_a/seasons/2024/teams/1"),
    ).toEqual(["seasonId"]);
  });

  it("serializa filtros extras e omite contexto quando o path ja e canonico", () => {
    expect(
      buildFilterQueryString(
        {
          competitionId: "8",
          seasonId: "2024",
          roundId: "29",
          venue: "home",
          lastN: 5,
        },
        ["competitionId", "seasonId"],
      ),
    ).toBe("?roundId=29&venue=home&lastN=5");

    expect(
      appendFilterQueryString("/competitions/premier_league/seasons/2024%2F2025", {
        competitionId: "8",
        seasonId: "2024",
        roundId: "29",
        venue: "home",
      }, ["competitionId", "seasonId"]),
    ).toBe("/competitions/premier_league/seasons/2024%2F2025?roundId=29&venue=home");
  });

  it("preserva query crua em reentradas legadas relevantes", () => {
    expect(
      buildPassthroughSearchParamsQueryString({
        competitionId: "8",
        seasonId: "2024",
        roundId: "29",
        venue: "home",
        lastN: "5",
      }),
    ).toBe("?competitionId=8&seasonId=2024&roundId=29&venue=home&lastN=5");

    expect(
      buildPassthroughSearchParamsQueryString({
        competitionId: "8",
        empty: "",
        repeated: ["home", "away"],
      }),
    ).toBe("?competitionId=8&repeated=home&repeated=away");
  });
});
