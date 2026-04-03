import { describe, expect, it } from "vitest";

import type { MatchListItem } from "@/features/matches/types";
import {
  resolveMatchDisplayContext,
  resolveMatchRoundFilter,
} from "@/features/matches/utils/match-context";

function buildMatch(overrides: Partial<MatchListItem>): MatchListItem {
  return {
    matchId: "1",
    ...overrides,
  };
}

describe("match-context utils", () => {
  it("mantem semantica de rodada para ligas", () => {
    const context = resolveMatchDisplayContext(
      buildMatch({
        competitionType: "league",
        roundId: "28",
      }),
    );

    expect(context.semantics).toBe("league");
    expect(context.summary).toBe("Rodada 28");
    expect(context.tags).toEqual(["Rodada 28"]);
  });

  it("prioriza fase e contexto de grupo em competicoes hibridas", () => {
    const context = resolveMatchDisplayContext(
      buildMatch({
        competitionType: "continental_cup",
        stageFormat: "group_table",
        stageName: "Group Stage",
        groupId: "A",
        roundId: "6",
      }),
    );

    expect(context.semantics).toBe("cup");
    expect(context.summary).toBe("Fase de grupos · Grupo A");
    expect(context.tags).toEqual(["Fase de grupos", "Grupo A"]);
  });

  it("remove fallback indevido de rodada em mata-mata e explicita ida e confronto", () => {
    const context = resolveMatchDisplayContext(
      buildMatch({
        competitionType: "continental_cup",
        stageFormat: "knockout",
        stageName: "Quarter-finals",
        roundId: "0",
        tieId: "tie-7",
        tieOrder: 7,
        tieMatchCount: 2,
        legNumber: 1,
      }),
    );

    expect(context.semantics).toBe("cup");
    expect(context.summary).toBe("Quartas de final · Ida · Confronto 7");
    expect(context.tags).toEqual(["Quartas de final", "Ida", "Confronto 7"]);
  });

  it("nao injeta roundId da partida em copas ao construir navegacao", () => {
    const match = buildMatch({
      competitionType: "continental_cup",
      stageFormat: "knockout",
      roundId: "0",
      stageName: "Semi-finals",
    });

    expect(resolveMatchRoundFilter(match, null)).toBeNull();
    expect(resolveMatchRoundFilter(match, "4")).toBe("4");
  });
});
