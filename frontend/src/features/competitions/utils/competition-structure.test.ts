import { describe, expect, it } from "vitest";

import type { CompetitionStructureData } from "@/features/competitions/types";
import {
  describeCompetitionEdition,
  getDefaultStructureStage,
} from "@/features/competitions/utils/competition-structure";

function buildStructure(
  stageFormats: Array<
    "league_table" | "group_table" | "knockout" | "qualification_knockout" | "placement_match"
  >,
): CompetitionStructureData {
  return {
    competition: {
      competitionKey: "test_competition",
      competitionName: "Test Competition",
      seasonLabel: "2024",
      formatFamily: "hybrid",
      seasonFormatCode: "test_format",
      participantScope: "club",
    },
    stages: stageFormats.map((stageFormat, index) => ({
      stageId: String(index + 1),
      stageName: `Stage ${index + 1}`,
      stageCode: `stage_${index + 1}`,
      stageFormat,
      stageOrder: index + 1,
      standingsContextMode: null,
      bracketContextMode: null,
      groupMode: null,
      eliminationMode: null,
      isCurrent: false,
      groups: [],
      transitions: [],
    })),
  };
}

describe("competition-structure utils", () => {
  it("descreve formato hibrido com grupos e mata-mata sem depender de competitionKey", () => {
    expect(
      describeCompetitionEdition(buildStructure(["qualification_knockout", "group_table", "knockout"])),
    ).toBe("Fase de grupos + mata-mata");
  });

  it("descreve formato hibrido com league phase e mata-mata sem depender de competitionKey", () => {
    expect(
      describeCompetitionEdition(buildStructure(["qualification_knockout", "league_table", "knockout"])),
    ).toBe("League phase + mata-mata");
  });

  it("prioriza a fase de tabela principal ao escolher a etapa padrao", () => {
    const structure = buildStructure(["qualification_knockout", "league_table", "knockout"]);

    expect(getDefaultStructureStage(structure.stages)?.stageFormat).toBe("league_table");
  });

  it("reconhece placement_match como fase eliminatoria estrutural explicita", () => {
    const structure = buildStructure(["knockout", "placement_match"]);

    expect(describeCompetitionEdition(structure)).toBe("Mata-mata");
  });
});
