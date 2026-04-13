import type {
  CompetitionStageFormat,
  CompetitionStructureData,
  CompetitionStructureStage,
} from "@/features/competitions/types/competition-structure.types";

export function isKnockoutStageFormat(stageFormat: CompetitionStageFormat): boolean {
  return (
    stageFormat === "knockout" ||
    stageFormat === "qualification_knockout" ||
    stageFormat === "placement_match"
  );
}

export function isTableStageFormat(stageFormat: CompetitionStageFormat): boolean {
  return stageFormat === "league_table" || stageFormat === "group_table";
}

export function getStageFormatLabel(stageFormat: CompetitionStageFormat): string {
  switch (stageFormat) {
    case "league_table":
      return "Fase classificatória";
    case "group_table":
      return "Fase de grupos";
    case "qualification_knockout":
      return "Eliminatória preliminar";
    case "knockout":
      return "Mata-mata";
    case "placement_match":
      return "Disputa de colocação";
    default:
      return "Estrutura";
  }
}

export function describeCompetitionEdition(structure: CompetitionStructureData | null | undefined): string | null {
  if (!structure) {
    return null;
  }

  const stageFormats = new Set(
    structure.stages
      .map((stage) => stage.stageFormat)
      .filter((stageFormat): stageFormat is Exclude<CompetitionStageFormat, null | undefined> => Boolean(stageFormat)),
  );

  const hasLeagueTable = stageFormats.has("league_table");
  const hasGroupTable = stageFormats.has("group_table");
  const hasKnockout =
    stageFormats.has("knockout") ||
    stageFormats.has("qualification_knockout") ||
    stageFormats.has("placement_match");

  if (hasLeagueTable && hasKnockout) {
    return "Fase classificatória + mata-mata";
  }

  if (hasGroupTable && hasKnockout) {
    return "Fase de grupos + mata-mata";
  }

  if (hasGroupTable) {
    return "Fase de grupos";
  }

  if (hasLeagueTable) {
    return "Fase classificatória";
  }

  if (hasKnockout) {
    return "Mata-mata";
  }

  return structure.competition.formatFamily || null;
}

export function getDefaultStructureStage(
  stages: CompetitionStructureStage[],
): CompetitionStructureStage | null {
  if (stages.length === 0) {
    return null;
  }

  return (
    stages.find((stage) => stage.isCurrent) ??
    stages.find((stage) => stage.stageFormat === "group_table" || stage.stageFormat === "league_table") ??
    stages.find((stage) => stage.stageFormat === "knockout") ??
    stages.find((stage) => stage.stageFormat === "qualification_knockout") ??
    stages.find((stage) => stage.stageFormat === "placement_match") ??
    stages[0]
  );
}
