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

function normalizeCompetitionStageIdentityValue(value: string | null | undefined): string {
  return (value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

export function localizeCompetitionStageName(value: string | null | undefined): string {
  const normalizedValue = normalizeCompetitionStageIdentityValue(value);

  if (!normalizedValue) {
    return "Fase";
  }

  if (
    normalizedValue.includes("round_of_16") ||
    normalizedValue === "round_16" ||
    normalizedValue.includes("last_16") ||
    normalizedValue.includes("8th_final") ||
    normalizedValue.includes("eighth_final") ||
    normalizedValue.includes("oitavas")
  ) {
    return "Oitavas de final";
  }

  if (
    normalizedValue.includes("quarter_final") ||
    normalizedValue.includes("quarter_finals") ||
    normalizedValue.includes("round_of_8") ||
    normalizedValue === "round_8" ||
    normalizedValue.includes("quartas")
  ) {
    return "Quartas de final";
  }

  if (
    normalizedValue.includes("semi_final") ||
    normalizedValue.includes("semi_finals") ||
    normalizedValue.includes("semifinal")
  ) {
    return "Semifinais";
  }

  if (
    normalizedValue === "final" ||
    normalizedValue === "finals" ||
    (normalizedValue.includes("final") &&
      !normalizedValue.includes("quarter") &&
      !normalizedValue.includes("semi") &&
      !normalizedValue.includes("eighth"))
  ) {
    return "Final";
  }

  if (
    normalizedValue.includes("group_stage") ||
    normalizedValue.includes("group_phase") ||
    normalizedValue.includes("fase_de_grupos")
  ) {
    return "Fase de grupos";
  }

  if (
    normalizedValue.includes("league_stage") ||
    normalizedValue.includes("league_phase") ||
    normalizedValue.includes("fase_de_liga")
  ) {
    return "Fase classificatória";
  }

  if (normalizedValue.includes("knockout") || normalizedValue.includes("mata_mata")) {
    return "Mata-mata";
  }

  if (
    normalizedValue.includes("playoff") ||
    normalizedValue.includes("playoffs") ||
    normalizedValue.includes("play_off")
  ) {
    return "Repescagem";
  }

  if (normalizedValue.includes("third") || normalizedValue.includes("terceiro")) {
    return "Disputa de 3º lugar";
  }

  if (
    normalizedValue.includes("1st_round") ||
    normalizedValue.includes("first_round") ||
    normalizedValue === "round_1"
  ) {
    return "Primeira fase";
  }

  if (
    normalizedValue.includes("2nd_round") ||
    normalizedValue.includes("second_round") ||
    normalizedValue === "round_2"
  ) {
    return "Segunda fase";
  }

  if (
    normalizedValue.includes("3rd_round") ||
    normalizedValue.includes("third_round") ||
    normalizedValue === "round_3"
  ) {
    return "Terceira fase";
  }

  return value ?? "Fase";
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
