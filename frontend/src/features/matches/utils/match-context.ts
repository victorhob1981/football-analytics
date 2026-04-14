import type { MatchListItem, MatchStageFormat } from "@/features/matches/types";

type MatchCompetitionSemantics = "league" | "cup";

export type MatchDisplayContext = {
  semantics: MatchCompetitionSemantics;
  summary: string;
  tags: string[];
};

function normalizeText(value: string | null | undefined): string | null {
  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}

function normalizeLookupValue(value: string | null | undefined): string {
  return (
    normalizeText(value)
      ?.normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLowerCase() ?? ""
  );
}

function normalizeRoundId(value: string | null | undefined): string | null {
  const normalized = normalizeText(value);

  if (!normalized) {
    return null;
  }

  const numericValue = Number(normalized);

  if (Number.isFinite(numericValue) && numericValue <= 0) {
    return null;
  }

  return normalized;
}

function normalizeCompetitionType(value: string | null | undefined): string | null {
  return normalizeLookupValue(value) || null;
}

function resolveSemantics(match: MatchListItem): MatchCompetitionSemantics {
  const competitionType = normalizeCompetitionType(match.competitionType);

  if (competitionType?.includes("league")) {
    return "league";
  }

  if (
    competitionType?.includes("cup") ||
    competitionType?.includes("hybrid") ||
    competitionType?.includes("knockout")
  ) {
    return "cup";
  }

  if (
    match.stageFormat === "group_table" ||
    match.stageFormat === "knockout" ||
    match.stageFormat === "qualification_knockout" ||
    match.stageFormat === "placement_match" ||
    match.tieId ||
    match.legNumber ||
    match.isKnockout
  ) {
    return "cup";
  }

  return "league";
}

function fallbackStageLabel(stageFormat: MatchStageFormat): string | null {
  switch (stageFormat) {
    case "league_table":
      return "Fase de liga";
    case "group_table":
      return "Fase de grupos";
    case "qualification_knockout":
      return "Preliminar";
    case "knockout":
      return "Mata-mata";
    case "placement_match":
      return "Disputa de colocação";
    default:
      return null;
  }
}

function translateStageLabel(stageName: string | null | undefined, stageFormat: MatchStageFormat): string | null {
  const normalizedStageName = normalizeText(stageName);

  if (!normalizedStageName) {
    return fallbackStageLabel(stageFormat);
  }

  const lookup = normalizeLookupValue(normalizedStageName);

  if (lookup === "league stage" || lookup === "league phase") {
    return "Fase de liga";
  }

  if (lookup === "group stage" || lookup === "fase de grupos") {
    return "Fase de grupos";
  }

  if (lookup === "1st round" || lookup === "first round" || lookup === "primeira fase") {
    return "Primeira fase";
  }

  if (lookup === "2nd round" || lookup === "second round" || lookup === "segunda fase") {
    return "Segunda fase";
  }

  if (lookup === "3rd round" || lookup === "third round" || lookup === "terceira fase") {
    return "Terceira fase";
  }

  if (
    lookup === "round of 16" ||
    lookup === "8th finals" ||
    lookup === "eighth finals" ||
    lookup === "oitavas de final"
  ) {
    return "Oitavas de final";
  }

  if (lookup === "quarter-finals" || lookup === "quarter finals" || lookup === "quartas de final") {
    return "Quartas de final";
  }

  if (lookup === "semi-finals" || lookup === "semi finals" || lookup === "semifinal") {
    return "Semifinal";
  }

  if (lookup === "final") {
    return "Final";
  }

  if (lookup === "third place" || lookup === "third place match") {
    return "Terceiro lugar";
  }

  if (lookup.includes("preliminary round") || lookup === "preliminar") {
    return "Preliminar";
  }

  if (lookup === "playoffs" || lookup === "play-offs" || lookup === "playoff") {
    return "Playoff";
  }

  return normalizedStageName;
}

function resolveLeagueLabel(match: MatchListItem): string | null {
  const normalizedRoundId = normalizeRoundId(match.roundId);

  if (normalizedRoundId) {
    return `Rodada ${normalizedRoundId}`;
  }

  return normalizeText(match.roundName) ?? translateStageLabel(match.stageName, match.stageFormat);
}

function resolveGroupLabel(match: MatchListItem): string | null {
  const groupName = normalizeText(match.groupName);

  if (groupName) {
    if (/^group\s+/i.test(groupName)) {
      return `Grupo ${groupName.replace(/^group\s+/i, "").trim()}`;
    }

    return groupName;
  }

  const groupId = normalizeText(match.groupId);

  if (!groupId) {
    return null;
  }

  if (/^grupo\s+/i.test(groupId)) {
    return groupId;
  }

  if (/^group\s+/i.test(groupId)) {
    return `Grupo ${groupId.replace(/^group\s+/i, "").trim()}`;
  }

  if (/^[a-z0-9]+$/i.test(groupId)) {
    return `Grupo ${groupId.toUpperCase()}`;
  }

  return groupId;
}

function resolveLegLabel(match: MatchListItem): string | null {
  if (typeof match.legNumber === "number" && Number.isFinite(match.legNumber)) {
    if (match.legNumber === 1) {
      return "Ida";
    }

    if (match.legNumber === 2) {
      return "Volta";
    }

    return `Jogo ${match.legNumber}`;
  }

  if (match.tieId && match.tieMatchCount === 1) {
    return "Jogo unico";
  }

  return null;
}

function resolveTieLabel(match: MatchListItem): string | null {
  if (typeof match.tieOrder === "number" && Number.isFinite(match.tieOrder) && match.tieOrder > 0) {
    return `Confronto ${match.tieOrder}`;
  }

  return match.tieId ? "Confronto" : null;
}

function dedupeLabels(labels: Array<string | null | undefined>): string[] {
  const seen = new Set<string>();
  const result: string[] = [];

  for (const label of labels) {
    const normalizedLabel = normalizeText(label);

    if (!normalizedLabel) {
      continue;
    }

    const lookup = normalizeLookupValue(normalizedLabel);

    if (seen.has(lookup)) {
      continue;
    }

    seen.add(lookup);
    result.push(normalizedLabel);
  }

  return result;
}

export function resolveMatchDisplayContext(match: MatchListItem): MatchDisplayContext {
  const semantics = resolveSemantics(match);

  if (semantics === "league") {
    const primaryLabel = resolveLeagueLabel(match) ?? "Partida";
    const tags = dedupeLabels([primaryLabel]);

    return {
      semantics,
      summary: primaryLabel,
      tags,
    };
  }

  const primaryLabel =
    translateStageLabel(match.stageName, match.stageFormat) ??
    normalizeText(match.roundName) ??
    "Partida";
  const tags = dedupeLabels([
    primaryLabel,
    match.stageFormat === "group_table" ? resolveGroupLabel(match) : null,
    resolveLegLabel(match),
    resolveTieLabel(match),
  ]);

  return {
    semantics,
    summary: tags.join(" · ") || primaryLabel,
    tags,
  };
}

export function resolveMatchRoundFilter(
  match: MatchListItem,
  fallbackRoundId: string | null | undefined,
): string | null {
  const normalizedFallbackRoundId = normalizeRoundId(fallbackRoundId);

  if (normalizedFallbackRoundId) {
    return normalizedFallbackRoundId;
  }

  return resolveMatchDisplayContext(match).semantics === "league"
    ? normalizeRoundId(match.roundId)
    : null;
}
