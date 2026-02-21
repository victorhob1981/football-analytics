import { METRICS_REGISTRY, getMetric } from "@/config/metrics.registry";
import type { RankingDefinition, RankingEntity, RankingRegistry } from "@/config/ranking.types";

// Adicionar ranking = adicionar entrada no registry; nao criar componente novo.
type RegisteredMetricKey = keyof typeof METRICS_REGISTRY;
type RegistryRankingDefinition = Omit<RankingDefinition, "metricKey"> & {
  metricKey: RegisteredMetricKey;
};

const RANKINGS_LIST: RegistryRankingDefinition[] = [
  {
    id: "player-goals",
    label: "Artilharia",
    description: "Jogadores com mais gols no recorte selecionado.",
    entity: "player",
    metricKey: "goals",
    endpoint: "/api/v1/rankings/player-goals",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-assists",
    label: "Assistencias",
    description: "Jogadores com mais assistencias no recorte selecionado.",
    entity: "player",
    metricKey: "assists",
    endpoint: "/api/v1/rankings/player-assists",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-shots-total",
    label: "Finalizacoes",
    description: "Jogadores com mais finalizacoes totais.",
    entity: "player",
    metricKey: "shots_total",
    endpoint: "/api/v1/rankings/player-shots-total",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-shots-on-target",
    label: "Finalizacoes no Alvo",
    description: "Jogadores com mais finalizacoes no alvo.",
    entity: "player",
    metricKey: "shots_on_target",
    endpoint: "/api/v1/rankings/player-shots-on-target",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-pass-accuracy",
    label: "Precisao de Passe",
    description: "Jogadores com maior percentual de acerto de passe.",
    entity: "player",
    metricKey: "pass_accuracy_pct",
    endpoint: "/api/v1/rankings/player-pass-accuracy",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-rating",
    label: "Rating",
    description: "Jogadores com melhor rating medio no recorte.",
    entity: "player",
    metricKey: "player_rating",
    endpoint: "/api/v1/rankings/player-rating",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "venue", "lastN", "dateRange"],
    coverageWarning: "Rating depende da disponibilidade da fonte de origem.",
  },
  {
    id: "player-yellow-cards",
    label: "Cartoes Amarelos",
    description: "Jogadores com mais cartoes amarelos.",
    entity: "player",
    metricKey: "yellow_cards",
    endpoint: "/api/v1/rankings/player-yellow-cards",
    defaultSort: "desc",
    availableFilters: ["competitionId", "seasonId", "roundId", "venue", "lastN", "dateRange"],
    coverageWarning: "Cobertura de cartoes pode variar por provider.",
  },
  {
    id: "team-possession",
    label: "Posse de Bola",
    description: "Times com maior posse media no recorte selecionado.",
    entity: "team",
    metricKey: "team_possession_pct",
    endpoint: "/api/v1/rankings/team-possession",
    defaultSort: "desc",
    availableFilters: ["competitionId", "seasonId", "roundId", "venue", "lastN", "dateRange"],
  },
  {
    id: "team-pass-accuracy",
    label: "Precisao de Passe do Time",
    description: "Times com melhor precisao de passe no recorte selecionado.",
    entity: "team",
    metricKey: "team_pass_accuracy_pct",
    endpoint: "/api/v1/rankings/team-pass-accuracy",
    defaultSort: "desc",
    availableFilters: ["competitionId", "seasonId", "roundId", "venue", "lastN", "dateRange"],
  },
];

function createRankingRegistry(rankings: RegistryRankingDefinition[]): RankingRegistry {
  return rankings.reduce<RankingRegistry>((registry, ranking) => {
    if (registry[ranking.id]) {
      throw new Error(`Duplicate ranking id detected in registry: ${ranking.id}`);
    }

    if (!getMetric(ranking.metricKey)) {
      throw new Error(
        `Unknown metric key "${ranking.metricKey}" in ranking "${ranking.id}". Add the metric to metrics.registry.ts first.`,
      );
    }

    const metric = getMetric(ranking.metricKey);

    registry[ranking.id] = {
      ...ranking,
      format: ranking.format ?? metric?.format,
      coverageWarning: ranking.coverageWarning ?? metric?.coverageWarning,
    };

    return registry;
  }, {});
}

export const RANKING_REGISTRY: Readonly<RankingRegistry> = Object.freeze(createRankingRegistry(RANKINGS_LIST));

export const RANKING_DEFINITIONS: ReadonlyArray<RankingDefinition> = Object.freeze(Object.values(RANKING_REGISTRY));

export function getRankingDefinition(rankingType: string): RankingDefinition | undefined {
  return RANKING_REGISTRY[rankingType];
}

export function listRankingsByEntity(entity: RankingEntity): RankingDefinition[] {
  return RANKING_DEFINITIONS.filter((ranking) => ranking.entity === entity);
}
