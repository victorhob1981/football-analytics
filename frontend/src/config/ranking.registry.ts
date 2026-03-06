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
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
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
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-offensive-contributions",
    label: "Part. Ofensivas",
    description: "Jogadores com mais participacoes ofensivas (gols + assistencias).",
    entity: "player",
    metricKey: "offensive_contributions",
    endpoint: "/api/v1/rankings/player-offensive-contributions",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
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
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
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
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-key-passes",
    label: "Passes-chave",
    description: "Jogadores com mais passes-chave no recorte selecionado.",
    entity: "player",
    metricKey: "key_passes",
    endpoint: "/api/v1/rankings/player-key-passes",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-tackles",
    label: "Desarmes",
    description: "Jogadores com mais desarmes no recorte selecionado.",
    entity: "player",
    metricKey: "tackles",
    endpoint: "/api/v1/rankings/player-tackles",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-interceptions",
    label: "Interceptacoes",
    description: "Jogadores com mais interceptacoes no recorte selecionado.",
    entity: "player",
    metricKey: "interceptions",
    endpoint: "/api/v1/rankings/player-interceptions",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-duels",
    label: "Duelos",
    description: "Jogadores com mais duelos no recorte selecionado.",
    entity: "player",
    metricKey: "duels",
    endpoint: "/api/v1/rankings/player-duels",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-fouls-committed",
    label: "Faltas Cometidas",
    description: "Jogadores com mais faltas cometidas no recorte selecionado.",
    entity: "player",
    metricKey: "fouls_committed",
    endpoint: "/api/v1/rankings/player-fouls-committed",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
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
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
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
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
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
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
    coverageWarning: "Cobertura de cartoes pode variar por provider.",
  },
  {
    id: "player-cards-total",
    label: "Cartoes",
    description: "Jogadores com mais cartoes (amarelos + vermelhos) no recorte selecionado.",
    entity: "player",
    metricKey: "cards_total",
    endpoint: "/api/v1/rankings/player-cards-total",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
    coverageWarning: "Cobertura de cartoes pode variar por provider.",
  },
  {
    id: "player-goals-per-90",
    label: "Gols/90",
    description: "Jogadores com maior media de gols por 90 minutos.",
    entity: "player",
    metricKey: "goals_per_90",
    endpoint: "/api/v1/rankings/player-goals-per-90",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-assists-per-90",
    label: "Assistencias/90",
    description: "Jogadores com maior media de assistencias por 90 minutos.",
    entity: "player",
    metricKey: "assists_per_90",
    endpoint: "/api/v1/rankings/player-assists-per-90",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-shots-total-per-90",
    label: "Chutes/90",
    description: "Jogadores com maior media de finalizacoes por 90 minutos.",
    entity: "player",
    metricKey: "shots_total_per_90",
    endpoint: "/api/v1/rankings/player-shots-total-per-90",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-key-passes-per-90",
    label: "Passes-chave/90",
    description: "Jogadores com maior media de passes-chave por 90 minutos.",
    entity: "player",
    metricKey: "key_passes_per_90",
    endpoint: "/api/v1/rankings/player-key-passes-per-90",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-tackles-per-90",
    label: "Desarmes/90",
    description: "Jogadores com maior media de desarmes por 90 minutos.",
    entity: "player",
    metricKey: "tackles_per_90",
    endpoint: "/api/v1/rankings/player-tackles-per-90",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "player-interceptions-per-90",
    label: "Interceptacoes/90",
    description: "Jogadores com maior media de interceptacoes por 90 minutos.",
    entity: "player",
    metricKey: "interceptions_per_90",
    endpoint: "/api/v1/rankings/player-interceptions-per-90",
    defaultSort: "desc",
    minSample: { field: "minutes_played", min: 180 },
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "team-possession",
    label: "Posse de Bola",
    description: "Times com maior posse media no recorte selecionado.",
    entity: "team",
    metricKey: "team_possession_pct",
    endpoint: "/api/v1/rankings/team-possession",
    defaultSort: "desc",
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
  },
  {
    id: "team-pass-accuracy",
    label: "Precisao de Passe do Time",
    description: "Times com melhor precisao de passe no recorte selecionado.",
    entity: "team",
    metricKey: "team_pass_accuracy_pct",
    endpoint: "/api/v1/rankings/team-pass-accuracy",
    defaultSort: "desc",
    availableFilters: ["competitionId", "seasonId", "roundId", "month", "venue", "lastN", "dateRange"],
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
