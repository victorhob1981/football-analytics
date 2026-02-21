import type { MetricFormat } from "@/config/metrics.types";

export type RankingEntity = "player" | "team" | "coach" | "h2h";

export type RankingSortDirection = "asc" | "desc";

export type RankingAvailableFilter =
  | "competitionId"
  | "seasonId"
  | "roundId"
  | "venue"
  | "lastN"
  | "dateRange";

export interface RankingMinSample {
  field: "minutes_played" | "matches_played";
  min: number;
}

export interface RankingDefinition {
  id: string;
  label: string;
  description: string;
  entity: RankingEntity;
  metricKey: string;
  format?: MetricFormat;
  endpoint: string;
  defaultSort: RankingSortDirection;
  minSample?: RankingMinSample;
  availableFilters: RankingAvailableFilter[];
  coverageWarning?: string;
}

export type RankingRegistry = Record<string, RankingDefinition>;
