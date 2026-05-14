import type { RankingDefinition, RankingSortDirection } from "@/config/ranking.types";
import type { VenueFilter } from "@/shared/types/filters.types";

export interface RankingTableRow extends Record<string, string | number | boolean | null | undefined> {
  entityId: string;
  entityName?: string | null;
  rank?: number | null;
  metricValue?: number | null;
}

export interface RankingTableData {
  rankingId: string;
  metricKey: string;
  rows: RankingTableRow[];
  updatedAt?: string | null;
}

export interface RankingsGlobalFilters {
  competitionId?: string | null;
  seasonId?: string | null;
  roundId?: string | null;
  venue?: VenueFilter;
  lastN?: number | null;
  dateRangeStart?: string | null;
  dateRangeEnd?: string | null;
}

export type RankingFreshnessClass = "season" | "fast";

export interface RankingLocalFilters {
  search?: string;
  page?: number;
  pageSize?: number;
  sortDirection?: RankingSortDirection;
  minSampleValue?: number | null;
  freshnessClass?: RankingFreshnessClass;
}

export type RankingQueryFilters = RankingsGlobalFilters & RankingLocalFilters;

export interface RankingCacheProfile {
  staleTimeMs: number;
  gcTimeMs: number;
  freshnessClass: RankingFreshnessClass;
}

export interface UseRankingTableOptions {
  localFilters?: RankingLocalFilters;
  enabled?: boolean;
}

export interface RankingFetchInput {
  rankingDefinition: RankingDefinition;
  filters?: RankingQueryFilters;
}
