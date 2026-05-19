import { useMemo } from "react";

import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { competitionStructureQueryKeys } from "@/features/competitions/queryKeys";
import { fetchCompetitionAnalytics } from "@/features/competitions/services/competition-hub.service";
import type {
  CompetitionAnalyticsData,
  CompetitionAnalyticsFilters,
} from "@/features/competitions/types/competition-structure.types";

const ANALYTICS_STALE_TIME_MS = 5 * 60 * 1000;
const ANALYTICS_GC_TIME_MS = 20 * 60 * 1000;

export function useCompetitionAnalytics(filters: CompetitionAnalyticsFilters) {
  const normalizedFilters = useMemo(
    () => ({
      competitionKey: filters.competitionKey ?? undefined,
      seasonLabel: filters.seasonLabel ?? undefined,
    }),
    [filters.competitionKey, filters.seasonLabel],
  );

  return useQueryWithCoverage<CompetitionAnalyticsData>({
    queryKey: competitionStructureQueryKeys.analytics(normalizedFilters),
    queryFn: () => fetchCompetitionAnalytics(normalizedFilters),
    enabled: Boolean(normalizedFilters.competitionKey && normalizedFilters.seasonLabel),
    staleTime: ANALYTICS_STALE_TIME_MS,
    gcTime: ANALYTICS_GC_TIME_MS,
    isDataEmpty: (data) => data.stageAnalytics.length === 0 && data.seasonComparisons.length === 0,
  });
}
