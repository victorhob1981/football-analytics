import { useMemo } from "react";

import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { competitionStructureQueryKeys } from "@/features/competitions/queryKeys";
import { fetchStageTies } from "@/features/competitions/services/competition-hub.service";
import type {
  StageTiesData,
  StageTiesFilters,
} from "@/features/competitions/types/competition-structure.types";

const TIES_STALE_TIME_MS = 5 * 60 * 1000;
const TIES_GC_TIME_MS = 20 * 60 * 1000;

export function useStageTies(filters: StageTiesFilters) {
  const normalizedFilters = useMemo(
    () => ({
      competitionKey: filters.competitionKey ?? undefined,
      seasonLabel: filters.seasonLabel ?? undefined,
      stageId: filters.stageId ?? undefined,
    }),
    [filters.competitionKey, filters.seasonLabel, filters.stageId],
  );

  return useQueryWithCoverage<StageTiesData>({
    queryKey: competitionStructureQueryKeys.ties(normalizedFilters),
    queryFn: () => fetchStageTies(normalizedFilters),
    enabled: Boolean(
      normalizedFilters.competitionKey &&
        normalizedFilters.seasonLabel &&
        normalizedFilters.stageId,
    ),
    staleTime: TIES_STALE_TIME_MS,
    gcTime: TIES_GC_TIME_MS,
    isDataEmpty: (data) => data.ties.length === 0,
  });
}
