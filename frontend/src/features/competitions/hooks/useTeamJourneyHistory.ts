import { useMemo } from "react";

import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { competitionStructureQueryKeys } from "@/features/competitions/queryKeys";
import { fetchTeamJourneyHistory } from "@/features/competitions/services/competition-hub.service";
import type {
  TeamJourneyHistoryData,
  TeamJourneyHistoryFilters,
} from "@/features/competitions/types/competition-structure.types";

const TEAM_JOURNEY_STALE_TIME_MS = 5 * 60 * 1000;
const TEAM_JOURNEY_GC_TIME_MS = 20 * 60 * 1000;

export function useTeamJourneyHistory(filters: TeamJourneyHistoryFilters) {
  const normalizedFilters = useMemo(
    () => ({
      competitionKey: filters.competitionKey ?? undefined,
      teamId: filters.teamId ?? undefined,
    }),
    [filters.competitionKey, filters.teamId],
  );

  return useQueryWithCoverage<TeamJourneyHistoryData>({
    queryKey: competitionStructureQueryKeys.teamJourney(normalizedFilters),
    queryFn: () => fetchTeamJourneyHistory(normalizedFilters),
    enabled: Boolean(normalizedFilters.competitionKey && normalizedFilters.teamId),
    staleTime: TEAM_JOURNEY_STALE_TIME_MS,
    gcTime: TEAM_JOURNEY_GC_TIME_MS,
    isDataEmpty: (data) => data.seasons.length === 0,
  });
}
