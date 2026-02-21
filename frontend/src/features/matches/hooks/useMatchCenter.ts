import { useMemo } from "react";

import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { matchesQueryKeys } from "@/features/matches/queryKeys";
import { fetchMatchCenter } from "@/features/matches/services/matches.service";
import type { MatchCenterData, MatchCenterFilters, MatchCenterLocalFilters } from "@/features/matches/types";

const MATCH_CENTER_STALE_TIME_MS = 5 * 60 * 1000;
const MATCH_CENTER_GC_TIME_MS = 20 * 60 * 1000;

export function useMatchCenter(matchId: string | null | undefined, localFilters: MatchCenterLocalFilters = {}) {
  const { competitionId, seasonId, venue } = useGlobalFiltersState();
  const { params: timeRangeParams } = useTimeRange();
  const normalizedMatchId = matchId?.trim() ?? "";

  const mergedFilters = useMemo<MatchCenterFilters>(
    () => ({
      competitionId,
      seasonId,
      roundId: timeRangeParams.roundId,
      venue,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
      includeTimeline: localFilters.includeTimeline,
      includeLineups: localFilters.includeLineups,
      includePlayerStats: localFilters.includePlayerStats,
    }),
    [
      competitionId,
      localFilters.includeLineups,
      localFilters.includePlayerStats,
      localFilters.includeTimeline,
      seasonId,
      timeRangeParams.dateRangeEnd,
      timeRangeParams.dateRangeStart,
      timeRangeParams.lastN,
      timeRangeParams.roundId,
      venue,
    ],
  );

  return useQueryWithCoverage<MatchCenterData>({
    queryKey: matchesQueryKeys.center(normalizedMatchId || "unknown", mergedFilters),
    queryFn: () => fetchMatchCenter(normalizedMatchId, mergedFilters),
    enabled: normalizedMatchId.length > 0,
    staleTime: MATCH_CENTER_STALE_TIME_MS,
    gcTime: MATCH_CENTER_GC_TIME_MS,
    isDataEmpty: (data) => data.match.matchId.trim().length === 0,
  });
}
