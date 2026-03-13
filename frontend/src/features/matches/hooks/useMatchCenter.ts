import { useMemo } from "react";

import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { matchesQueryKeys } from "@/features/matches/queryKeys";
import { fetchMatchCenter } from "@/features/matches/services/matches.service";
import type { MatchCenterData, MatchCenterFilters, MatchCenterLocalFilters } from "@/features/matches/types";

const MATCH_CENTER_STALE_TIME_MS = 5 * 60 * 1000;
const MATCH_CENTER_GC_TIME_MS = 20 * 60 * 1000;

export function useMatchCenter(matchId: string | null | undefined, localFilters: MatchCenterLocalFilters = {}) {
  const normalizedMatchId = matchId?.trim() ?? "";

  const mergedFilters = useMemo<MatchCenterFilters>(
    () => ({
      includeTeamStats: localFilters.includeTeamStats,
      includeTimeline: localFilters.includeTimeline,
      includeLineups: localFilters.includeLineups,
      includePlayerStats: localFilters.includePlayerStats,
    }),
    [
      localFilters.includeTeamStats,
      localFilters.includeLineups,
      localFilters.includePlayerStats,
      localFilters.includeTimeline,
    ],
  );

  return useQueryWithCoverage<MatchCenterData>({
    queryKey: matchesQueryKeys.center(normalizedMatchId || "unknown", mergedFilters),
    queryFn: () => fetchMatchCenter(normalizedMatchId, mergedFilters),
    enabled: normalizedMatchId.length > 0,
    staleTime: MATCH_CENTER_STALE_TIME_MS,
    gcTime: MATCH_CENTER_GC_TIME_MS,
    isDataEmpty: (data) => {
      const responseMatchId = typeof data.match?.matchId === "string" ? data.match.matchId.trim() : "";
      return responseMatchId.length === 0;
    },
  });
}
