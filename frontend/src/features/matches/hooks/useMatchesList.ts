import { useMemo } from "react";

import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { matchesQueryKeys } from "@/features/matches/queryKeys";
import { fetchMatchesList } from "@/features/matches/services/matches.service";
import type { MatchesListData, MatchesListFilters, MatchesListLocalFilters } from "@/features/matches/types";

const MATCHES_LIST_STALE_TIME_MS = 5 * 60 * 1000;
const MATCHES_LIST_GC_TIME_MS = 20 * 60 * 1000;

export function useMatchesList(localFilters: MatchesListLocalFilters = {}) {
  const { competitionId, seasonId, venue } = useGlobalFiltersState();
  const { params: timeRangeParams } = useTimeRange();

  const mergedFilters = useMemo<MatchesListFilters>(() => {
    const search = localFilters.search?.trim();
    const status = localFilters.status?.trim();

    return {
      competitionId,
      seasonId,
      roundId: timeRangeParams.roundId,
      venue,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
      search: search && search.length > 0 ? search : undefined,
      status: status && status.length > 0 ? status : undefined,
      page: localFilters.page,
      pageSize: localFilters.pageSize,
      sortBy: localFilters.sortBy,
      sortDirection: localFilters.sortDirection,
    };
  }, [
    competitionId,
    localFilters.page,
    localFilters.pageSize,
    localFilters.search,
    localFilters.sortBy,
    localFilters.sortDirection,
    localFilters.status,
    seasonId,
    timeRangeParams.dateRangeEnd,
    timeRangeParams.dateRangeStart,
    timeRangeParams.lastN,
    timeRangeParams.roundId,
    venue,
  ]);

  return useQueryWithCoverage<MatchesListData>({
    queryKey: matchesQueryKeys.list(mergedFilters),
    queryFn: () => fetchMatchesList(mergedFilters),
    staleTime: MATCHES_LIST_STALE_TIME_MS,
    gcTime: MATCHES_LIST_GC_TIME_MS,
    isDataEmpty: (data) => data.items.length === 0,
  });
}
