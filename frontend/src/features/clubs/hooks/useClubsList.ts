import { useMemo } from "react";

import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { clubsQueryKeys } from "@/features/clubs/queryKeys";
import { fetchClubsList } from "@/features/clubs/services/clubs.service";
import type { ClubsListData, ClubsListFilters, ClubsListLocalFilters } from "@/features/clubs/types";

const CLUBS_LIST_STALE_TIME_MS = 5 * 60 * 1000;
const CLUBS_LIST_GC_TIME_MS = 20 * 60 * 1000;

export function useClubsList(localFilters: ClubsListLocalFilters = {}) {
  const { competitionId, seasonId, venue } = useGlobalFiltersState();
  const { params: timeRangeParams } = useTimeRange();

  const mergedFilters = useMemo<ClubsListFilters>(() => {
    const search = localFilters.search?.trim();

    return {
      competitionId,
      seasonId,
      roundId: timeRangeParams.roundId,
      monthKey: timeRangeParams.monthKey,
      venue,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
      search: search && search.length > 0 ? search : undefined,
      orderBy: localFilters.orderBy,
      page: localFilters.page,
      pageSize: localFilters.pageSize,
    };
  }, [
    competitionId,
    localFilters.orderBy,
    localFilters.page,
    localFilters.pageSize,
    localFilters.search,
    seasonId,
    timeRangeParams.dateRangeEnd,
    timeRangeParams.dateRangeStart,
    timeRangeParams.lastN,
    timeRangeParams.monthKey,
    timeRangeParams.roundId,
    venue,
  ]);

  return useQueryWithCoverage<ClubsListData>({
    queryKey: clubsQueryKeys.list(mergedFilters),
    queryFn: () => fetchClubsList(mergedFilters),
    staleTime: CLUBS_LIST_STALE_TIME_MS,
    gcTime: CLUBS_LIST_GC_TIME_MS,
    isDataEmpty: (data) => !Array.isArray(data.items) || data.items.length === 0,
  });
}
