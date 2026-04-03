import { useMemo } from "react";

import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";
import type {
  TeamMatchesListData,
  TeamMatchesListFilters,
  TeamMatchesListLocalFilters,
} from "@/features/teams/types";
import { teamsQueryKeys } from "@/features/teams/queryKeys";
import { fetchTeamMatchesList } from "@/features/teams/services/team-matches.service";

const TEAM_MATCHES_STALE_TIME_MS = 5 * 60 * 1000;
const TEAM_MATCHES_GC_TIME_MS = 20 * 60 * 1000;

export function useTeamMatches(
  teamId: string | null | undefined,
  contextOverride: CompetitionSeasonContext | null,
  localFilters: TeamMatchesListLocalFilters = {},
) {
  const {
    competitionId: globalCompetitionId,
    seasonId: globalSeasonId,
    venue,
  } = useGlobalFiltersState();
  const { params: timeRangeParams } = useTimeRange();
  const normalizedTeamId = teamId?.trim() ?? "";
  const competitionId = contextOverride?.competitionId ?? globalCompetitionId;
  const seasonId = contextOverride?.seasonId ?? globalSeasonId;

  const mergedFilters = useMemo<TeamMatchesListFilters>(() => {
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
      teamId: normalizedTeamId || undefined,
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
    normalizedTeamId,
    seasonId,
    timeRangeParams.dateRangeEnd,
    timeRangeParams.dateRangeStart,
    timeRangeParams.lastN,
    timeRangeParams.roundId,
    venue,
  ]);

  return useQueryWithCoverage<TeamMatchesListData>({
    queryKey: teamsQueryKeys.matches(normalizedTeamId, mergedFilters),
    queryFn: () => fetchTeamMatchesList(mergedFilters),
    enabled: normalizedTeamId.length > 0 && Boolean(competitionId && seasonId),
    staleTime: TEAM_MATCHES_STALE_TIME_MS,
    gcTime: TEAM_MATCHES_GC_TIME_MS,
    isDataEmpty: (data) => data.items.length === 0,
  });
}
