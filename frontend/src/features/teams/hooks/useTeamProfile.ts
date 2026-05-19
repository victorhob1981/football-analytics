import { useMemo } from "react";

import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";

import { teamsQueryKeys } from "@/features/teams/queryKeys";
import { fetchTeamProfile } from "@/features/teams/services/teams.service";
import type { TeamProfile, TeamProfileFilters, TeamProfileLocalFilters } from "@/features/teams/types";

const TEAM_PROFILE_STALE_TIME_MS = 5 * 60 * 1000;
const TEAM_PROFILE_GC_TIME_MS = 30 * 60 * 1000;

export function useTeamProfile(
  teamId: string | null | undefined,
  localFilters: TeamProfileLocalFilters = {},
  contextOverride: CompetitionSeasonContext | null = null,
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

  const mergedFilters = useMemo<TeamProfileFilters>(
    () => ({
      competitionId,
      seasonId,
      roundId: timeRangeParams.roundId,
      venue,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
      includeRecentMatches: localFilters.includeRecentMatches,
      includeSquad: localFilters.includeSquad,
      includeStats: localFilters.includeStats,
    }),
    [
      competitionId,
      localFilters.includeRecentMatches,
      localFilters.includeSquad,
      localFilters.includeStats,
      seasonId,
      timeRangeParams.dateRangeEnd,
      timeRangeParams.dateRangeStart,
      timeRangeParams.lastN,
      timeRangeParams.roundId,
      venue,
    ],
  );

  return useQueryWithCoverage<TeamProfile>({
    queryKey: teamsQueryKeys.profile(normalizedTeamId || "unknown", mergedFilters),
    queryFn: () => fetchTeamProfile(normalizedTeamId, mergedFilters),
    enabled: normalizedTeamId.length > 0 && Boolean(competitionId && seasonId),
    staleTime: TEAM_PROFILE_STALE_TIME_MS,
    gcTime: TEAM_PROFILE_GC_TIME_MS,
    isDataEmpty: (data) => data.team.teamId.trim().length === 0,
  });
}
