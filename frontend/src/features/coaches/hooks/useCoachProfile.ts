import { useMemo } from "react";

import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";

import { coachesQueryKeys } from "@/features/coaches/queryKeys";
import { fetchCoachProfile } from "@/features/coaches/services/coaches.service";
import type { CoachProfile, CoachProfileFilters, CoachProfileLocalFilters } from "@/features/coaches/types";

const COACH_PROFILE_STALE_TIME_MS = 5 * 60 * 1000;
const COACH_PROFILE_GC_TIME_MS = 30 * 60 * 1000;

export function useCoachProfile(
  coachId: string | null | undefined,
  localFilters: CoachProfileLocalFilters = {},
  contextOverride: CompetitionSeasonContext | null = null,
) {
  const { competitionId: globalCompetitionId, seasonId: globalSeasonId, venue } = useGlobalFiltersState();
  const { params: timeRangeParams } = useTimeRange();
  const normalizedCoachId = coachId?.trim() ?? "";
  const competitionId = contextOverride?.competitionId ?? globalCompetitionId;
  const seasonId = contextOverride?.seasonId ?? globalSeasonId;

  const mergedFilters = useMemo<CoachProfileFilters>(
    () => ({
      competitionId,
      seasonId,
      roundId: timeRangeParams.roundId,
      venue,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
      ...localFilters,
    }),
    [
      competitionId,
      localFilters,
      seasonId,
      timeRangeParams.dateRangeEnd,
      timeRangeParams.dateRangeStart,
      timeRangeParams.lastN,
      timeRangeParams.roundId,
      venue,
    ],
  );

  return useQueryWithCoverage<CoachProfile>({
    queryKey: coachesQueryKeys.profile(normalizedCoachId || "unknown", mergedFilters),
    queryFn: () => fetchCoachProfile(normalizedCoachId, mergedFilters),
    enabled: normalizedCoachId.length > 0,
    staleTime: COACH_PROFILE_STALE_TIME_MS,
    gcTime: COACH_PROFILE_GC_TIME_MS,
    isDataEmpty: (data) => data.coach.coachId.trim().length === 0,
  });
}
