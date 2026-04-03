import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";
import type { CompetitionSeasonContextFilters, CompetitionSeasonContextsData } from "@/shared/types/context.types";

import { teamsQueryKeys } from "@/features/teams/queryKeys";
import { fetchTeamContexts } from "@/features/teams/services/teams.service";

const TEAM_CONTEXTS_STALE_TIME_MS = 5 * 60 * 1000;
const TEAM_CONTEXTS_GC_TIME_MS = 30 * 60 * 1000;

export function useTeamContexts(
  teamId: string | null | undefined,
  filters: CompetitionSeasonContextFilters = {},
  enabled = true,
) {
  const normalizedTeamId = teamId?.trim() ?? "";

  return useQueryWithCoverage<CompetitionSeasonContextsData>({
    queryKey: teamsQueryKeys.contexts(normalizedTeamId || "unknown", filters),
    queryFn: () => fetchTeamContexts(normalizedTeamId, filters),
    enabled: enabled && normalizedTeamId.length > 0,
    staleTime: TEAM_CONTEXTS_STALE_TIME_MS,
    gcTime: TEAM_CONTEXTS_GC_TIME_MS,
    isDataEmpty: (data) => data.availableContexts.length === 0,
  });
}
