import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";
import type { CompetitionSeasonContextFilters, CompetitionSeasonContextsData } from "@/shared/types/context.types";

import { playersQueryKeys } from "@/features/players/queryKeys";
import { fetchPlayerContexts } from "@/features/players/services/players.service";

const PLAYER_CONTEXTS_STALE_TIME_MS = 5 * 60 * 1000;
const PLAYER_CONTEXTS_GC_TIME_MS = 30 * 60 * 1000;

export function usePlayerContexts(
  playerId: string | null | undefined,
  filters: CompetitionSeasonContextFilters = {},
  enabled = true,
) {
  const normalizedPlayerId = playerId?.trim() ?? "";

  return useQueryWithCoverage<CompetitionSeasonContextsData>({
    queryKey: playersQueryKeys.contexts(normalizedPlayerId || "unknown", filters),
    queryFn: () => fetchPlayerContexts(normalizedPlayerId, filters),
    enabled: enabled && normalizedPlayerId.length > 0,
    staleTime: PLAYER_CONTEXTS_STALE_TIME_MS,
    gcTime: PLAYER_CONTEXTS_GC_TIME_MS,
    isDataEmpty: (data) => data.availableContexts.length === 0,
  });
}
