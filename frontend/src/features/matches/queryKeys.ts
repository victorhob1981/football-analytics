import { buildQueryKey } from "@/shared/utils/queryKeys";

import type { MatchCenterFilters, MatchesListFilters } from "@/features/matches/types";

const MATCHES_DOMAIN = "matches";

export const matchesQueryKeys = {
  all: () => buildQueryKey(MATCHES_DOMAIN, "all"),
  list: (filters: MatchesListFilters) => buildQueryKey(MATCHES_DOMAIN, "list", filters),
  center: (matchId: string, filters: MatchCenterFilters) => buildQueryKey(MATCHES_DOMAIN, "center", matchId, filters),
};
