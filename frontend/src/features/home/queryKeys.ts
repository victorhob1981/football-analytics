import { buildQueryKey } from "@/shared/utils/queryKeys";

import type { HomeGlobalFilters } from "@/features/home/types";

export const homeQueryKeys = {
  all: () => buildQueryKey("home", "all"),
  kpi: (filters: HomeGlobalFilters) => buildQueryKey("home", "kpi", filters),
  topTeams: (filters: HomeGlobalFilters) => buildQueryKey("home", "top-teams", filters),
  standingsEvolution: (filters: HomeGlobalFilters) =>
    buildQueryKey("home", "standings-evolution", filters),
  topPlayers: (filters: HomeGlobalFilters) => buildQueryKey("home", "top-players", filters),
  coverage: (filters: HomeGlobalFilters) => buildQueryKey("home", "coverage", filters),
} as const;
