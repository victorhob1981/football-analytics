import { buildQueryKey } from "@/shared/utils/queryKeys";

import type { StandingsFilters } from "@/features/standings/types";

const STANDINGS_DOMAIN = "standings";

export const standingsQueryKeys = {
  all: () => buildQueryKey(STANDINGS_DOMAIN, "all"),
  table: (filters: StandingsFilters) => buildQueryKey(STANDINGS_DOMAIN, "table", filters),
  group: (filters: StandingsFilters) => buildQueryKey(STANDINGS_DOMAIN, "group", filters),
};
