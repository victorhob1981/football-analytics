import { buildQueryKey } from "@/shared/utils/queryKeys";

import type { GlobalSearchFilters } from "@/features/search/types";

const SEARCH_DOMAIN = "search";

export const searchQueryKeys = {
  all: () => buildQueryKey(SEARCH_DOMAIN, "all"),
  global: (filters: GlobalSearchFilters) => buildQueryKey(SEARCH_DOMAIN, "global", filters),
};
