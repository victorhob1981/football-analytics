import { buildQueryKey } from "@/shared/utils/queryKeys";

import type { RankingQueryFilters } from "@/features/rankings/types";

const RANKINGS_DOMAIN = "rankings";

export const rankingsQueryKeys = {
  all: () => buildQueryKey(RANKINGS_DOMAIN, "all"),
  table: (rankingId: string, filters: RankingQueryFilters) => buildQueryKey(RANKINGS_DOMAIN, "table", rankingId, filters),
};
