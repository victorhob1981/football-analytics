import { buildQueryKey } from "@/shared/utils/queryKeys";

import type { MarketTransfersFilters } from "@/features/market/types";

const MARKET_DOMAIN = "market";

export const marketQueryKeys = {
  all: () => buildQueryKey(MARKET_DOMAIN, "all"),
  transfers: (filters: MarketTransfersFilters) => buildQueryKey(MARKET_DOMAIN, "transfers", filters),
};
