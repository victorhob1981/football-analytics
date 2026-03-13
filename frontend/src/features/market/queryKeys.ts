import { buildQueryKey } from "@/shared/utils/queryKeys";
import type { MarketFilters } from "@/features/market/types";

const MARKET_DOMAIN = "market";

export const marketQueryKeys = {
    all: () => buildQueryKey(MARKET_DOMAIN, "all"),
    transfers: (filters: MarketFilters) => buildQueryKey(MARKET_DOMAIN, "transfers", filters),
    sidelined: (filters: MarketFilters) => buildQueryKey(MARKET_DOMAIN, "sidelined", filters),
};
