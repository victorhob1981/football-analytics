import { buildQueryKey } from "@/shared/utils/queryKeys";
import type { H2HFilters } from "@/features/head-to-head/types";

const H2H_DOMAIN = "head-to-head";

export const h2hQueryKeys = {
    all: () => buildQueryKey(H2H_DOMAIN, "all"),
    summary: (filters: H2HFilters) => buildQueryKey(H2H_DOMAIN, "summary", filters),
    fixtures: (filters: H2HFilters) => buildQueryKey(H2H_DOMAIN, "fixtures", filters),
};
