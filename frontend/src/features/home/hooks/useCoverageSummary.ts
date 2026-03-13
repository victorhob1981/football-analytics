import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { homeQueryKeys } from "@/features/home/queryKeys";
import { fetchCoverageSummary } from "@/features/home/services";
import type { CoverageSummaryData } from "@/features/home/types";
import { useHomeFilters } from "./useHomeFilters";

const STALE_TIME_MS = 5 * 60 * 1000;
const GC_TIME_MS = 10 * 60 * 1000;

export function useCoverageSummary() {
  const filters = useHomeFilters();

  return useQueryWithCoverage<CoverageSummaryData>({
    queryKey: homeQueryKeys.coverage(filters),
    queryFn: () => fetchCoverageSummary(filters),
    staleTime: STALE_TIME_MS,
    gcTime: GC_TIME_MS,
    isDataEmpty: (data) => data.modules.length === 0,
  });
}
