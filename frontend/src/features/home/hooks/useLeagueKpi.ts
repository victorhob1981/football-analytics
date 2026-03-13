import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { homeQueryKeys } from "@/features/home/queryKeys";
import { fetchLeagueKpi } from "@/features/home/services";
import type { LeagueKpiData } from "@/features/home/types";
import { useHomeFilters } from "./useHomeFilters";

const STALE_TIME_MS = 5 * 60 * 1000;
const GC_TIME_MS = 15 * 60 * 1000;

export function useLeagueKpi() {
  const filters = useHomeFilters();

  return useQueryWithCoverage<LeagueKpiData>({
    queryKey: homeQueryKeys.kpi(filters),
    queryFn: () => fetchLeagueKpi(filters),
    staleTime: STALE_TIME_MS,
    gcTime: GC_TIME_MS,
  });
}
