import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { homeQueryKeys } from "@/features/home/queryKeys";
import { fetchStandingsEvolution } from "@/features/home/services";
import type { StandingsEvolutionData } from "@/features/home/types";
import { useHomeFilters } from "./useHomeFilters";

const STALE_TIME_MS = 10 * 60 * 1000;
const GC_TIME_MS = 20 * 60 * 1000;

export function useStandingsEvolution() {
  const filters = useHomeFilters();

  return useQueryWithCoverage<StandingsEvolutionData>({
    queryKey: homeQueryKeys.standingsEvolution(filters),
    queryFn: () => fetchStandingsEvolution(filters),
    staleTime: STALE_TIME_MS,
    gcTime: GC_TIME_MS,
    isDataEmpty: (data) => data.series.length === 0,
  });
}
