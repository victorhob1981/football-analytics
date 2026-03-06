import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { homeQueryKeys } from "@/features/home/queryKeys";
import { fetchTopTeams } from "@/features/home/services";
import type { TopTeamsData } from "@/features/home/types";
import { useHomeFilters } from "./useHomeFilters";

const STALE_TIME_MS = 5 * 60 * 1000;
const GC_TIME_MS = 15 * 60 * 1000;

export function useTopTeams() {
  const filters = useHomeFilters();

  return useQueryWithCoverage<TopTeamsData>({
    queryKey: homeQueryKeys.topTeams(filters),
    queryFn: () => fetchTopTeams(filters),
    staleTime: STALE_TIME_MS,
    gcTime: GC_TIME_MS,
  });
}
