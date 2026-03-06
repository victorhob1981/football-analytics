import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { homeQueryKeys } from "@/features/home/queryKeys";
import { fetchTopPlayers } from "@/features/home/services";
import type { TopPlayersData } from "@/features/home/types";
import { useHomeFilters } from "./useHomeFilters";

const STALE_TIME_MS = 5 * 60 * 1000;
const GC_TIME_MS = 15 * 60 * 1000;

export function useTopPlayers() {
  const filters = useHomeFilters();

  return useQueryWithCoverage<TopPlayersData>({
    queryKey: homeQueryKeys.topPlayers(filters),
    queryFn: () => fetchTopPlayers(filters),
    staleTime: STALE_TIME_MS,
    gcTime: GC_TIME_MS,
    isDataEmpty: (data) => data.items.length === 0,
  });
}
