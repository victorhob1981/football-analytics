import { useMemo } from "react";

import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { searchQueryKeys } from "@/features/search/queryKeys";
import { fetchGlobalSearch } from "@/features/search/services/search.service";
import type { GlobalSearchData, GlobalSearchFilters, SearchGroupType } from "@/features/search/types";

const DEFAULT_SEARCH_TYPES: SearchGroupType[] = ["competition", "team", "player", "match"];
const SEARCH_STALE_TIME_MS = 30 * 1000;
const SEARCH_GC_TIME_MS = 5 * 60 * 1000;

type UseGlobalSearchOptions = {
  enabled?: boolean;
  limitPerType?: number;
  types?: SearchGroupType[];
};

export function useGlobalSearch(
  query: string,
  { enabled = true, limitPerType = 5, types = DEFAULT_SEARCH_TYPES }: UseGlobalSearchOptions = {},
) {
  const { competitionId, seasonId } = useGlobalFiltersState();

  const filters = useMemo<GlobalSearchFilters>(() => {
    const normalizedQuery = query.trim();

    return {
      q: normalizedQuery,
      types,
      competitionId,
      seasonId,
      limitPerType,
    };
  }, [competitionId, limitPerType, query, seasonId, types]);

  const searchEnabled = enabled && filters.q.length >= 2;

  return useQueryWithCoverage<GlobalSearchData>({
    queryKey: searchQueryKeys.global(filters),
    queryFn: () => fetchGlobalSearch(filters),
    enabled: searchEnabled,
    staleTime: SEARCH_STALE_TIME_MS,
    gcTime: SEARCH_GC_TIME_MS,
    isDataEmpty: (data) => data.totalResults === 0,
  });
}
