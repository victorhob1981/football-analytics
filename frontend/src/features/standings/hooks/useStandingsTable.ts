import { useMemo } from "react";

import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { standingsQueryKeys } from "@/features/standings/queryKeys";
import { fetchStandings } from "@/features/standings/services/standings.service";
import type { StandingsFilters, StandingsTableData } from "@/features/standings/types";

const STANDINGS_STALE_TIME_MS = 5 * 60 * 1000;
const STANDINGS_GC_TIME_MS = 20 * 60 * 1000;

type UseStandingsTableOptions = {
  enabled?: boolean;
};

export function useStandingsTable(
  localFilters: Partial<StandingsFilters> = {},
  options: UseStandingsTableOptions = {},
) {
  const { competitionId, seasonId, roundId } = useGlobalFiltersState();

  const filters = useMemo<StandingsFilters>(
    () => ({
      competitionId,
      seasonId,
      roundId,
      stageId: localFilters.stageId ?? undefined,
      groupId: localFilters.groupId ?? undefined,
    }),
    [competitionId, localFilters.groupId, localFilters.stageId, roundId, seasonId],
  );

  return useQueryWithCoverage<StandingsTableData>({
    queryKey: standingsQueryKeys.table(filters),
    queryFn: () => fetchStandings(filters),
    enabled: options.enabled ?? (competitionId !== null && seasonId !== null),
    staleTime: STANDINGS_STALE_TIME_MS,
    gcTime: STANDINGS_GC_TIME_MS,
    isDataEmpty: (data) => data.rows.length === 0,
  });
}
