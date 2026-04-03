import { useMemo } from "react";

import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { standingsQueryKeys } from "@/features/standings/queryKeys";
import { fetchGroupStandings } from "@/features/standings/services/standings.service";
import type { StandingsFilters, StandingsTableData } from "@/features/standings/types";

const STANDINGS_STALE_TIME_MS = 5 * 60 * 1000;
const STANDINGS_GC_TIME_MS = 20 * 60 * 1000;

type GroupStandingsFilters = {
  competitionKey?: string | null;
  seasonLabel?: string | null;
  stageId?: string | null;
  groupId?: string | null;
};

export function useGroupStandingsTable(filters: GroupStandingsFilters) {
  const { roundId } = useGlobalFiltersState();

  const mergedFilters = useMemo<StandingsFilters>(
    () => ({
      competitionKey: filters.competitionKey ?? undefined,
      seasonLabel: filters.seasonLabel ?? undefined,
      stageId: filters.stageId ?? undefined,
      groupId: filters.groupId ?? undefined,
      roundId,
    }),
    [filters.competitionKey, filters.groupId, filters.seasonLabel, filters.stageId, roundId],
  );

  return useQueryWithCoverage<StandingsTableData>({
    queryKey: standingsQueryKeys.group(mergedFilters),
    queryFn: () => fetchGroupStandings(mergedFilters),
    enabled: Boolean(
      mergedFilters.competitionKey &&
        mergedFilters.seasonLabel &&
        mergedFilters.stageId &&
        mergedFilters.groupId,
    ),
    staleTime: STANDINGS_STALE_TIME_MS,
    gcTime: STANDINGS_GC_TIME_MS,
    isDataEmpty: (data) => data.rows.length === 0,
  });
}
