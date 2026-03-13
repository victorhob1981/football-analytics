import { useMemo } from "react";

import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";

import type { HomeGlobalFilters } from "@/features/home/types";

export function useHomeFilters(): HomeGlobalFilters {
  const { competitionId, seasonId, venue } = useGlobalFiltersState();
  const { params } = useTimeRange();

  return useMemo<HomeGlobalFilters>(
    () => ({
      competitionId,
      seasonId,
      venue,
      roundId: params.roundId,
      lastN: params.lastN,
      dateRangeStart: params.dateRangeStart,
      dateRangeEnd: params.dateRangeEnd,
    }),
    [competitionId, seasonId, venue, params],
  );
}
