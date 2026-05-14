import { useMemo } from "react";

import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import { fetchInsights } from "@/shared/services/insights.service";
import type { InsightEntityType, InsightFilters, InsightObject } from "@/shared/types/insight.types";
import { buildQueryKey } from "@/shared/utils/queryKeys";

const INSIGHTS_STALE_TIME_MS = 5 * 60 * 1000;
const INSIGHTS_GC_TIME_MS = 15 * 60 * 1000;

type UseInsightsInput = {
  entityType: InsightEntityType;
  entityId?: string | null;
  filters?: InsightFilters;
  enabled?: boolean;
};

function normalizeEntityId(entityId: string | null | undefined): string | null {
  const normalizedEntityId = entityId?.trim();
  return normalizedEntityId && normalizedEntityId.length > 0 ? normalizedEntityId : null;
}

export function useInsights({ entityType, entityId, filters, enabled = true }: UseInsightsInput) {
  const { competitionId, seasonId, venue } = useGlobalFiltersState();
  const { params: timeRangeParams } = useTimeRange();

  const mergedFilters = useMemo<InsightFilters>(
    () => ({
      competitionId,
      seasonId,
      roundId: timeRangeParams.roundId,
      venue,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
      ...filters,
    }),
    [
      competitionId,
      filters,
      seasonId,
      timeRangeParams.dateRangeEnd,
      timeRangeParams.dateRangeStart,
      timeRangeParams.lastN,
      timeRangeParams.roundId,
      venue,
    ],
  );

  const normalizedEntityId = normalizeEntityId(entityId);

  return useQueryWithCoverage<InsightObject[]>({
    queryKey: buildQueryKey("insights", "list", {
      entityType,
      entityId: normalizedEntityId,
      ...mergedFilters,
    }),
    queryFn: () =>
      fetchInsights({
        entityType,
        entityId: normalizedEntityId,
        filters: mergedFilters,
      }),
    enabled,
    staleTime: INSIGHTS_STALE_TIME_MS,
    gcTime: INSIGHTS_GC_TIME_MS,
    isDataEmpty: (data) => data.length === 0,
  });
}
