import { useMemo } from "react";

import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";

export type ActiveTimeRangeMode = "none" | "lastN" | "dateRange";

export interface NormalizedTimeRangeParams {
  roundId: string | null;
  lastN: number | null;
  dateRangeStart: string | null;
  dateRangeEnd: string | null;
}

export interface TimeRangeState {
  activeMode: ActiveTimeRangeMode;
  hasTimeRange: boolean;
  params: NormalizedTimeRangeParams;
}

export function useTimeRange(): TimeRangeState {
  const { roundId, lastN, dateRangeStart, dateRangeEnd } = useGlobalFiltersState();

  return useMemo(() => {
    if (lastN !== null) {
      return {
        activeMode: "lastN" as const,
        hasTimeRange: true,
        params: {
          roundId,
          lastN,
          dateRangeStart: null,
          dateRangeEnd: null,
        },
      };
    }

    if (dateRangeStart !== null || dateRangeEnd !== null) {
      return {
        activeMode: "dateRange" as const,
        hasTimeRange: true,
        params: {
          roundId,
          lastN: null,
          dateRangeStart,
          dateRangeEnd,
        },
      };
    }

    return {
      activeMode: "none" as const,
      hasTimeRange: false,
      params: {
        roundId,
        lastN: null,
        dateRangeStart: null,
        dateRangeEnd: null,
      },
    };
  }, [dateRangeEnd, dateRangeStart, lastN, roundId]);
}
