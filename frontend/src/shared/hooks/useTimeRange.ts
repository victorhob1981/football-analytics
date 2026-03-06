import { useMemo } from "react";

import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";

export type ActiveTimeRangeMode = "none" | "round" | "month" | "lastN" | "dateRange";

export interface NormalizedTimeRangeParams {
  roundId: string | null;
  monthKey: string | null;
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
  const { roundId, monthKey, lastN, dateRangeStart, dateRangeEnd } = useGlobalFiltersState();

  return useMemo(() => {
    if (roundId !== null) {
      return {
        activeMode: "round" as const,
        hasTimeRange: true,
        params: {
          roundId,
          monthKey: null,
          lastN: null,
          dateRangeStart: null,
          dateRangeEnd: null,
        },
      };
    }

    if (monthKey !== null) {
      return {
        activeMode: "month" as const,
        hasTimeRange: true,
        params: {
          roundId: null,
          monthKey,
          lastN: null,
          dateRangeStart: null,
          dateRangeEnd: null,
        },
      };
    }

    if (lastN !== null) {
      return {
        activeMode: "lastN" as const,
        hasTimeRange: true,
        params: {
          roundId: null,
          monthKey: null,
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
          roundId: null,
          monthKey: null,
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
        roundId: null,
        monthKey: null,
        lastN: null,
        dateRangeStart: null,
        dateRangeEnd: null,
      },
    };
  }, [dateRangeEnd, dateRangeStart, lastN, monthKey, roundId]);
}
