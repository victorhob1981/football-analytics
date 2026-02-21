import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it } from "vitest";

import { useTimeRange } from "@/shared/hooks/useTimeRange";
import { INITIAL_GLOBAL_FILTERS_STATE, useGlobalFiltersStore } from "@/shared/stores/globalFilters.store";

describe("useTimeRange", () => {
  beforeEach(() => {
    useGlobalFiltersStore.setState({ ...INITIAL_GLOBAL_FILTERS_STATE });
  });

  it("retorna modo none no estado inicial", () => {
    const { result } = renderHook(() => useTimeRange());

    expect(result.current.activeMode).toBe("none");
    expect(result.current.hasTimeRange).toBe(false);
    expect(result.current.params).toEqual({
      roundId: null,
      lastN: null,
      dateRangeStart: null,
      dateRangeEnd: null,
    });
  });

  it("retorna modo lastN com dateRange limpo", () => {
    const { result } = renderHook(() => useTimeRange());

    act(() => {
      useGlobalFiltersStore.getState().setTimeRange({
        mode: "lastN",
        lastN: 6,
      });
      useGlobalFiltersStore.getState().setRoundId("8");
    });

    expect(result.current.activeMode).toBe("lastN");
    expect(result.current.hasTimeRange).toBe(true);
    expect(result.current.params).toEqual({
      roundId: "8",
      lastN: 6,
      dateRangeStart: null,
      dateRangeEnd: null,
    });
  });

  it("retorna modo dateRange e zera lastN", () => {
    const { result } = renderHook(() => useTimeRange());

    act(() => {
      useGlobalFiltersStore.getState().setTimeRange({
        mode: "lastN",
        lastN: 4,
      });
      useGlobalFiltersStore.getState().setTimeRange({
        mode: "dateRange",
        dateRangeStart: "2026-02-01",
        dateRangeEnd: "2026-02-21",
      });
    });

    expect(result.current.activeMode).toBe("dateRange");
    expect(result.current.hasTimeRange).toBe(true);
    expect(result.current.params).toEqual({
      roundId: null,
      lastN: null,
      dateRangeStart: "2026-02-01",
      dateRangeEnd: "2026-02-21",
    });
  });
});
