import { beforeEach, describe, expect, it } from "vitest";

import { INITIAL_GLOBAL_FILTERS_STATE, useGlobalFiltersStore } from "@/shared/stores/globalFilters.store";

describe("globalFilters.store", () => {
  beforeEach(() => {
    useGlobalFiltersStore.setState({ ...INITIAL_GLOBAL_FILTERS_STATE });
  });

  it("aplica lastN e limpa dateRangeStart/dateRangeEnd", () => {
    useGlobalFiltersStore.getState().setTimeRange({
      mode: "dateRange",
      dateRangeStart: "2026-01-01",
      dateRangeEnd: "2026-01-31",
    });

    useGlobalFiltersStore.getState().setTimeRange({
      mode: "lastN",
      lastN: 5,
    });

    const state = useGlobalFiltersStore.getState();

    expect(state.lastN).toBe(5);
    expect(state.dateRangeStart).toBeNull();
    expect(state.dateRangeEnd).toBeNull();
  });

  it("aplica dateRange e limpa lastN", () => {
    useGlobalFiltersStore.getState().setTimeRange({
      mode: "lastN",
      lastN: 8,
    });

    useGlobalFiltersStore.getState().setTimeRange({
      mode: "dateRange",
      dateRangeStart: "2026-02-01",
      dateRangeEnd: "2026-02-20",
    });

    const state = useGlobalFiltersStore.getState();

    expect(state.lastN).toBeNull();
    expect(state.dateRangeStart).toBe("2026-02-01");
    expect(state.dateRangeEnd).toBe("2026-02-20");
  });

  it("mantem timeRange quando roundId muda", () => {
    useGlobalFiltersStore.getState().setTimeRange({
      mode: "lastN",
      lastN: 3,
    });

    useGlobalFiltersStore.getState().setRoundId("12");

    const state = useGlobalFiltersStore.getState();

    expect(state.roundId).toBe("12");
    expect(state.lastN).toBe(3);
    expect(state.dateRangeStart).toBeNull();
    expect(state.dateRangeEnd).toBeNull();
  });
});
