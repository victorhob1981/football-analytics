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
    useGlobalFiltersStore.getState().setMonthKey("2026-01");

    useGlobalFiltersStore.getState().setTimeRange({
      mode: "lastN",
      lastN: 5,
    });

    const state = useGlobalFiltersStore.getState();

    expect(state.roundId).toBeNull();
    expect(state.monthKey).toBeNull();
    expect(state.lastN).toBe(5);
    expect(state.dateRangeStart).toBeNull();
    expect(state.dateRangeEnd).toBeNull();
  });

  it("aplica dateRange e limpa lastN", () => {
    useGlobalFiltersStore.getState().setTimeRange({
      mode: "lastN",
      lastN: 8,
    });
    useGlobalFiltersStore.getState().setRoundId("9");

    useGlobalFiltersStore.getState().setTimeRange({
      mode: "dateRange",
      dateRangeStart: "2026-02-01",
      dateRangeEnd: "2026-02-20",
    });

    const state = useGlobalFiltersStore.getState();

    expect(state.roundId).toBeNull();
    expect(state.monthKey).toBeNull();
    expect(state.lastN).toBeNull();
    expect(state.dateRangeStart).toBe("2026-02-01");
    expect(state.dateRangeEnd).toBe("2026-02-20");
  });

  it("setRoundId limpa os demais recortes temporais", () => {
    useGlobalFiltersStore.getState().setTimeRange({
      mode: "lastN",
      lastN: 3,
    });
    useGlobalFiltersStore.getState().setMonthKey("2026-03");

    useGlobalFiltersStore.getState().setRoundId("12");

    const state = useGlobalFiltersStore.getState();

    expect(state.roundId).toBe("12");
    expect(state.monthKey).toBeNull();
    expect(state.lastN).toBeNull();
    expect(state.dateRangeStart).toBeNull();
    expect(state.dateRangeEnd).toBeNull();
  });

  it("setMonthKey limpa roundId, lastN e dateRange", () => {
    useGlobalFiltersStore.getState().setRoundId("2");
    useGlobalFiltersStore.getState().setTimeRange({
      mode: "lastN",
      lastN: 5,
    });
    useGlobalFiltersStore.getState().setTimeRange({
      mode: "dateRange",
      dateRangeStart: "2026-04-01",
      dateRangeEnd: "2026-04-30",
    });

    useGlobalFiltersStore.getState().setMonthKey("2026-05");

    const state = useGlobalFiltersStore.getState();

    expect(state.roundId).toBeNull();
    expect(state.monthKey).toBe("2026-05");
    expect(state.lastN).toBeNull();
    expect(state.dateRangeStart).toBeNull();
    expect(state.dateRangeEnd).toBeNull();
  });

  it("setTimeRange modo none limpa todos os recortes temporais", () => {
    useGlobalFiltersStore.getState().setRoundId("17");
    useGlobalFiltersStore.getState().setTimeRange({
      mode: "none",
    });

    const state = useGlobalFiltersStore.getState();

    expect(state.roundId).toBeNull();
    expect(state.monthKey).toBeNull();
    expect(state.lastN).toBeNull();
    expect(state.dateRangeStart).toBeNull();
    expect(state.dateRangeEnd).toBeNull();
  });
});
