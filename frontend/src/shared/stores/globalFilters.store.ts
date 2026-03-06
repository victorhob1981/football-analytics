import { create } from "zustand";

import type {
  GlobalFiltersState,
  GlobalFiltersStore,
  TimeRangeInput,
  VenueFilter,
} from "@/shared/types/filters.types";

const INITIAL_GLOBAL_FILTERS_STATE: GlobalFiltersState = {
  competitionId: null,
  seasonId: null,
  roundId: null,
  monthKey: null,
  dateRangeStart: null,
  dateRangeEnd: null,
  lastN: null,
  venue: "all",
};

function clearTemporalFilters(state: GlobalFiltersState): GlobalFiltersState {
  return {
    ...state,
    roundId: null,
    monthKey: null,
    dateRangeStart: null,
    dateRangeEnd: null,
    lastN: null,
  };
}

function applyTimeRange(state: GlobalFiltersState, timeRange: TimeRangeInput): GlobalFiltersState {
  if (timeRange.mode === "none") {
    return clearTemporalFilters(state);
  }

  if (timeRange.mode === "round") {
    return {
      ...clearTemporalFilters(state),
      roundId: timeRange.roundId,
    };
  }

  if (timeRange.mode === "month") {
    return {
      ...clearTemporalFilters(state),
      monthKey: timeRange.monthKey,
    };
  }

  if (timeRange.mode === "lastN") {
    return {
      ...clearTemporalFilters(state),
      lastN: timeRange.lastN,
    };
  }

  return {
    ...clearTemporalFilters(state),
    dateRangeStart: timeRange.dateRangeStart,
    dateRangeEnd: timeRange.dateRangeEnd,
  };
}

function setVenueValue(state: GlobalFiltersState, venue: VenueFilter): GlobalFiltersState {
  return {
    ...state,
    venue,
  };
}

export const useGlobalFiltersStore = create<GlobalFiltersStore>((set) => ({
  ...INITIAL_GLOBAL_FILTERS_STATE,
  setCompetitionId: (competitionId) => {
    set((state) => ({
      ...state,
      competitionId,
    }));
  },
  setSeasonId: (seasonId) => {
    set((state) => ({
      ...state,
      seasonId,
    }));
  },
  setRoundId: (roundId) => {
    set((state) =>
      applyTimeRange(state, {
        mode: "round",
        roundId,
      }),
    );
  },
  setMonthKey: (monthKey) => {
    set((state) =>
      applyTimeRange(state, {
        mode: "month",
        monthKey,
      }),
    );
  },
  setVenue: (venue) => {
    set((state) => setVenueValue(state, venue));
  },
  setTimeRange: (timeRange) => {
    set((state) => applyTimeRange(state, timeRange));
  },
  reset: () => {
    set(() => ({
      ...INITIAL_GLOBAL_FILTERS_STATE,
    }));
  },
}));

export { INITIAL_GLOBAL_FILTERS_STATE };
