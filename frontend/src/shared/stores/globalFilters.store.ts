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
  dateRangeStart: null,
  dateRangeEnd: null,
  lastN: null,
  venue: "all",
};

function applyTimeRange(state: GlobalFiltersState, timeRange: TimeRangeInput): GlobalFiltersState {
  if (timeRange.mode === "lastN") {
    return {
      ...state,
      lastN: timeRange.lastN,
      dateRangeStart: null,
      dateRangeEnd: null,
    };
  }

  return {
    ...state,
    dateRangeStart: timeRange.dateRangeStart,
    dateRangeEnd: timeRange.dateRangeEnd,
    lastN: null,
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
    set((state) => ({
      ...state,
      roundId,
    }));
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
