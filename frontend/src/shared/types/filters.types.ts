export type VenueFilter = "home" | "away" | "all";

export interface GlobalFiltersState {
  competitionId: string | null;
  seasonId: string | null;
  roundId: string | null;
  dateRangeStart: string | null;
  dateRangeEnd: string | null;
  lastN: number | null;
  venue: VenueFilter;
}

export type TimeRangeInput =
  | {
      mode: "lastN";
      lastN: number | null;
    }
  | {
      mode: "dateRange";
      dateRangeStart: string | null;
      dateRangeEnd: string | null;
    };

export interface GlobalFiltersActions {
  setCompetitionId: (competitionId: string | null) => void;
  setSeasonId: (seasonId: string | null) => void;
  setRoundId: (roundId: string | null) => void;
  setVenue: (venue: VenueFilter) => void;
  setTimeRange: (timeRange: TimeRangeInput) => void;
  reset: () => void;
}

export type GlobalFiltersStore = GlobalFiltersState & GlobalFiltersActions;
