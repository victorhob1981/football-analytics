import { useShallow } from "zustand/react/shallow";

import { useGlobalFiltersStore } from "@/shared/stores/globalFilters.store";
import type {
  GlobalFiltersActions,
  GlobalFiltersState,
  GlobalFiltersStore,
} from "@/shared/types/filters.types";

const selectGlobalFiltersState = (state: GlobalFiltersStore): GlobalFiltersState => ({
  competitionId: state.competitionId,
  seasonId: state.seasonId,
  roundId: state.roundId,
  dateRangeStart: state.dateRangeStart,
  dateRangeEnd: state.dateRangeEnd,
  lastN: state.lastN,
  venue: state.venue,
});

const selectGlobalFiltersActions = (state: GlobalFiltersStore): GlobalFiltersActions => ({
  setCompetitionId: state.setCompetitionId,
  setSeasonId: state.setSeasonId,
  setRoundId: state.setRoundId,
  setVenue: state.setVenue,
  setTimeRange: state.setTimeRange,
  reset: state.reset,
});

export function useGlobalFilters(): GlobalFiltersState & GlobalFiltersActions {
  return useGlobalFiltersStore(
    useShallow((state) => ({
      ...selectGlobalFiltersState(state),
      ...selectGlobalFiltersActions(state),
    })),
  );
}

export function useGlobalFiltersState(): GlobalFiltersState {
  return useGlobalFiltersStore(useShallow(selectGlobalFiltersState));
}

export function useGlobalFiltersActions(): GlobalFiltersActions {
  return useGlobalFiltersStore(useShallow(selectGlobalFiltersActions));
}
