import { buildQueryKey } from "@/shared/utils/queryKeys";

import type {
  CompetitionAnalyticsFilters,
  CompetitionStructureFilters,
  StageTiesFilters,
  TeamJourneyHistoryFilters,
} from "@/features/competitions/types/competition-structure.types";

const COMPETITIONS_DOMAIN = "competition-structure";

export const competitionStructureQueryKeys = {
  all: () => buildQueryKey(COMPETITIONS_DOMAIN, "all"),
  structure: (filters: CompetitionStructureFilters) =>
    buildQueryKey(COMPETITIONS_DOMAIN, "structure", filters),
  analytics: (filters: CompetitionAnalyticsFilters) =>
    buildQueryKey(COMPETITIONS_DOMAIN, "analytics", filters),
  ties: (filters: StageTiesFilters) => buildQueryKey(COMPETITIONS_DOMAIN, "ties", filters),
  teamJourney: (filters: TeamJourneyHistoryFilters) =>
    buildQueryKey(COMPETITIONS_DOMAIN, "team-journey", filters),
};
