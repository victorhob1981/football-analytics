import { buildQueryKey } from "@/shared/utils/queryKeys";
import type { CompetitionSeasonContextFilters } from "@/shared/types/context.types";

import type {
  TeamJourneyHistoryFilters,
  TeamMatchesListFilters,
  TeamProfileFilters,
  TeamsListFilters,
} from "@/features/teams/types";

const TEAMS_DOMAIN = "teams";

export const teamsQueryKeys = {
  all: () => buildQueryKey(TEAMS_DOMAIN, "all"),
  list: (filters: TeamsListFilters) => buildQueryKey(TEAMS_DOMAIN, "list", filters),
  contexts: (teamId: string, filters: CompetitionSeasonContextFilters) =>
    buildQueryKey(TEAMS_DOMAIN, "contexts", teamId, filters),
  matches: (teamId: string, filters: TeamMatchesListFilters) =>
    buildQueryKey(TEAMS_DOMAIN, "matches", teamId, filters),
  journey: (teamId: string, filters: TeamJourneyHistoryFilters) =>
    buildQueryKey(TEAMS_DOMAIN, "journey", teamId, filters),
  profile: (teamId: string, filters: TeamProfileFilters) =>
    buildQueryKey(TEAMS_DOMAIN, "profile", teamId, filters),
};
