import { buildQueryKey } from "@/shared/utils/queryKeys";
import type { CompetitionSeasonContextFilters } from "@/shared/types/context.types";

import type { PlayerProfileFilters, PlayersListFilters } from "@/features/players/types";

const PLAYERS_DOMAIN = "players";

export const playersQueryKeys = {
  all: () => buildQueryKey(PLAYERS_DOMAIN, "all"),
  list: (filters: PlayersListFilters) => buildQueryKey(PLAYERS_DOMAIN, "list", filters),
  contexts: (playerId: string, filters: CompetitionSeasonContextFilters) =>
    buildQueryKey(PLAYERS_DOMAIN, "contexts", playerId, filters),
  profile: (playerId: string, filters: PlayerProfileFilters) =>
    buildQueryKey(PLAYERS_DOMAIN, "profile", playerId, filters),
};
