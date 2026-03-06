import { buildQueryKey } from "@/shared/utils/queryKeys";
import type { CompetitionFilters } from "@/features/competition/types";

const COMPETITION_DOMAIN = "competition";

export const competitionQueryKeys = {
    all: () => buildQueryKey(COMPETITION_DOMAIN, "all"),
    standings: (filters: CompetitionFilters) =>
        buildQueryKey(COMPETITION_DOMAIN, "standings", filters),
    evolution: (filters: CompetitionFilters) =>
        buildQueryKey(COMPETITION_DOMAIN, "evolution", filters),
    fixtures: (filters: CompetitionFilters) =>
        buildQueryKey(COMPETITION_DOMAIN, "fixtures", filters),
};
