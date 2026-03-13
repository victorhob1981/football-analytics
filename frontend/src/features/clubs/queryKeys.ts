import { buildQueryKey } from "@/shared/utils/queryKeys";
import type { ClubProfileFilters, ClubsListFilters } from "@/features/clubs/types";

const CLUBS_DOMAIN = "clubs";

export const clubsQueryKeys = {
    all: () => buildQueryKey(CLUBS_DOMAIN, "all"),
    list: (filters: ClubsListFilters) => buildQueryKey(CLUBS_DOMAIN, "list", filters),
    profile: (clubId: string, filters: ClubProfileFilters) =>
        buildQueryKey(CLUBS_DOMAIN, "profile", clubId, filters),
};
