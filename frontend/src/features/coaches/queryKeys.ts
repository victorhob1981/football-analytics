import { buildQueryKey } from "@/shared/utils/queryKeys";
import type { CoachProfileFilters, CoachesListFilters } from "@/features/coaches/types";

const COACHES_DOMAIN = "coaches";

export const coachesQueryKeys = {
    all: () => buildQueryKey(COACHES_DOMAIN, "all"),
    list: (filters: CoachesListFilters) => buildQueryKey(COACHES_DOMAIN, "list", filters),
    profile: (coachId: string, filters: CoachProfileFilters) =>
        buildQueryKey(COACHES_DOMAIN, "profile", coachId, filters),
};
