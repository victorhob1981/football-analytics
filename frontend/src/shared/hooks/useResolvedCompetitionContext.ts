import { useMemo } from "react";

import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";
import { resolveCompetitionSeasonContext } from "@/shared/utils/context-routing";

export function useResolvedCompetitionContext(): CompetitionSeasonContext | null {
  const { competitionId, seasonId } = useGlobalFiltersState();

  return useMemo(
    () =>
      resolveCompetitionSeasonContext({
        competitionId,
        seasonId,
      }),
    [competitionId, seasonId],
  );
}
