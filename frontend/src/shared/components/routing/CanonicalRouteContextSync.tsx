"use client";

import { useEffect, type ReactNode } from "react";
import { useSearchParams } from "next/navigation";

import { useGlobalFiltersActions, useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";

type CanonicalRouteContextSyncProps = {
  children: ReactNode;
  context: CompetitionSeasonContext;
};

export function CanonicalRouteContextSync({ children, context }: CanonicalRouteContextSyncProps) {
  const searchParams = useSearchParams();
  const { competitionId, seasonId, roundId } = useGlobalFiltersState();
  const { setCompetitionId, setRoundId, setSeasonId } = useGlobalFiltersActions();
  const hasExplicitRoundId = searchParams.get("roundId") !== null;

  useEffect(() => {
    if (
      !hasExplicitRoundId &&
      roundId !== null &&
      (competitionId !== context.competitionId || seasonId !== context.seasonId)
    ) {
      setRoundId(null);
    }

    if (competitionId !== context.competitionId) {
      setCompetitionId(context.competitionId);
    }

    if (seasonId !== context.seasonId) {
      setSeasonId(context.seasonId);
    }
  }, [
    competitionId,
    context.competitionId,
    context.seasonId,
    hasExplicitRoundId,
    roundId,
    seasonId,
    setCompetitionId,
    setRoundId,
    setSeasonId,
  ]);

  return children;
}
