"use client";

import { useEffect, type ReactNode } from "react";
import { useSearchParams } from "next/navigation";

import type { CompetitionDef } from "@/config/competitions.registry";
import { resolveSeasonForCompetition } from "@/config/seasons.registry";
import { useGlobalFiltersActions, useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";

type CompetitionRouteContextSyncProps = {
  children: ReactNode;
  competition: CompetitionDef;
};

export function CompetitionRouteContextSync({
  children,
  competition,
}: CompetitionRouteContextSyncProps) {
  const searchParams = useSearchParams();
  const { competitionId, seasonId, roundId } = useGlobalFiltersState();
  const { setCompetitionId, setRoundId, setSeasonId } = useGlobalFiltersActions();
  const resolvedSeason = resolveSeasonForCompetition(competition, { seasonId });
  const hasExplicitRoundId = searchParams.get("roundId") !== null;

  useEffect(() => {
    if (!hasExplicitRoundId && roundId !== null && competitionId !== competition.id) {
      setRoundId(null);
    }

    if (competitionId !== competition.id) {
      setCompetitionId(competition.id);
    }

    if (seasonId && !resolvedSeason) {
      setSeasonId(null);
      if (!hasExplicitRoundId && roundId !== null) {
        setRoundId(null);
      }
    }
  }, [
    competition.id,
    competitionId,
    hasExplicitRoundId,
    resolvedSeason,
    roundId,
    seasonId,
    setCompetitionId,
    setRoundId,
    setSeasonId,
  ]);

  return children;
}
