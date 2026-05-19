"use client";

import type { CompetitionSeasonContext } from "@/shared/types/context.types";

import { CompetitionSeasonSurface } from "./CompetitionSeasonSurface";

type SeasonHubContentProps = {
  context: CompetitionSeasonContext;
  initialTab?: string | null;
};

export function SeasonHubContent({ context, initialTab }: SeasonHubContentProps) {
  return <CompetitionSeasonSurface context={context} initialTab={initialTab} />;
}
