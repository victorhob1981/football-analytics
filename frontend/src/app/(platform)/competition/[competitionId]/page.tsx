import { redirect } from "next/navigation";

import { getCompetitionById } from "@/config/competitions.registry";
import {
  buildCompetitionHubPath,
  buildPassthroughSearchParamsQueryString,
} from "@/shared/utils/context-routing";

type CompetitionPageProps = {
  params: Promise<{ competitionId: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function CompetitionPage({ params, searchParams }: CompetitionPageProps) {
  const { competitionId } = await params;
  const resolvedSearchParams = searchParams ? await searchParams : undefined;
  const competition = getCompetitionById(competitionId);
  const queryString = buildPassthroughSearchParamsQueryString(resolvedSearchParams);

  redirect(competition ? `${buildCompetitionHubPath(competition.key)}${queryString}` : `/competitions${queryString}`);
}
