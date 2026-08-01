import { permanentRedirect } from "next/navigation";

import { buildPassthroughSearchParamsQueryString } from "@/shared/utils/context-routing";

type TeamsPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function TeamsPage({ searchParams }: TeamsPageProps) {
  const resolvedSearchParams = searchParams ? await searchParams : undefined;

  permanentRedirect(`/clubs${buildPassthroughSearchParamsQueryString(resolvedSearchParams)}`);
}
