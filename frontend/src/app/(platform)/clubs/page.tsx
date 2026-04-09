import { redirect } from "next/navigation";

import { buildPassthroughSearchParamsQueryString } from "@/shared/utils/context-routing";

type ClubsPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function ClubsPage({ searchParams }: ClubsPageProps) {
  const resolvedSearchParams = searchParams ? await searchParams : undefined;
  const queryString = buildPassthroughSearchParamsQueryString(resolvedSearchParams);

  redirect(`/competitions${queryString}`);
}
