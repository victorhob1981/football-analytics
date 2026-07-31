import { redirect } from "next/navigation";

import { buildPassthroughSearchParamsQueryString } from "@/shared/utils/context-routing";

type LegacyPowerBiPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function AnalisesPage({ searchParams }: LegacyPowerBiPageProps) {
  redirect(`/power-bi${buildPassthroughSearchParamsQueryString(await searchParams)}`);
}
