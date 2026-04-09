import Link from "next/link";

import { PlatformStateSurface } from "@/shared/components/feedback/PlatformStateSurface";
import { buildPassthroughSearchParamsQueryString } from "@/shared/utils/context-routing";

type MarketPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function MarketPage({ searchParams }: MarketPageProps) {
  const resolvedSearchParams = searchParams ? await searchParams : undefined;
  const queryString = buildPassthroughSearchParamsQueryString(resolvedSearchParams);

  return (
    <PlatformStateSurface
      actionHref={`/players${queryString}`}
      actionLabel="Abrir jogadores"
      description="A trilha de transferências ainda não tem contrato público estável neste produto."
      detail={
        <p>
          Use jogadores, times, rankings e partidas para continuar a exploração no mesmo recorte
          competitivo.
        </p>
      }
      kicker="Mercado"
      secondaryAction={
        <Link
          className="inline-flex items-center rounded-full border border-[rgba(112,121,116,0.28)] bg-white/92 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#1f2d40]"
          href={`/competitions${queryString}`}
        >
          Abrir competições
        </Link>
      }
      title="Mercado indisponível"
      tone="warning"
    />
  );
}
