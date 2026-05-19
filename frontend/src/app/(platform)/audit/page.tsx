import Link from "next/link";

import { PlatformStateSurface } from "@/shared/components/feedback/PlatformStateSurface";
import { buildPassthroughSearchParamsQueryString } from "@/shared/utils/context-routing";

type AuditPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function AuditPage({ searchParams }: AuditPageProps) {
  const resolvedSearchParams = searchParams ? await searchParams : undefined;
  const queryString = buildPassthroughSearchParamsQueryString(resolvedSearchParams);

  return (
    <PlatformStateSurface
      actionHref={`/${queryString}`}
      actionLabel="Voltar ao início"
      description="A trilha de auditoria não faz parte da experiência pública do produto nesta fase."
      detail="Os blocos centrais do acervo já estão disponíveis na navegação principal. Use as entradas abaixo para seguir pela experiência pública."
      kicker="Auditoria"
      secondaryAction={
        <>
          <Link
            className="inline-flex items-center rounded-full border border-[rgba(112,121,116,0.28)] bg-white/92 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#1f2d40]"
            href={`/competitions${queryString}`}
          >
            Abrir competições
          </Link>
          <Link
            className="inline-flex items-center rounded-full border border-[rgba(112,121,116,0.28)] bg-white/92 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#1f2d40]"
            href={`/matches${queryString}`}
          >
            Abrir partidas
          </Link>
        </>
      }
      title="Auditoria indisponível"
      tone="warning"
    />
  );
}
