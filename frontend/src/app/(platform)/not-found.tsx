import Link from "next/link";

import { PlatformStateSurface } from "@/shared/components/feedback/PlatformStateSurface";

export default function PlatformNotFound() {
  return (
    <PlatformStateSurface
      actionHref="/"
      actionLabel="Voltar ao início"
      description="Esse caminho não existe ou não está disponível dentro do recorte atual do produto."
      detail="Confira o endereço ou retome a navegação pelas áreas principais do acervo histórico."
      secondaryAction={
        <>
          <Link
            className="inline-flex items-center rounded-full border border-[rgba(112,121,116,0.28)] bg-white/92 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#1f2d40]"
            href="/competitions"
          >
            Abrir competições
          </Link>
          <Link
            className="inline-flex items-center rounded-full border border-[rgba(112,121,116,0.28)] bg-white/92 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#1f2d40]"
            href="/matches"
          >
            Abrir partidas
          </Link>
        </>
      }
      title="Área não encontrada"
      tone="warning"
    />
  );
}
