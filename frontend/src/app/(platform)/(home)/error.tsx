"use client";

import Link from "next/link";

import { PlatformStateSurface } from "@/shared/components/feedback/PlatformStateSurface";

type PlatformHomeErrorProps = {
  error: Error & { digest?: string };
  reset: () => void;
};

export default function PlatformHomeError({ reset }: PlatformHomeErrorProps) {
  return (
    <PlatformStateSurface
      actionHref="/"
      actionLabel="Voltar para a home"
      description="Não foi possível carregar a visão inicial agora. Tente novamente ou siga por outra área já estável do produto."
      detail="A falha ficou restrita à abertura da home. O restante da navegação principal continua disponível."
      kicker="Home"
      secondaryAction={
        <>
          <button
            className="inline-flex items-center rounded-full border border-[rgba(112,121,116,0.28)] bg-white/92 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#1f2d40]"
            onClick={reset}
            type="button"
          >
            Tentar novamente
          </button>
          <Link
            className="inline-flex items-center rounded-full border border-[rgba(112,121,116,0.28)] bg-white/92 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#1f2d40]"
            href="/competitions"
          >
            Abrir competições
          </Link>
        </>
      }
      title="Não foi possível carregar a home"
      tone="critical"
    />
  );
}
