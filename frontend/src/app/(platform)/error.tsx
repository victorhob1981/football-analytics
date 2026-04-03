"use client";

import { useEffect } from "react";
import Link from "next/link";

import { PlatformStateSurface } from "@/shared/components/feedback/PlatformStateSurface";

type PlatformErrorProps = {
  error: Error & { digest?: string };
  reset: () => void;
};

export default function PlatformError({ error, reset }: PlatformErrorProps) {
  useEffect(() => {
    // TODO: integrar com tracking central de erro.
    console.error(error);
  }, [error]);

  return (
    <PlatformStateSurface
      actionHref="/"
      actionLabel="Voltar ao início"
      description="Não foi possível abrir esta área agora. Tente novamente ou siga para uma rota estável do produto."
      detail="O problema ficou restrito a esta superfície. A shell e os atalhos principais seguem disponíveis."
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
      title="Não foi possível abrir esta área"
      tone="critical"
    />
  );
}
