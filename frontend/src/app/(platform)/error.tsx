"use client";

import { useEffect } from "react";

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
    <section>
      <h2>Erro na plataforma</h2>
      <p>{error.message}</p>
      <button onClick={reset} type="button">
        Tentar novamente
      </button>
    </section>
  );
}

