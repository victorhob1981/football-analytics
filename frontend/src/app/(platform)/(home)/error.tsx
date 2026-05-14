"use client";

type PlatformHomeErrorProps = {
  error: Error & { digest?: string };
  reset: () => void;
};

export default function PlatformHomeError({ error, reset }: PlatformHomeErrorProps) {
  return (
    <section>
      <h2>Erro na Home da plataforma</h2>
      <p>{error.message}</p>
      <button onClick={reset} type="button">
        Tentar novamente
      </button>
    </section>
  );
}

