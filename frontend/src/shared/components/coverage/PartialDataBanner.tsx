import type { CoverageState } from "@/shared/types/coverage.types";

type PartialDataBannerProps = {
  coverage: CoverageState;
  message?: string;
  className?: string;
};

export function PartialDataBanner({
  coverage,
  message = "Os dados exibidos estao com cobertura parcial neste recorte.",
  className,
}: PartialDataBannerProps) {
  if (coverage.status !== "partial") {
    return null;
  }

  const coverageSuffix = typeof coverage.percentage === "number" ? ` Cobertura atual: ${coverage.percentage.toFixed(1)}%.` : "";
  const classes = ["rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800", className]
    .filter(Boolean)
    .join(" ");

  return (
    <section aria-live="polite" className={classes}>
      <strong>Dados parciais.</strong> {message}
      {coverageSuffix}
    </section>
  );
}
