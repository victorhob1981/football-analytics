import type { CoverageState } from "@/shared/types/coverage.types";

type PartialDataBannerProps = {
  coverage: CoverageState;
  message?: string;
  className?: string;
};

export function PartialDataBanner({
  coverage,
  message = "Os dados exibidos estão com cobertura parcial neste recorte.",
  className,
}: PartialDataBannerProps) {
  if (coverage.status !== "partial") {
    return null;
  }

  const coverageSuffix = typeof coverage.percentage === "number" ? ` Cobertura atual: ${coverage.percentage.toFixed(1)}%.` : "";
  const classes = [
    "rounded-[1.15rem] border border-[#ffdcc3] bg-[#fff3e8] px-4 py-3 text-sm text-[#6e3900] shadow-[0_18px_48px_-44px_rgba(110,57,0,0.35)]",
    className,
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <section aria-live="polite" className={classes}>
      <strong className="font-semibold uppercase tracking-[0.12em]">Dados parciais.</strong>{" "}
      {message}
      {coverageSuffix}
    </section>
  );
}
