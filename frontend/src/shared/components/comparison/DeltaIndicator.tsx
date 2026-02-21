import { formatMetricValue } from "@/config/metrics.registry";
import { formatNumber, formatPercentage } from "@/shared/utils/formatters";

type DeltaIndicatorProps = {
  leftValue: number | null | undefined;
  rightValue: number | null | undefined;
  metricKey?: string;
  precision?: number;
  showRelative?: boolean;
  className?: string;
};

function isValidNumber(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value) && !Number.isNaN(value);
}

function formatAbsoluteDelta(delta: number, metricKey?: string, precision = 2): string {
  if (metricKey) {
    return formatMetricValue(metricKey, delta);
  }

  return formatNumber(delta, precision);
}

export function DeltaIndicator({
  leftValue,
  rightValue,
  metricKey,
  precision = 2,
  showRelative = true,
  className,
}: DeltaIndicatorProps) {
  const classes = ["inline-flex items-center rounded border px-2 py-1 text-xs font-medium", className].filter(Boolean).join(" ");

  if (!isValidNumber(leftValue) || !isValidNumber(rightValue)) {
    return <span className={`${classes} border-slate-300 bg-slate-50 text-slate-600`}>Delta: -</span>;
  }

  const absoluteDelta = rightValue - leftValue;
  const relativeDelta = leftValue === 0 ? null : (absoluteDelta / Math.abs(leftValue)) * 100;
  const signPrefix = absoluteDelta > 0 ? "+" : "";

  const toneClass =
    absoluteDelta > 0
      ? "border-emerald-300 bg-emerald-50 text-emerald-700"
      : absoluteDelta < 0
        ? "border-rose-300 bg-rose-50 text-rose-700"
        : "border-slate-300 bg-slate-50 text-slate-700";

  const absoluteText = `${signPrefix}${formatAbsoluteDelta(absoluteDelta, metricKey, precision)}`;
  const relativeText =
    showRelative && relativeDelta !== null
      ? ` (${relativeDelta > 0 ? "+" : ""}${formatPercentage(relativeDelta, 1)})`
      : showRelative
        ? " (n/a)"
        : "";

  return <span className={`${classes} ${toneClass}`}>Delta: {absoluteText}{relativeText}</span>;
}
