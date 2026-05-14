import { formatMetricValue, getMetric } from "@/config/metrics.registry";

type MetricBadgeProps = {
  metricKey: string;
  value: number | null | undefined;
  showLabel?: boolean;
  className?: string;
};

export function MetricBadge({ metricKey, value, showLabel = true, className }: MetricBadgeProps) {
  const metric = getMetric(metricKey);
  const label = metric?.label ?? metricKey;
  const formattedValue = formatMetricValue(metricKey, value);
  const classes = [
    "inline-flex items-center gap-1 rounded-full border border-slate-300 bg-slate-50 px-2 py-1 text-xs text-slate-700",
    className,
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <span className={classes}>
      {showLabel ? <strong>{label}:</strong> : null}
      <span>{formattedValue}</span>
    </span>
  );
}
