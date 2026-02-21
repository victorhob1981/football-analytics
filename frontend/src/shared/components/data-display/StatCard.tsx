import { formatMetricValue } from "@/config/metrics.registry";
import { formatNumber } from "@/shared/utils/formatters";

type StatCardProps = {
  title: string;
  value: number | string | null | undefined;
  metricKey?: string;
  description?: string;
  footer?: string;
  className?: string;
};

function resolveDisplayValue(value: number | string | null | undefined, metricKey?: string): string {
  if (typeof value === "string") {
    return value.trim().length > 0 ? value : "-";
  }

  if (typeof value === "number") {
    if (metricKey) {
      return formatMetricValue(metricKey, value);
    }

    return formatNumber(value);
  }

  return "-";
}

export function StatCard({ title, value, metricKey, description, footer, className }: StatCardProps) {
  const displayValue = resolveDisplayValue(value, metricKey);
  const classes = ["rounded-lg border border-slate-200 bg-white p-4", className].filter(Boolean).join(" ");

  return (
    <article className={classes}>
      <p className="text-sm text-slate-500">{title}</p>
      <p className="mt-1 text-2xl font-semibold text-slate-900">{displayValue}</p>
      {description ? <p className="mt-2 text-sm text-slate-600">{description}</p> : null}
      {footer ? <p className="mt-2 text-xs text-slate-500">{footer}</p> : null}
    </article>
  );
}
