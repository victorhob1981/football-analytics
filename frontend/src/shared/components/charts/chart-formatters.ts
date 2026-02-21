import { formatMetricValue } from "@/config/metrics.registry";
import { formatNumber } from "@/shared/utils/formatters";

export function formatChartValue(value: unknown, metricKey?: string): string {
  if (typeof value !== "number" || Number.isNaN(value) || !Number.isFinite(value)) {
    return "-";
  }

  if (metricKey) {
    return formatMetricValue(metricKey, value);
  }

  return formatNumber(value);
}
