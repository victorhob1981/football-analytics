import type { InsightSeverity } from "@/shared/types/insight.types";

type InsightBadgeProps = {
  count: number;
  highestSeverity?: InsightSeverity | null;
  className?: string;
};

const SEVERITY_CLASSES: Record<InsightSeverity, string> = {
  info: "border-sky-300 bg-sky-50 text-sky-700",
  warning: "border-amber-300 bg-amber-50 text-amber-700",
  critical: "border-rose-300 bg-rose-50 text-rose-700",
};

export function InsightBadge({ count, highestSeverity = "info", className }: InsightBadgeProps) {
  if (count <= 0) {
    return null;
  }

  const resolvedSeverity = highestSeverity ?? "info";
  const classes = [
    "inline-flex items-center rounded-full border px-2 py-1 text-xs font-medium",
    SEVERITY_CLASSES[resolvedSeverity],
    className,
  ]
    .filter(Boolean)
    .join(" ");

  const label = count === 1 ? "1 insight" : `${count} insights`;

  return <span className={classes}>{label}</span>;
}
