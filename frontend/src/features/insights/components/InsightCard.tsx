import type { InsightObject, InsightSeverity } from "@/shared/types/insight.types";
import { formatNumber } from "@/shared/utils/formatters";

type InsightCardProps = {
  insight: InsightObject;
  className?: string;
};

const SEVERITY_LABEL: Record<InsightSeverity, string> = {
  info: "Info",
  warning: "Warning",
  critical: "Critical",
};

const SEVERITY_CLASSES: Record<InsightSeverity, string> = {
  info: "border-sky-300 bg-sky-50 text-sky-700",
  warning: "border-amber-300 bg-amber-50 text-amber-700",
  critical: "border-rose-300 bg-rose-50 text-rose-700",
};

function formatEvidenceValue(value: number): string {
  return formatNumber(value, Number.isInteger(value) ? 0 : 2);
}

export function InsightCard({ insight, className }: InsightCardProps) {
  const evidences = Object.entries(insight.evidences ?? {});
  const classes = ["space-y-3 rounded-md border border-slate-200 bg-white p-4", className].filter(Boolean).join(" ");

  return (
    <article className={classes}>
      <header className="flex items-start justify-between gap-2">
        <span
          className={`inline-flex items-center rounded-full border px-2 py-1 text-xs font-medium ${SEVERITY_CLASSES[insight.severity]}`}
        >
          {SEVERITY_LABEL[insight.severity]}
        </span>
        <span className="text-xs text-slate-500">ID: {insight.insight_id}</span>
      </header>

      <p className="text-sm text-slate-800">{insight.explanation}</p>

      <div className="text-xs text-slate-600">
        <strong className="text-slate-700">Periodo:</strong> {insight.reference_period}
      </div>

      {evidences.length > 0 ? (
        <ul className="space-y-1 text-sm">
          {evidences.map(([key, value]) => (
            <li className="flex items-center justify-between gap-2 rounded bg-slate-50 px-2 py-1" key={`${insight.insight_id}-${key}`}>
              <span className="text-slate-600">{key}</span>
              <span className="font-medium text-slate-900">{formatEvidenceValue(value)}</span>
            </li>
          ))}
        </ul>
      ) : null}

      {insight.data_source.length > 0 ? (
        <p className="text-xs text-slate-500">Fonte: {insight.data_source.join(", ")}</p>
      ) : null}
    </article>
  );
}
