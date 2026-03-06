"use client";

import { AlertCircle, AlertTriangle, Info } from "lucide-react";

import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useInsights } from "@/shared/hooks/useInsights";
import type { InsightObject, InsightSeverity } from "@/shared/types/insight.types";

const SEVERITY_PRIORITY: Record<InsightSeverity, number> = {
  critical: 0,
  warning: 1,
  info: 2,
};

const SEVERITY_CONFIG: Record<
  InsightSeverity,
  {
    card: string;
    badge: string;
    icon: typeof AlertCircle;
    title: string;
    description: string;
    label: string;
  }
> = {
  critical: {
    card: "border-rose-700 bg-rose-950/25",
    badge: "border-rose-700 bg-rose-900/70 text-rose-200",
    icon: AlertCircle,
    title: "text-rose-100",
    description: "text-rose-200/90",
    label: "Critico",
  },
  warning: {
    card: "border-amber-700 bg-amber-950/25",
    badge: "border-amber-700 bg-amber-900/70 text-amber-200",
    icon: AlertTriangle,
    title: "text-amber-100",
    description: "text-amber-200/90",
    label: "Aviso",
  },
  info: {
    card: "border-sky-700 bg-sky-950/25",
    badge: "border-sky-700 bg-sky-900/70 text-sky-200",
    icon: Info,
    title: "text-sky-100",
    description: "text-sky-200/90",
    label: "Info",
  },
};

function sortBySeverity(insights: InsightObject[]): InsightObject[] {
  return [...insights].sort((left, right) => {
    const severityDiff = SEVERITY_PRIORITY[left.severity] - SEVERITY_PRIORITY[right.severity];

    if (severityDiff !== 0) {
      return severityDiff;
    }

    return left.insight_id.localeCompare(right.insight_id);
  });
}

function getHighestSeverity(insights: InsightObject[]): InsightSeverity | null {
  if (insights.length === 0) {
    return null;
  }

  return insights.reduce<InsightSeverity>((highest, current) => {
    if (SEVERITY_PRIORITY[current.severity] < SEVERITY_PRIORITY[highest]) {
      return current.severity;
    }

    return highest;
  }, insights[0].severity);
}

function resolveInsightTitle(explanation: string): string {
  const normalized = explanation.trim();
  if (normalized.length <= 72) {
    return normalized;
  }

  const separators = [". ", ": ", " - "];

  for (const separator of separators) {
    const index = normalized.indexOf(separator);

    if (index > 18 && index < 90) {
      return normalized.slice(0, index).trim();
    }
  }

  return `${normalized.slice(0, 72).trim()}...`;
}

function resolveInsightDescription(explanation: string, title: string): string {
  if (explanation === title) {
    return "Sem detalhes adicionais.";
  }

  const remainder = explanation
    .slice(title.length)
    .replace(/^[:.\s-]+/, "")
    .trim();

  return remainder.length > 0 ? remainder : explanation;
}

function InsightsBadge({
  count,
  highestSeverity,
}: {
  count: number;
  highestSeverity: InsightSeverity | null;
}) {
  if (count <= 0) {
    return null;
  }

  const severity = highestSeverity ?? "info";
  const config = SEVERITY_CONFIG[severity];
  const label = count === 1 ? "1 insight" : `${count} insights`;

  return <span className={`rounded-full border px-2 py-1 text-xs font-medium ${config.badge}`}>{label}</span>;
}

export function HomeInsightsSection() {
  const { competitionId } = useGlobalFiltersState();
  const entityType = competitionId ? "competition" : "global";

  const { data: insights, isLoading, isEmpty } = useInsights({
    entityType,
    entityId: competitionId ?? null,
  });

  const list = sortBySeverity(insights ?? []);
  const highestSeverity = getHighestSeverity(list);

  if (isLoading) {
    return (
      <div className="space-y-3">
        {Array.from({ length: 4 }).map((_, i) => (
          <LoadingSkeleton className="bg-slate-700" height={96} key={i} />
        ))}
      </div>
    );
  }

  if (isEmpty || list.length === 0) {
    return (
      <EmptyState
        className="rounded-xl border-slate-700 bg-slate-900 [&_h3]:text-slate-100 [&_p]:text-slate-400"
        description="Nenhum insight disponivel para o contexto atual da temporada."
        title="Sem insights"
      />
    );
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        <InsightsBadge count={list.length} highestSeverity={highestSeverity} />
        <span className="text-xs text-slate-400">ordenados por severidade</span>
      </div>

      {list.map((insight) => {
        const config = SEVERITY_CONFIG[insight.severity];
        const Icon = config.icon;
        const title = resolveInsightTitle(insight.explanation);
        const description = resolveInsightDescription(insight.explanation, title);

        return (
          <article className={`rounded-lg border p-4 ${config.card}`} key={insight.insight_id}>
            <div className="flex items-start gap-3">
              <Icon className={config.description} size={18} />

              <div className="min-w-0 flex-1 space-y-1">
                <div className="flex items-start justify-between gap-2">
                  <h3 className={`text-sm font-semibold ${config.title}`}>{title}</h3>
                  <span className={`rounded border px-2 py-0.5 text-[11px] font-medium ${config.badge}`}>
                    {config.label}
                  </span>
                </div>

                <p className={`text-xs ${config.description}`}>{description}</p>
                <p className="text-[11px] text-slate-400">{insight.reference_period}</p>
              </div>
            </div>
          </article>
        );
      })}
    </div>
  );
}
