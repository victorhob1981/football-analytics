import { useMemo } from "react";

import { InsightCard } from "@/features/insights/components/InsightCard";
import type { InsightObject, InsightSeverity } from "@/shared/types/insight.types";
import { EmptyState } from "@/shared/components/feedback/EmptyState";

type InsightFeedProps = {
  insights: InsightObject[];
  title?: string;
  emptyTitle?: string;
  emptyDescription?: string;
  className?: string;
};

const SEVERITY_PRIORITY: Record<InsightSeverity, number> = {
  critical: 0,
  warning: 1,
  info: 2,
};

export function getHighestInsightSeverity(insights: InsightObject[]): InsightSeverity | null {
  if (insights.length === 0) {
    return null;
  }

  return insights.reduce<InsightSeverity>((highestSeverity, insight) => {
    if (SEVERITY_PRIORITY[insight.severity] < SEVERITY_PRIORITY[highestSeverity]) {
      return insight.severity;
    }

    return highestSeverity;
  }, insights[0].severity);
}

function sortInsightsBySeverity(insights: InsightObject[]): InsightObject[] {
  return [...insights].sort((left, right) => {
    const severityDifference = SEVERITY_PRIORITY[left.severity] - SEVERITY_PRIORITY[right.severity];

    if (severityDifference !== 0) {
      return severityDifference;
    }

    return left.insight_id.localeCompare(right.insight_id);
  });
}

export function InsightFeed({
  insights,
  title = "Insight Feed",
  emptyTitle = "Sem insights",
  emptyDescription = "Nao ha insights para o contexto selecionado.",
  className,
}: InsightFeedProps) {
  const sortedInsights = useMemo(() => sortInsightsBySeverity(insights), [insights]);
  const classes = ["space-y-3", className].filter(Boolean).join(" ");

  return (
    <section className={classes}>
      <h3 className="text-base font-medium text-slate-900">{title}</h3>

      {sortedInsights.length === 0 ? (
        <EmptyState description={emptyDescription} title={emptyTitle} />
      ) : (
        <div className="space-y-2">
          {sortedInsights.map((insight) => (
            <InsightCard insight={insight} key={insight.insight_id} />
          ))}
        </div>
      )}
    </section>
  );
}
