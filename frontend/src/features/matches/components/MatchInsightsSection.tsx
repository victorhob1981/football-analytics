import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { useInsights } from "@/shared/hooks/useInsights";
import type { InsightObject, InsightSeverity } from "@/shared/types/insight.types";

type MatchInsightsSectionProps = {
  matchId: string;
};

export function MatchInsightsSection({ matchId }: MatchInsightsSectionProps) {
  const insightsQuery = useInsights({
    entityType: "match",
    entityId: matchId,
    filters: {
      competitionId: null,
      seasonId: null,
      roundId: null,
      monthKey: null,
      venue: "all",
      lastN: null,
      dateRangeStart: null,
      dateRangeEnd: null,
    },
    enabled: matchId.trim().length > 0,
  });

  const insights = insightsQuery.data ?? [];
  const hasFatalError = insightsQuery.isError && !insightsQuery.data;
  const sortedInsights = [...insights].sort((left, right) => {
    const priority: Record<InsightSeverity, number> = {
      critical: 0,
      warning: 1,
      info: 2,
    };
    const diff = priority[left.severity] - priority[right.severity];
    if (diff !== 0) {
      return diff;
    }

    return left.insight_id.localeCompare(right.insight_id);
  });

  return (
    <section className="space-y-2 rounded-md border border-slate-200 bg-white p-4">
      <h2 className="text-lg font-medium text-slate-900">InsightFeed da Partida</h2>

      {insightsQuery.isLoading ? (
        <div className="space-y-2">
          <LoadingSkeleton height={64} />
          <LoadingSkeleton height={64} />
        </div>
      ) : hasFatalError ? (
        <p className="rounded border border-rose-300 bg-rose-50 p-3 text-sm text-rose-700">
          Falha ao carregar insights: {insightsQuery.error?.message}
        </p>
      ) : (
        <>
          {insightsQuery.isPartial ? <PartialDataBanner coverage={insightsQuery.coverage} /> : null}
          {insightsQuery.isEmpty ? (
            <p className="rounded border border-dashed border-slate-300 bg-slate-50 p-3 text-sm text-slate-600">
              Sem insights para esta partida.
            </p>
          ) : (
            <ul className="space-y-2">
              {sortedInsights.map((insight) => (
                <InsightItem insight={insight} key={insight.insight_id} />
              ))}
            </ul>
          )}
        </>
      )}
    </section>
  );
}

function InsightItem({ insight }: { insight: InsightObject }) {
  const severityStyles: Record<InsightSeverity, string> = {
    info: "border-sky-200 bg-sky-50 text-sky-900",
    warning: "border-amber-200 bg-amber-50 text-amber-900",
    critical: "border-rose-200 bg-rose-50 text-rose-900",
  };

  return (
    <li className={`space-y-1 rounded border p-3 text-sm ${severityStyles[insight.severity]}`}>
      <p className="text-xs font-semibold uppercase tracking-wide">{insight.severity}</p>
      <p>{insight.explanation}</p>
    </li>
  );
}
