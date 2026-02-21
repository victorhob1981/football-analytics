"use client";

import { InsightBadge, InsightFeed, getHighestInsightSeverity } from "@/features/insights/components";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useInsights } from "@/shared/hooks/useInsights";

export default function PlatformHomePage() {
  const { competitionId } = useGlobalFiltersState();
  const insightEntityType = competitionId ? "competition" : "global";
  const insightsQuery = useInsights({
    entityType: insightEntityType,
    entityId: competitionId ?? null,
  });

  const insights = insightsQuery.data ?? [];
  const highestSeverity = getHighestInsightSeverity(insights);

  if (insightsQuery.isLoading) {
    return (
      <main className="space-y-4">
        <header className="space-y-1">
          <h1 className="text-xl font-semibold">Home da Plataforma</h1>
          <p className="text-sm text-slate-600">Carregando insights executivos...</p>
        </header>

        <LoadingSkeleton height={84} />
        <LoadingSkeleton height={120} />
        <LoadingSkeleton height={120} />
      </main>
    );
  }

  if (insightsQuery.isError && !insightsQuery.data) {
    return (
      <main className="space-y-4">
        <h1 className="text-xl font-semibold">Home da Plataforma</h1>
        <section className="rounded-md border border-rose-300 bg-rose-50 p-4 text-sm text-rose-700">
          <p>Falha ao carregar insights da home.</p>
          <p>{insightsQuery.error?.message}</p>
        </section>
      </main>
    );
  }

  return (
    <main className="space-y-4">
      <header className="space-y-1">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-xl font-semibold">Home da Plataforma</h1>
          <InsightBadge count={insights.length} highestSeverity={highestSeverity} />
        </div>
        <p className="text-sm text-slate-600">
          Feed de insights no contexto <strong>{insightEntityType}</strong>.
        </p>
      </header>

      {insightsQuery.isError ? (
        <section className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
          Dados carregados com alerta: {insightsQuery.error?.message}
        </section>
      ) : null}

      {insightsQuery.isPartial ? <PartialDataBanner coverage={insightsQuery.coverage} /> : null}

      <div className="flex items-center gap-2">
        <span className="text-sm text-slate-600">Status de cobertura:</span>
        <CoverageBadge coverage={insightsQuery.coverage} />
      </div>

      {insightsQuery.isEmpty ? (
        <EmptyState
          description="Nenhum insight foi retornado para o contexto atual da home."
          title="Sem insights na home"
        />
      ) : (
        <InsightFeed insights={insights} title="Insights em destaque" />
      )}
    </main>
  );
}
