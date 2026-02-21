"use client";

import { useMemo } from "react";

import { formatMetricValue, getMetric } from "@/config/metrics.registry";
import { usePlayerProfile } from "@/features/players/hooks";
import type { PlayerStatsSummary } from "@/features/players/types";
import { ComparisonLayout } from "@/shared/components/comparison/ComparisonLayout";
import { DeltaIndicator } from "@/shared/components/comparison/DeltaIndicator";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { useComparisonStore } from "@/shared/stores/comparison.store";
import type { CoverageState } from "@/shared/types/coverage.types";

const DEFAULT_PLAYER_COMPARISON_METRICS = [
  "minutes_played",
  "goals",
  "assists",
  "shots_total",
  "pass_accuracy_pct",
  "player_rating",
] as const;

const PLAYER_SUMMARY_METRIC_READERS: Record<string, (summary: PlayerStatsSummary | undefined) => number | null | undefined> = {
  minutes_played: (summary) => summary?.minutesPlayed,
  goals: (summary) => summary?.goals,
  assists: (summary) => summary?.assists,
  shots_total: (summary) => summary?.shotsTotal,
  pass_accuracy_pct: (summary) => summary?.passAccuracyPct,
  player_rating: (summary) => summary?.rating,
  yellow_cards: (summary) => summary?.yellowCards,
  red_cards: (summary) => summary?.redCards,
};

function resolveCoverageStatus(coverages: CoverageState[]): CoverageState {
  if (coverages.length === 0) {
    return { status: "unknown" };
  }

  if (coverages.some((coverage) => coverage.status === "partial")) {
    return { status: "partial", label: "Cobertura parcial no comparativo" };
  }

  if (coverages.every((coverage) => coverage.status === "complete")) {
    return { status: "complete", label: "Cobertura completa no comparativo" };
  }

  if (coverages.every((coverage) => coverage.status === "empty")) {
    return { status: "empty", label: "Sem dados no comparativo" };
  }

  if (coverages.some((coverage) => coverage.status === "empty")) {
    return { status: "partial", label: "Uma das entidades nao possui dados" };
  }

  return { status: "unknown", label: "Cobertura desconhecida no comparativo" };
}

function resolveMetricKeys(activeMetrics: string[]): string[] {
  const fallbackMetrics = Array.from(DEFAULT_PLAYER_COMPARISON_METRICS);
  const candidates = activeMetrics.length > 0 ? activeMetrics : fallbackMetrics;

  return candidates.filter((metricKey) => Boolean(getMetric(metricKey)) && Boolean(PLAYER_SUMMARY_METRIC_READERS[metricKey]));
}

type ComparisonColumnMetricsProps = {
  summary: PlayerStatsSummary | undefined;
  metricKeys: string[];
};

function ComparisonColumnMetrics({ summary, metricKeys }: ComparisonColumnMetricsProps) {
  return (
    <ul className="space-y-2 text-sm">
      {metricKeys.map((metricKey) => {
        const metric = getMetric(metricKey);
        const value = PLAYER_SUMMARY_METRIC_READERS[metricKey]?.(summary);

        return (
          <li className="flex items-center justify-between gap-3 border-b border-slate-100 pb-1 last:border-b-0 last:pb-0" key={metricKey}>
            <span className="text-slate-600">{metric?.label ?? metricKey}</span>
            <strong className="text-slate-900">{formatMetricValue(metricKey, value)}</strong>
          </li>
        );
      })}
    </ul>
  );
}

type DeltaListProps = {
  metricKeys: string[];
  leftSummary: PlayerStatsSummary | undefined;
  rightSummary: PlayerStatsSummary | undefined;
};

function DeltaList({ metricKeys, leftSummary, rightSummary }: DeltaListProps) {
  return (
    <section className="space-y-2 rounded-md border border-slate-200 bg-white p-3">
      <h3 className="text-sm font-medium text-slate-900">Deltas (direita - esquerda)</h3>
      <ul className="space-y-2 text-sm">
        {metricKeys.map((metricKey) => {
          const metric = getMetric(metricKey);
          const leftValue = PLAYER_SUMMARY_METRIC_READERS[metricKey]?.(leftSummary);
          const rightValue = PLAYER_SUMMARY_METRIC_READERS[metricKey]?.(rightSummary);

          return (
            <li className="flex items-center justify-between gap-3" key={`delta-${metricKey}`}>
              <span className="text-slate-600">{metric?.label ?? metricKey}</span>
              <DeltaIndicator leftValue={leftValue} metricKey={metricKey} rightValue={rightValue} />
            </li>
          );
        })}
      </ul>
    </section>
  );
}

function toDisplayName(playerId: string, playerName: string | null | undefined): string {
  const normalizedName = playerName?.trim();
  return normalizedName && normalizedName.length > 0 ? normalizedName : `Jogador ${playerId}`;
}

export function PlayerComparisonPanel() {
  const entityType = useComparisonStore((state) => state.entityType);
  const selectedIds = useComparisonStore((state) => state.selectedIds);
  const activeMetrics = useComparisonStore((state) => state.activeMetrics);
  const removeSelectedId = useComparisonStore((state) => state.remove);
  const clearSelection = useComparisonStore((state) => state.clear);

  const leftPlayerId = selectedIds[0] ?? null;
  const rightPlayerId = selectedIds[1] ?? null;
  const leftProfileQuery = usePlayerProfile(leftPlayerId, { includeRecentMatches: false });
  const rightProfileQuery = usePlayerProfile(rightPlayerId, { includeRecentMatches: false });

  const metricKeys = useMemo(() => resolveMetricKeys(activeMetrics), [activeMetrics]);
  const canRenderComparison = entityType === "player" && selectedIds.length === 2;

  if (entityType !== "player" || selectedIds.length === 0) {
    return null;
  }

  const combinedCoverage = resolveCoverageStatus(
    [leftProfileQuery.coverage, rightProfileQuery.coverage].filter((coverage): coverage is CoverageState => Boolean(coverage)),
  );

  const hasLoadingState = canRenderComparison && (leftProfileQuery.isLoading || rightProfileQuery.isLoading);
  const hasErrorState = canRenderComparison && (leftProfileQuery.isError || rightProfileQuery.isError);
  const hasEmptyState = canRenderComparison && (leftProfileQuery.isEmpty || rightProfileQuery.isEmpty);

  const leftProfile = leftProfileQuery.data;
  const rightProfile = rightProfileQuery.data;
  const leftDisplayName = toDisplayName(leftPlayerId ?? "?", leftProfile?.player.playerName);
  const rightDisplayName = toDisplayName(rightPlayerId ?? "?", rightProfile?.player.playerName);

  return (
    <aside className="space-y-3 border-t border-slate-200 bg-slate-100 px-4 py-4">
      <header className="space-y-2">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="text-base font-semibold text-slate-900">Player Comparison</h2>
          <button
            className="rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-700"
            onClick={() => {
              clearSelection();
            }}
            type="button"
          >
            Limpar comparativo
          </button>
        </div>

        <div className="flex flex-wrap items-center gap-2 text-xs">
          {selectedIds.map((selectedId) => (
            <span className="inline-flex items-center gap-2 rounded border border-slate-300 bg-white px-2 py-1 text-slate-700" key={selectedId}>
              ID: {selectedId}
              <button
                className="rounded border border-slate-300 px-1 py-0.5 text-[11px] text-slate-600"
                onClick={() => {
                  removeSelectedId(selectedId);
                }}
                type="button"
              >
                remover
              </button>
            </span>
          ))}
        </div>
      </header>

      {!canRenderComparison ? (
        <EmptyState
          description="Selecione dois jogadores na lista para habilitar o comparativo lado a lado."
          title="Comparativo aguardando selecao"
        />
      ) : null}

      {canRenderComparison && hasLoadingState ? (
        <section className="grid gap-3 md:grid-cols-2">
          <LoadingSkeleton height={180} />
          <LoadingSkeleton height={180} />
        </section>
      ) : null}

      {canRenderComparison && hasErrorState ? (
        <section className="rounded-md border border-rose-300 bg-rose-50 p-3 text-sm text-rose-700">
          <p>Falha ao carregar dados de comparacao.</p>
          <p>{leftProfileQuery.error?.message ?? rightProfileQuery.error?.message}</p>
        </section>
      ) : null}

      {canRenderComparison && hasEmptyState ? (
        <EmptyState description="Um dos jogadores nao possui dados para o recorte atual." title="Comparativo sem dados" />
      ) : null}

      {canRenderComparison && !hasLoadingState && !hasErrorState && !hasEmptyState ? (
        <section className="space-y-3">
          {(leftProfileQuery.isPartial || rightProfileQuery.isPartial) ? (
            <PartialDataBanner coverage={combinedCoverage} message="Parte das metricas pode estar incompleta para um dos jogadores." />
          ) : null}

          <div className="flex items-center gap-2">
            <span className="text-sm text-slate-600">Cobertura:</span>
            <CoverageBadge coverage={combinedCoverage} />
          </div>

          <ComparisonLayout
            description="Conjunto inicial de metricas para comparacao rapida."
            left={{
              title: leftDisplayName,
              subtitle: `ID: ${leftPlayerId}`,
              content: <ComparisonColumnMetrics metricKeys={metricKeys} summary={leftProfile?.summary} />,
            }}
            right={{
              title: rightDisplayName,
              subtitle: `ID: ${rightPlayerId}`,
              content: <ComparisonColumnMetrics metricKeys={metricKeys} summary={rightProfile?.summary} />,
            }}
            title="Comparativo de jogadores"
          />

          <DeltaList leftSummary={leftProfile?.summary} metricKeys={metricKeys} rightSummary={rightProfile?.summary} />
        </section>
      ) : null}
    </aside>
  );
}
