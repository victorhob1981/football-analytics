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
  const clearSelection = useComparisonStore((state) => state.clear);

  const leftPlayerId = selectedIds[0] ?? null;
  const rightPlayerId = selectedIds[1] ?? null;
  const leftProfileQuery = usePlayerProfile(leftPlayerId, {
    includeRecentMatches: false,
    includeHistory: false,
    includeStats: false,
  });
  const rightProfileQuery = usePlayerProfile(rightPlayerId, {
    includeRecentMatches: false,
    includeHistory: false,
    includeStats: false,
  });

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
  const selectionLabel =
    selectedIds.length === 1 ? "1 jogador selecionado" : `${selectedIds.length} jogadores selecionados`;

  return (
    <aside className="mt-6 space-y-4 rounded-[1.75rem] border border-white/60 bg-[rgba(255,255,255,0.84)] p-5 shadow-[0_24px_60px_-48px_rgba(17,28,45,0.32)] backdrop-blur-xl">
      <header className="space-y-2">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="font-[family:var(--font-profile-headline)] text-2xl font-extrabold text-[#111c2d]">
            Comparativo de jogadores
          </h2>
          <button
            className="rounded-full border border-[rgba(112,121,116,0.28)] bg-white px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.16em] text-[#1f2d40]"
            onClick={() => {
              clearSelection();
            }}
            type="button"
          >
            Limpar comparativo
          </button>
        </div>

        <p className="text-sm text-[#57657a]">
          {selectionLabel}. Selecione ate dois nomes na lista para abrir a leitura lado a lado.
        </p>
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
        <section className="rounded-[1.2rem] border border-[#ffdcc3] bg-[#fff3e8] p-4 text-sm text-[#6e3900]">
          <p>Falha ao carregar dados de comparacao.</p>
          <p>{leftProfileQuery.error?.message ?? rightProfileQuery.error?.message}</p>
        </section>
      ) : null}

      {canRenderComparison && hasEmptyState ? (
        <EmptyState description="Um dos jogadores nao possui dados suficientes para esta comparacao." title="Comparativo sem dados" />
      ) : null}

      {canRenderComparison && !hasLoadingState && !hasErrorState && !hasEmptyState ? (
        <section className="space-y-3">
          {(leftProfileQuery.isPartial || rightProfileQuery.isPartial) ? (
            <PartialDataBanner coverage={combinedCoverage} message="Parte das metricas pode estar incompleta para um dos jogadores." />
          ) : null}

          <div className="flex items-center gap-2">
            <span className="text-sm text-[#57657a]">Cobertura:</span>
            <CoverageBadge coverage={combinedCoverage} />
          </div>

          <ComparisonLayout
            description="Leitura rapida lado a lado das metricas principais."
            left={{
              title: leftDisplayName,
              subtitle: "Jogador 1",
              content: <ComparisonColumnMetrics metricKeys={metricKeys} summary={leftProfile?.summary} />,
            }}
            right={{
              title: rightDisplayName,
              subtitle: "Jogador 2",
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
