"use client";

import { useMemo } from "react";

import type { ColumnDef } from "@tanstack/react-table";

import { InsightBadge, InsightFeed, getHighestInsightSeverity } from "@/features/insights/components";
import { usePlayerProfile } from "@/features/players/hooks";
import type { PlayerMatchStatsPoint } from "@/features/players/types";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { DataTable } from "@/shared/components/data-display/DataTable";
import { MetricBadge } from "@/shared/components/data-display/MetricBadge";
import { StatCard } from "@/shared/components/data-display/StatCard";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { useInsights } from "@/shared/hooks/useInsights";
import { formatDate } from "@/shared/utils/formatters";
import { formatMetricValue } from "@/config/metrics.registry";

type PlayerProfileContentProps = {
  playerId: string;
};

export function PlayerProfileContent({ playerId }: PlayerProfileContentProps) {
  const profileQuery = usePlayerProfile(playerId, { includeRecentMatches: true });
  const playerInsightsQuery = useInsights({
    entityType: "player",
    entityId: playerId,
  });

  const recentMatchesColumns = useMemo<Array<ColumnDef<PlayerMatchStatsPoint, unknown>>>(
    () => [
      {
        accessorKey: "playedAt",
        header: "Data",
        cell: ({ row }) => formatDate(row.original.playedAt),
      },
      {
        accessorKey: "opponentName",
        header: "Adversario",
        cell: ({ row }) => row.original.opponentName ?? "-",
      },
      {
        accessorKey: "minutesPlayed",
        header: "Minutos",
        cell: ({ row }) => formatMetricValue("minutes_played", row.original.minutesPlayed),
      },
      {
        accessorKey: "goals",
        header: "Gols",
        cell: ({ row }) => formatMetricValue("goals", row.original.goals),
      },
      {
        accessorKey: "assists",
        header: "Assistencias",
        cell: ({ row }) => formatMetricValue("assists", row.original.assists),
      },
      {
        accessorKey: "rating",
        header: "Rating",
        cell: ({ row }) => formatMetricValue("player_rating", row.original.rating),
      },
    ],
    [],
  );
  const playerInsights = playerInsightsQuery.data ?? [];
  const highestPlayerInsightSeverity = getHighestInsightSeverity(playerInsights);
  const hasFatalPlayerInsightsError = playerInsightsQuery.isError && !playerInsightsQuery.data;

  if (profileQuery.isLoading) {
    return (
      <main className="space-y-4">
        <header className="space-y-1">
          <h1 className="text-xl font-semibold">Jogador {playerId}</h1>
          <p className="text-sm text-slate-600">Carregando perfil...</p>
        </header>

        <section className="grid gap-3 md:grid-cols-2 lg:grid-cols-4">
          <LoadingSkeleton height={96} />
          <LoadingSkeleton height={96} />
          <LoadingSkeleton height={96} />
          <LoadingSkeleton height={96} />
        </section>

        <section className="space-y-2">
          <LoadingSkeleton height={18} width={220} />
          <LoadingSkeleton height={40} />
          <LoadingSkeleton height={40} />
        </section>
      </main>
    );
  }

  if (profileQuery.isError && !profileQuery.data) {
    return (
      <main className="space-y-4">
        <h1 className="text-xl font-semibold">Jogador {playerId}</h1>
        <section className="rounded-md border border-rose-300 bg-rose-50 p-4 text-sm text-rose-700">
          <p>Falha ao carregar perfil do jogador.</p>
          <p>{profileQuery.error?.message}</p>
        </section>
      </main>
    );
  }

  if (profileQuery.isEmpty || !profileQuery.data) {
    return (
      <main className="space-y-4">
        <h1 className="text-xl font-semibold">Jogador {playerId}</h1>
        <EmptyState
          description="Nao foi possivel encontrar dados para este jogador com os filtros globais atuais."
          title="Perfil indisponivel"
        />
      </main>
    );
  }

  const { player, summary, recentMatches } = profileQuery.data;
  const displayName = player.playerName?.trim().length ? player.playerName : `Jogador ${playerId}`;
  const hasRecentMatches = Boolean(recentMatches && recentMatches.length > 0);

  return (
    <main className="space-y-4">
      <header className="space-y-1">
        <h1 className="text-xl font-semibold">{displayName}</h1>
        <p className="text-sm text-slate-600">
          ID: {player.playerId}
          {player.teamName ? ` | Time: ${player.teamName}` : ""}
          {player.position ? ` | Posicao: ${player.position}` : ""}
        </p>
      </header>

      {profileQuery.isError ? (
        <section className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
          Dados carregados com alerta: {profileQuery.error?.message}
        </section>
      ) : null}

      {profileQuery.isPartial ? <PartialDataBanner coverage={profileQuery.coverage} /> : null}

      <div className="flex items-center gap-2">
        <span className="text-sm text-slate-600">Status de cobertura:</span>
        <CoverageBadge coverage={profileQuery.coverage} />
      </div>

      <section className="space-y-2">
        <h2 className="text-lg font-medium">Resumo</h2>
        <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-4">
          <StatCard title="Partidas" value={summary.matchesPlayed ?? null} />
          <StatCard title="Minutos" value={summary.minutesPlayed ?? null} metricKey="minutes_played" />
          <StatCard title="Gols" value={summary.goals ?? null} metricKey="goals" />
          <StatCard title="Assistencias" value={summary.assists ?? null} metricKey="assists" />
        </div>
      </section>

      <section className="space-y-2">
        <h2 className="text-lg font-medium">Metricas principais</h2>
        <div className="flex flex-wrap gap-2">
          <MetricBadge metricKey="shots_total" value={summary.shotsTotal ?? null} />
          <MetricBadge metricKey="shots_on_target" value={summary.shotsOnTarget ?? null} />
          <MetricBadge metricKey="pass_accuracy_pct" value={summary.passAccuracyPct ?? null} />
          <MetricBadge metricKey="yellow_cards" value={summary.yellowCards ?? null} />
          <MetricBadge metricKey="red_cards" value={summary.redCards ?? null} />
          <MetricBadge metricKey="player_rating" value={summary.rating ?? null} />
        </div>
      </section>

      <section className="space-y-2">
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-lg font-medium">Insights</h2>
          <InsightBadge count={playerInsights.length} highestSeverity={highestPlayerInsightSeverity} />
        </div>

        {playerInsightsQuery.isLoading ? (
          <div className="space-y-2">
            <LoadingSkeleton height={72} />
            <LoadingSkeleton height={72} />
          </div>
        ) : null}

        {!playerInsightsQuery.isLoading && hasFatalPlayerInsightsError ? (
          <section className="rounded-md border border-rose-300 bg-rose-50 p-4 text-sm text-rose-700">
            <p>Falha ao carregar insights do jogador.</p>
            <p>{playerInsightsQuery.error?.message}</p>
          </section>
        ) : null}

        {!playerInsightsQuery.isLoading && !hasFatalPlayerInsightsError ? (
          <>
            {playerInsightsQuery.isPartial ? <PartialDataBanner coverage={playerInsightsQuery.coverage} /> : null}

            <div className="flex items-center gap-2">
              <span className="text-sm text-slate-600">Cobertura de insights:</span>
              <CoverageBadge coverage={playerInsightsQuery.coverage} />
            </div>

            {playerInsightsQuery.isEmpty ? (
              <EmptyState
                description="Nao ha insights para este jogador no recorte atual."
                title="Sem insights do jogador"
              />
            ) : (
              <InsightFeed insights={playerInsights} title="Insights do jogador" />
            )}
          </>
        ) : null}

        {!playerInsightsQuery.isLoading && playerInsightsQuery.isError && playerInsightsQuery.data ? (
          <section className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
            Dados de insights carregados com alerta: {playerInsightsQuery.error?.message}
          </section>
        ) : null}
      </section>

      <section className="space-y-2">
        <h2 className="text-lg font-medium">Ultimos jogos</h2>
        {hasRecentMatches ? (
          <DataTable<PlayerMatchStatsPoint>
            columns={recentMatchesColumns}
            data={recentMatches ?? []}
            emptyDescription="Sem ultimos jogos para o recorte."
            emptyTitle="Sem jogos"
            initialPageSize={5}
            pageSizeOptions={[5, 10, 20]}
          />
        ) : (
          <div className="rounded-md border border-dashed border-slate-300 bg-slate-50 p-4 text-sm text-slate-600">
            Historico de ultimos jogos indisponivel no payload atual.
          </div>
        )}
      </section>

    </main>
  );
}
