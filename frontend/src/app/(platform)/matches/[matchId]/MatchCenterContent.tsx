"use client";

import { useMatchCenter } from "@/features/matches/hooks";
import {
  MatchCenterHeader,
  MatchLineupsPlaceholder,
  MatchPlayerStatsPlaceholder,
  MatchTimelinePlaceholder,
} from "@/features/matches/components";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";

type MatchCenterContentProps = {
  matchId: string;
};

export function MatchCenterContent({ matchId }: MatchCenterContentProps) {
  const matchCenterQuery = useMatchCenter(matchId, {
    includeTimeline: true,
    includeLineups: true,
    includePlayerStats: true,
  });

  if (matchCenterQuery.isLoading) {
    return (
      <main className="space-y-4">
        <header className="space-y-1">
          <h1 className="text-xl font-semibold">Match Center</h1>
          <p className="text-sm text-slate-600">Carregando partida {matchId}...</p>
        </header>

        <LoadingSkeleton height={120} />
        <LoadingSkeleton height={160} />
        <LoadingSkeleton height={160} />
        <LoadingSkeleton height={160} />
      </main>
    );
  }

  if (matchCenterQuery.isError && !matchCenterQuery.data) {
    return (
      <main className="space-y-4">
        <h1 className="text-xl font-semibold">Match Center</h1>
        <section className="rounded-md border border-rose-300 bg-rose-50 p-4 text-sm text-rose-700">
          <p>Falha ao carregar dados da partida.</p>
          <p>{matchCenterQuery.error?.message}</p>
        </section>
      </main>
    );
  }

  if (matchCenterQuery.isEmpty || !matchCenterQuery.data) {
    return (
      <main className="space-y-4">
        <h1 className="text-xl font-semibold">Match Center</h1>
        <EmptyState
          description="Nao foi possivel encontrar dados para esta partida com os filtros globais atuais."
          title="Partida indisponivel"
        />
      </main>
    );
  }

  const { match, timeline, lineups, playerStats } = matchCenterQuery.data;

  return (
    <main className="space-y-4">
      <MatchCenterHeader match={match} />

      {matchCenterQuery.isError ? (
        <section className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
          Dados carregados com alerta: {matchCenterQuery.error?.message}
        </section>
      ) : null}

      {matchCenterQuery.isPartial ? <PartialDataBanner coverage={matchCenterQuery.coverage} /> : null}

      <div className="flex items-center gap-2">
        <span className="text-sm text-slate-600">Status de cobertura:</span>
        <CoverageBadge coverage={matchCenterQuery.coverage} />
      </div>

      <MatchTimelinePlaceholder events={timeline} />
      <MatchLineupsPlaceholder lineups={lineups} />
      <MatchPlayerStatsPlaceholder playerStats={playerStats} />
    </main>
  );
}
