"use client";

import { Target, Trophy, TrendingUp, Users, type LucideIcon } from "lucide-react";

import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { formatNumber } from "@/shared/utils/formatters";
import { useLeagueKpi } from "@/features/home/hooks";

type KpiCardProps = {
  title: string;
  value: number | null | undefined;
  icon: LucideIcon;
  accent: string;
  precision?: number;
};

function resolveValue(value: number | null | undefined, precision = 0): string {
  if (value === null || value === undefined || Number.isNaN(value) || !Number.isFinite(value)) {
    return "-";
  }

  return formatNumber(value, precision);
}

function KpiCard({ title, value, icon: Icon, accent, precision = 0 }: KpiCardProps) {
  return (
    <article className="stat-card hover-lift" style={{ borderLeftColor: accent }}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-sm font-medium text-slate-400">{title}</p>
          <p className="mt-2 text-3xl font-semibold text-slate-100">{resolveValue(value, precision)}</p>
        </div>
        <div className="rounded-lg p-2" style={{ backgroundColor: `${accent}1f` }}>
          <Icon className="h-6 w-6" color={accent} />
        </div>
      </div>
    </article>
  );
}

function KpiSkeletons() {
  return (
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      {Array.from({ length: 4 }).map((_, i) => (
        <div className="stat-card" key={i}>
          <div className="flex items-start justify-between gap-3">
            <div className="flex-1 space-y-3">
              <LoadingSkeleton className="bg-slate-700" height={14} width="45%" />
              <LoadingSkeleton className="bg-slate-700" height={34} width="40%" />
            </div>
            <LoadingSkeleton className="bg-slate-700" height={40} rounded="md" width={40} />
          </div>
        </div>
      ))}
    </div>
  );
}

export function HomeKpiSection() {
  const { data, isLoading, isEmpty } = useLeagueKpi();

  if (isLoading) {
    return <KpiSkeletons />;
  }

  if (isEmpty || !data) {
    return (
      <EmptyState
        className="rounded-xl border-slate-700 bg-slate-900 [&_h3]:text-slate-100 [&_p]:text-slate-400"
        description="Nenhum KPI disponivel para o recorte atual."
        title="Sem dados de temporada"
      />
    );
  }

  return (
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
      <KpiCard accent="#10B981" icon={Trophy} title="Partidas Disputadas" value={data.totalMatches} />
      <KpiCard accent="#1E40AF" icon={Target} title="Gols Marcados" value={data.totalGoals} />
      <KpiCard
        accent="#F97316"
        icon={TrendingUp}
        precision={2}
        title="Media de Gols/Jogo"
        value={data.avgGoalsPerMatch}
      />
      <KpiCard accent="#06B6D4" icon={Users} title="Times Participantes" value={data.totalTeams} />
    </div>
  );
}
