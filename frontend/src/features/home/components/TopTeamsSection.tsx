"use client";

import Link from "next/link";

import { SparklineChart } from "@/shared/components/charts/SparklineChart";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { formatNumber } from "@/shared/utils/formatters";
import { useTopTeams } from "@/features/home/hooks";
import type { TeamStrengthItem } from "@/features/home/types";

function TeamRow({
  team,
  rank,
  metricLabel,
  color,
}: {
  team: TeamStrengthItem;
  rank: number;
  metricLabel: string;
  color: string;
}) {
  return (
    <li className="flex items-center gap-3 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 transition-colors hover:bg-slate-800">
      <span className="w-5 text-center text-xs font-semibold text-slate-500">{rank}</span>
      <Link
        className="min-w-0 flex-1 truncate text-sm font-medium text-slate-100 no-underline hover:text-emerald-300"
        href={`/clubs/${team.teamId}`}
      >
        {team.teamName}
      </Link>
      {team.trend && team.trend.length > 1 ? (
        <div className="w-16 shrink-0">
          <SparklineChart
            color={color}
            data={team.trend}
            dataKey="value"
            height={24}
            showTooltip={false}
          />
        </div>
      ) : null}
      <span className="shrink-0 text-base font-semibold text-slate-100">{formatNumber(team.value)}</span>
      <span className="hidden shrink-0 text-xs text-slate-400 sm:block">{metricLabel}</span>
    </li>
  );
}

function ColumnSkeleton() {
  return (
    <div className="space-y-3">
      <LoadingSkeleton className="bg-slate-700" height={16} width="40%" />
      {Array.from({ length: 5 }).map((_, i) => (
        <LoadingSkeleton className="bg-slate-700" height={44} key={i} />
      ))}
    </div>
  );
}

export function TopTeamsSection() {
  const { data, isLoading, isEmpty } = useTopTeams();

  if (isLoading) {
    return (
      <div className="grid gap-6 sm:grid-cols-2">
        <ColumnSkeleton />
        <ColumnSkeleton />
      </div>
    );
  }

  if (isEmpty || !data) {
    return (
      <EmptyState
        className="rounded-xl border-slate-700 bg-slate-900 [&_h3]:text-slate-100 [&_p]:text-slate-400"
        description="Dados de ataques e defesas nao disponiveis para o recorte atual."
        title="Sem dados de times"
      />
    );
  }

  return (
    <div className="grid gap-6 sm:grid-cols-2">
      <div className="space-y-2">
        <h3 className="flex items-center gap-2 text-sm font-semibold text-slate-200">
          <span className="inline-block h-2.5 w-2.5 rounded-full bg-emerald-400" />
          Melhores Ataques
        </h3>
        <ul className="space-y-2">
          {data.topAttacks.map((team, i) => (
            <TeamRow
              color="#10B981"
              key={team.teamId}
              metricLabel="gols"
              rank={i + 1}
              team={team}
            />
          ))}
        </ul>
      </div>

      <div className="space-y-2">
        <h3 className="flex items-center gap-2 text-sm font-semibold text-slate-200">
          <span className="inline-block h-2.5 w-2.5 rounded-full bg-sky-400" />
          Melhores Defesas
        </h3>
        <ul className="space-y-2">
          {data.topDefenses.map((team, i) => (
            <TeamRow
              color="#1E40AF"
              key={team.teamId}
              metricLabel="sofridos"
              rank={i + 1}
              team={team}
            />
          ))}
        </ul>
      </div>
    </div>
  );
}
