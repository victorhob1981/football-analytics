"use client";

import Link from "next/link";

import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { formatNumber, formatPercentage } from "@/shared/utils/formatters";
import { useTopPlayers } from "@/features/home/hooks";
import type { TopPlayerItem } from "@/features/home/types";

function resolveOptionalValue(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value) || !Number.isFinite(value)) {
    return "-";
  }

  return formatNumber(value);
}

function resolveVariation(player: TopPlayerItem): { label: string; className: string } {
  const variationValue = player.variationValue;
  const variationPct = player.variationPct;

  if (
    variationValue === null ||
    variationValue === undefined ||
    Number.isNaN(variationValue) ||
    !Number.isFinite(variationValue)
  ) {
    return {
      label: "-",
      className: "text-slate-500",
    };
  }

  const variationLabel = `${variationValue > 0 ? "+" : ""}${formatNumber(variationValue, 0)}`;
  const variationPctLabel =
    variationPct !== null &&
    variationPct !== undefined &&
    Number.isFinite(variationPct) &&
    !Number.isNaN(variationPct)
      ? ` (${variationPct > 0 ? "+" : ""}${formatPercentage(variationPct, 1)})`
      : "";

  return {
    label: `${variationLabel}${variationPctLabel}`,
    className:
      variationValue > 0 ? "text-emerald-300" : variationValue < 0 ? "text-red-400" : "text-slate-300",
  };
}

function PlayerRow({ player }: { player: TopPlayerItem }) {
  const goals = player.goals ?? 0;
  const assists = player.assists ?? 0;
  const goalAssist = goals + assists;
  const variation = resolveVariation(player);

  return (
    <tr className="border-b border-slate-700/80 transition-colors hover:bg-slate-800/60">
      <td className="px-4 py-3">
        <div>
          <Link
            className="block truncate text-sm font-medium text-slate-100 no-underline hover:text-emerald-300"
            href={`/players/${player.playerId}`}
          >
            {player.playerName}
          </Link>
          {player.teamName ? <p className="text-xs text-slate-400">{player.teamName}</p> : null}
        </div>
      </td>
      <td className="px-4 py-3 text-center">
        <span className="inline-flex h-8 w-8 items-center justify-center rounded-full bg-emerald-900/70 font-semibold text-emerald-300">
          {resolveOptionalValue(player.goals)}
        </span>
      </td>
      <td className="px-4 py-3 text-center">
        <span className="inline-flex h-8 w-8 items-center justify-center rounded-full bg-sky-900/70 font-semibold text-sky-300">
          {resolveOptionalValue(player.assists)}
        </span>
      </td>
      <td className="px-4 py-3 text-center text-sm font-medium text-slate-100">
        {player.rating != null ? formatNumber(player.rating, 2) : "-"}
      </td>
      <td className="px-4 py-3 text-center text-sm font-semibold text-amber-300">{formatNumber(goalAssist)}</td>
      <td className={`px-4 py-3 text-center text-xs font-semibold ${variation.className}`}>{variation.label}</td>
    </tr>
  );
}

function TableSkeleton() {
  return (
    <div className="rounded-lg border border-slate-700 bg-slate-900">
      <div className="border-b border-slate-700 px-4 py-3">
        <LoadingSkeleton className="bg-slate-700" height={14} width="30%" />
      </div>
      <div className="space-y-2 px-4 py-3">
        {Array.from({ length: 6 }).map((_, i) => (
          <LoadingSkeleton className="bg-slate-700" height={42} key={i} />
        ))}
      </div>
    </div>
  );
}

export function TopPlayersSection() {
  const { data, isLoading, isEmpty } = useTopPlayers();

  if (isLoading) {
    return <TableSkeleton />;
  }

  if (isEmpty || !data || data.items.length === 0) {
    return (
      <EmptyState
        className="rounded-xl border-slate-700 bg-slate-900 [&_h3]:text-slate-100 [&_p]:text-slate-400"
        description="Nenhum dado de jogadores disponivel para o recorte atual."
        title="Sem dados de jogadores"
      />
    );
  }

  return (
    <div className="overflow-hidden rounded-lg border border-slate-700 bg-slate-900/70">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-700 bg-slate-800/80 text-slate-300">
              <th className="px-4 py-3 text-left font-semibold">Jogador</th>
              <th className="px-4 py-3 text-center font-semibold">Gols</th>
              <th className="px-4 py-3 text-center font-semibold">Assist.</th>
              <th className="px-4 py-3 text-center font-semibold">Rating</th>
              <th className="px-4 py-3 text-center font-semibold">G+A</th>
              <th className="px-4 py-3 text-center font-semibold">Var. vs ant.</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((player) => (
              <PlayerRow key={player.playerId} player={player} />
            ))}
          </tbody>
        </table>
      </div>
      <div className="border-t border-slate-700 px-4 py-2 text-right">
        <Link
          className="text-xs font-medium text-emerald-300 no-underline hover:text-emerald-200 hover:underline"
          href="/players"
        >
          Ver todos os jogadores
        </Link>
      </div>
    </div>
  );
}
