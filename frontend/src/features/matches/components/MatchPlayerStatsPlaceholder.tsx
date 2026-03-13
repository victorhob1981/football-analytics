import { useMemo, useState } from "react";

import Link from "next/link";

import type { MatchPlayerStat } from "@/features/matches/types";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";

type MatchPlayerStatsPlaceholderProps = {
  playerStats: MatchPlayerStat[] | undefined;
  homeTeamId?: string | null;
  homeTeamName?: string | null;
  awayTeamName?: string | null;
};

type SortDirection = "asc" | "desc";
type SortKey =
  | "playerName"
  | "minutesPlayed"
  | "goals"
  | "assists"
  | "shotsTotal"
  | "keyPasses"
  | "tackles"
  | "interceptions"
  | "duels"
  | "rating";

type ColumnDefinition = {
  key: SortKey;
  label: string;
  withCoverage?: boolean;
};

const COLUMNS: ColumnDefinition[] = [
  { key: "playerName", label: "Jogador" },
  { key: "minutesPlayed", label: "Min" },
  { key: "goals", label: "Gols" },
  { key: "assists", label: "Assist." },
  { key: "shotsTotal", label: "Chutes" },
  { key: "keyPasses", label: "Passes-chave", withCoverage: true },
  { key: "tackles", label: "Desarmes", withCoverage: true },
  { key: "interceptions", label: "Intercept.", withCoverage: true },
  { key: "duels", label: "Duelos", withCoverage: true },
  { key: "rating", label: "Rating", withCoverage: true },
];

function toCoverage(availableCount: number, totalCount: number, label: string) {
  const percentage = totalCount > 0 ? Number(((availableCount / totalCount) * 100).toFixed(1)) : undefined;

  if (totalCount === 0) {
    return { status: "unknown" as const, label };
  }

  if (availableCount === 0) {
    return { status: "empty" as const, percentage, label };
  }

  if (availableCount < totalCount) {
    return { status: "partial" as const, percentage, label };
  }

  return { status: "complete" as const, percentage, label };
}

function compareNumber(left: number | null | undefined, right: number | null | undefined): number {
  const leftValid = typeof left === "number" && !Number.isNaN(left);
  const rightValid = typeof right === "number" && !Number.isNaN(right);

  if (!leftValid && !rightValid) return 0;
  if (!leftValid) return 1;
  if (!rightValid) return -1;

  return left - right;
}

function formatValue(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }

  return Number.isInteger(value) ? String(value) : value.toFixed(1);
}

export function MatchPlayerStatsPlaceholder({
  playerStats,
  homeTeamId,
  homeTeamName,
  awayTeamName,
}: MatchPlayerStatsPlaceholderProps) {
  const statsItems = useMemo(() => playerStats ?? [], [playerStats]);
  const [tab, setTab] = useState<"home" | "away">("home");
  const [sortKey, setSortKey] = useState<SortKey>("rating");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");

  const filteredStats = useMemo(
    () =>
      statsItems.filter((player) =>
        tab === "home" ? player.teamId === homeTeamId : player.teamId !== homeTeamId,
      ),
    [homeTeamId, statsItems, tab],
  );

  const sortedStats = useMemo(() => {
    const rows = [...filteredStats].sort((left, right) => {
      if (sortKey === "playerName") {
        return (left.playerName ?? "").localeCompare(right.playerName ?? "", "pt-BR");
      }

      return compareNumber(
        left[sortKey] as number | null | undefined,
        right[sortKey] as number | null | undefined,
      );
    });

    return sortDirection === "desc" ? rows.reverse() : rows;
  }, [filteredStats, sortDirection, sortKey]);

  const coverageByColumn = useMemo(() => {
    const entries = COLUMNS.filter((column) => column.withCoverage).map((column) => {
      const availableCount = filteredStats.reduce((acc, row) => {
        const value = row[column.key] as number | null | undefined;
        return acc + Number(typeof value === "number" && !Number.isNaN(value));
      }, 0);

      return [
        column.key,
        toCoverage(availableCount, filteredStats.length, `Cobertura ${column.label.toLowerCase()}`),
      ] as const;
    });

    return Object.fromEntries(entries) as Record<SortKey, ReturnType<typeof toCoverage>>;
  }, [filteredStats]);

  function toggleSort(columnKey: SortKey) {
    if (columnKey === sortKey) {
      setSortDirection((current) => (current === "asc" ? "desc" : "asc"));
      return;
    }

    setSortKey(columnKey);
    setSortDirection(columnKey === "playerName" ? "asc" : "desc");
  }

  return (
    <section className="space-y-3 rounded-md border border-slate-200 bg-white p-4">
      <h2 className="text-lg font-medium text-slate-900">Player Stats</h2>

      {statsItems.length === 0 ? (
        <p className="rounded border border-dashed border-slate-300 bg-slate-50 p-3 text-sm text-slate-600">
          Estatisticas individuais indisponiveis para esta partida.
        </p>
      ) : (
        <>
          <div className="flex gap-2">
            <button
              className={`rounded border px-3 py-1.5 text-sm ${
                tab === "home" ? "border-slate-900 bg-slate-900 text-white" : "border-slate-300 text-slate-700"
              }`}
              onClick={() => {
                setTab("home");
              }}
              type="button"
            >
              {homeTeamName ?? "Mandante"}
            </button>
            <button
              className={`rounded border px-3 py-1.5 text-sm ${
                tab === "away" ? "border-slate-900 bg-slate-900 text-white" : "border-slate-300 text-slate-700"
              }`}
              onClick={() => {
                setTab("away");
              }}
              type="button"
            >
              {awayTeamName ?? "Visitante"}
            </button>
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b border-slate-200 text-left text-xs uppercase text-slate-500">
                  {COLUMNS.map((column) => {
                    const coverage = coverageByColumn[column.key];
                    const shouldShowCoverage = column.withCoverage && coverage && coverage.status !== "complete";

                    return (
                      <th className="px-2 py-2" key={column.key}>
                        <div className="flex min-w-max items-center gap-2">
                          <button
                            className="rounded border border-transparent px-1 py-0.5 hover:border-slate-300"
                            onClick={() => {
                              toggleSort(column.key);
                            }}
                            type="button"
                          >
                            {column.label}
                          </button>
                          {shouldShowCoverage ? <CoverageBadge coverage={coverage} /> : null}
                        </div>
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {sortedStats.map((playerStat, index) => (
                  <tr key={`${playerStat.playerId ?? `player-${index}`}-${playerStat.teamId ?? "team-unknown"}`}>
                    <td className="px-2 py-2">
                      {playerStat.playerId ? (
                        <Link className="font-medium text-slate-900 hover:underline" href={`/players/${playerStat.playerId}`}>
                          {playerStat.playerName ?? "Jogador sem nome"}
                        </Link>
                      ) : (
                        <span className="font-medium text-slate-700">{playerStat.playerName ?? "Jogador sem nome"}</span>
                      )}
                    </td>
                    <td className="px-2 py-2 text-slate-700">{formatValue(playerStat.minutesPlayed)}</td>
                    <td className="px-2 py-2 text-slate-700">{formatValue(playerStat.goals)}</td>
                    <td className="px-2 py-2 text-slate-700">{formatValue(playerStat.assists)}</td>
                    <td className="px-2 py-2 text-slate-700">{formatValue(playerStat.shotsTotal)}</td>
                    <td className="px-2 py-2 text-slate-700">{formatValue(playerStat.keyPasses)}</td>
                    <td className="px-2 py-2 text-slate-700">{formatValue(playerStat.tackles)}</td>
                    <td className="px-2 py-2 text-slate-700">{formatValue(playerStat.interceptions)}</td>
                    <td className="px-2 py-2 text-slate-700">{formatValue(playerStat.duels)}</td>
                    <td className="px-2 py-2 text-slate-700">{formatValue(playerStat.rating)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {sortedStats.length === 0 ? (
            <p className="rounded border border-dashed border-slate-300 bg-slate-50 p-3 text-sm text-slate-600">
              Sem dados de jogadores para o time selecionado.
            </p>
          ) : null}
        </>
      )}
    </section>
  );
}
