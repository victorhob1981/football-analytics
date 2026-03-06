"use client";

import { useCallback, useMemo, useState } from "react";

import { Filter, GitCompare, Search, Star, Timer, Users } from "lucide-react";
import Link from "next/link";

import { useQueryClient } from "@tanstack/react-query";

import { formatMetricValue } from "@/config/metrics.registry";
import { usePlayersList } from "@/features/players/hooks";
import { playersQueryKeys } from "@/features/players/queryKeys";
import { fetchPlayerProfile } from "@/features/players/services/players.service";
import type { PlayerListItem, PlayerProfileFilters } from "@/features/players/types";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import { useComparisonStore } from "@/shared/stores/comparison.store";

// ── Helpers ───────────────────────────────────────────────────────────────────

const POSITION_BADGE: Record<string, string> = {
  Goalkeeper: "bg-amber-500/20 text-amber-300",
  Defender: "bg-sky-500/20 text-sky-300",
  Midfielder: "bg-emerald-500/20 text-emerald-300",
  Forward: "bg-red-500/20 text-red-400",
  Attacker: "bg-red-500/20 text-red-400",
};
const POSITION_ABBR: Record<string, string> = {
  Goalkeeper: "GK", Defender: "DEF", Midfielder: "MID",
  Forward: "ATK", Attacker: "ATK",
};

function positionBadge(position?: string | null) {
  const cls = POSITION_BADGE[position ?? ""] ?? "bg-slate-700 text-slate-400";
  const abbr = POSITION_ABBR[position ?? ""] ?? (position?.slice(0, 3).toUpperCase() ?? "—");
  return (
    <span className={`rounded px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wide ${cls}`}>{abbr}</span>
  );
}

function parseMinMinutes(value: string): number | null {
  const v = value.trim();
  if (!v) return null;
  const n = Number.parseInt(v, 10);
  return Number.isInteger(n) && n >= 0 ? n : null;
}

// ── Card de jogador ───────────────────────────────────────────────────────────

function PlayerCard({
  player,
  isSelected,
  isDisabled,
  onPrefetch,
  onCompare,
}: {
  player: PlayerListItem;
  isSelected: boolean;
  isDisabled: boolean;
  onPrefetch: (id: string) => void;
  onCompare: (id: string) => void;
}) {
  return (
    <article
      className={`group rounded-xl border p-4 shadow-sm transition-all ${isSelected
          ? "border-emerald-500/60 bg-emerald-900/15 ring-1 ring-emerald-500/30"
          : "border-slate-700/80 bg-slate-900/80 hover:border-slate-600 hover:bg-slate-900"
        }`}
    >
      {/* Header: posição + nome + clube */}
      <div className="mb-3 flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="mb-1 flex items-center gap-2">
            {positionBadge(player.position)}
            <span className="text-xs text-slate-500">{player.teamName ?? "—"}</span>
          </div>
          <Link
            className="block truncate text-sm font-semibold text-slate-100 no-underline transition-colors group-hover:text-emerald-300"
            href={`/players/${player.playerId}`}
            onFocus={() => { onPrefetch(player.playerId); }}
            onMouseEnter={() => { onPrefetch(player.playerId); }}
          >
            {player.playerName ?? "Jogador sem nome"}
          </Link>
          {player.nationality && (
            <p className="mt-0.5 text-[10px] text-slate-600">{player.nationality}</p>
          )}
        </div>
      </div>

      {/* Stats mini-grid */}
      <div className="mb-3 grid grid-cols-3 gap-1.5">
        <div className="rounded-md bg-slate-800/60 p-2 text-center">
          <p className="text-sm font-bold text-emerald-300">{formatMetricValue("goals", player.goals)}</p>
          <p className="text-[9px] text-slate-500">Gols</p>
        </div>
        <div className="rounded-md bg-slate-800/60 p-2 text-center">
          <p className="text-sm font-bold text-sky-300">{formatMetricValue("assists", player.assists)}</p>
          <p className="text-[9px] text-slate-500">Assist.</p>
        </div>
        <div className="rounded-md bg-slate-800/60 p-2 text-center">
          {player.rating !== null && player.rating !== undefined ? (
            <>
              <p className="flex items-center justify-center gap-0.5 text-sm font-bold text-amber-300">
                <Star className="h-2.5 w-2.5" />
                {formatMetricValue("player_rating", player.rating)}
              </p>
              <p className="text-[9px] text-slate-500">Rating</p>
            </>
          ) : (
            <>
              <p className="text-sm font-bold text-slate-600">—</p>
              <p className="text-[9px] text-slate-500">Rating</p>
            </>
          )}
        </div>
      </div>

      {/* Footer: minutos + botão comparar */}
      <div className="flex items-center justify-between border-t border-slate-700/50 pt-2.5">
        <span className="flex items-center gap-1 text-[10px] text-slate-500">
          <Timer className="h-3 w-3" />
          {formatMetricValue("minutes_played", player.minutesPlayed)} min
        </span>
        <button
          className={`rounded-md px-2.5 py-1 text-[10px] font-semibold transition-colors disabled:cursor-not-allowed disabled:opacity-40 ${isSelected
              ? "bg-emerald-500/20 text-emerald-300 hover:bg-emerald-500/30"
              : "border border-slate-700 bg-slate-800 text-slate-400 hover:border-emerald-500/60 hover:text-emerald-300"
            }`}
          disabled={isDisabled}
          onClick={() => { onCompare(player.playerId); }}
          type="button"
        >
          {isSelected ? "✓ Selecionado" : "Comparar"}
        </button>
      </div>
    </article>
  );
}

// ── Skeleton ──────────────────────────────────────────────────────────────────

function CardSkeleton() {
  return (
    <div className="h-40 animate-pulse rounded-xl border border-slate-700/80 bg-slate-900/80 p-4">
      <div className="mb-3 flex gap-2">
        <div className="h-4 w-8 rounded bg-slate-700" />
        <div className="h-4 w-20 rounded bg-slate-700" />
      </div>
      <div className="mb-2 h-5 w-32 rounded bg-slate-700" />
      <div className="mb-3 grid grid-cols-3 gap-1.5">
        {[0, 1, 2].map((i) => <div className="h-10 rounded-md bg-slate-800" key={i} />)}
      </div>
    </div>
  );
}

// ── Página principal ──────────────────────────────────────────────────────────

export default function PlayersPage() {
  const [search, setSearch] = useState("");
  const [minMinutesInput, setMinMinutesInput] = useState("");
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize] = useState(24);

  const queryClient = useQueryClient();
  const { competitionId, seasonId, venue } = useGlobalFiltersState();
  const { params: timeRangeParams } = useTimeRange();

  const comparisonEntityType = useComparisonStore((s) => s.entityType);
  const selectedIds = useComparisonStore((s) => s.selectedIds);
  const addToComparison = useComparisonStore((s) => s.add);
  const removeFromComparison = useComparisonStore((s) => s.remove);
  const setComparisonEntityType = useComparisonStore((s) => s.setEntityType);

  const normalizedMinMinutes = useMemo(() => parseMinMinutes(minMinutesInput), [minMinutesInput]);
  const selectedIdsSet = useMemo(() => new Set(selectedIds), [selectedIds]);

  const playersQuery = usePlayersList({
    search,
    minMinutes: normalizedMinMinutes,
    page: pageIndex + 1,
    pageSize,
  });

  const detailPrefetchFilters = useMemo<PlayerProfileFilters>(
    () => ({
      competitionId, seasonId,
      roundId: timeRangeParams.roundId,
      monthKey: timeRangeParams.monthKey,
      venue,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
      includeRecentMatches: true,
    }),
    [competitionId, seasonId, timeRangeParams.dateRangeEnd, timeRangeParams.dateRangeStart,
      timeRangeParams.lastN, timeRangeParams.monthKey, timeRangeParams.roundId, venue],
  );

  const prefetchPlayerDetail = useCallback((playerId: string) => {
    const id = playerId.trim();
    if (!id) return;
    void queryClient.prefetchQuery({
      queryKey: playersQueryKeys.profile(id, detailPrefetchFilters),
      queryFn: () => fetchPlayerProfile(id, detailPrefetchFilters),
      staleTime: 5 * 60 * 1000,
    });
  }, [detailPrefetchFilters, queryClient]);

  const handleCompareAction = useCallback((playerId: string) => {
    if (comparisonEntityType !== "player") setComparisonEntityType("player");
    if (selectedIdsSet.has(playerId)) {
      removeFromComparison(playerId);
    } else {
      addToComparison(playerId);
    }
  }, [addToComparison, comparisonEntityType, removeFromComparison, selectedIdsSet, setComparisonEntityType]);

  const tableData = useMemo(() => playersQuery.data?.items ?? [], [playersQuery.data?.items]);
  const totalCount = playersQuery.pagination?.totalCount ?? tableData.length;
  const totalPages = Math.ceil(totalCount / pageSize);

  return (
    <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
      <div className="mx-auto max-w-7xl space-y-6">

        {/* Header */}
        <header className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-slate-100">Jogadores</h1>
            <p className="mt-1 text-sm text-slate-400">
              {totalCount > 0 ? `${totalCount} jogadores encontrados` : "Scouting — filtre e encontre jogadores por métricas"}
            </p>
          </div>
          {selectedIds.length > 0 && comparisonEntityType === "player" && (
            <Link
              className="flex items-center gap-2 rounded-xl bg-emerald-600 px-4 py-2 text-sm font-semibold text-white no-underline transition-colors hover:bg-emerald-700"
              href="/players/compare"
            >
              <GitCompare className="h-4 w-4" />
              Comparar ({selectedIds.length}/2)
            </Link>
          )}
        </header>

        {/* Filtros locais */}
        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-4 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
          <div className="flex flex-wrap items-end gap-4">
            {/* Busca */}
            <label className="flex flex-1 flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
              <span className="flex items-center gap-1.5"><Search className="h-3 w-3" /> Buscar jogador</span>
              <input
                className="w-full min-w-[200px] rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 placeholder:text-slate-500 focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
                onChange={(e) => { setSearch(e.target.value); setPageIndex(0); }}
                placeholder="Ex.: Arrascaeta"
                type="text"
                value={search}
              />
            </label>

            {/* Mínimo de minutos */}
            <label className="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
              <span className="flex items-center gap-1.5"><Timer className="h-3 w-3" /> Mín. minutos</span>
              <input
                className="w-32 rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 placeholder:text-slate-500 focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
                min={0}
                onChange={(e) => { setMinMinutesInput(e.target.value); setPageIndex(0); }}
                placeholder="Ex.: 300"
                type="number"
                value={minMinutesInput}
              />
            </label>

            {/* Filtro de posição (visual, client-side) */}
            <label className="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
              <span className="flex items-center gap-1.5"><Filter className="h-3 w-3" /> Posição</span>
              <select
                className="rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
                disabled
                title="Filtro por posição em breve"
              >
                <option value="">Todas</option>
              </select>
            </label>
          </div>
        </section>

        {/* Banners */}
        {playersQuery.isPartial && <PartialDataBanner coverage={playersQuery.coverage} />}
        {playersQuery.isError && tableData.length === 0 && (
          <section className="rounded-xl border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-400">
            <p className="font-medium">Falha ao carregar lista de jogadores.</p>
            <p className="mt-1 opacity-70">{playersQuery.error?.message}</p>
          </section>
        )}
        {!playersQuery.isLoading && (
          <div className="flex items-center gap-2">
            <span className="text-xs text-slate-500">Cobertura:</span>
            <CoverageBadge coverage={playersQuery.coverage} />
          </div>
        )}

        {/* Grid de cards */}
        {playersQuery.isLoading ? (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            {Array.from({ length: 12 }).map((_, i) => <CardSkeleton key={i} />)}
          </div>
        ) : tableData.length === 0 ? (
          <section className="rounded-xl border border-slate-700 bg-slate-900 p-10 text-center">
            <Users className="mx-auto mb-3 h-8 w-8 text-slate-600" />
            <p className="text-sm text-slate-400">Nenhum jogador encontrado para os filtros atuais.</p>
          </section>
        ) : (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            {tableData.map((player) => (
              <PlayerCard
                isDisabled={!selectedIdsSet.has(player.playerId) && selectedIds.length >= 2}
                isSelected={selectedIdsSet.has(player.playerId)}
                key={player.playerId}
                onCompare={handleCompareAction}
                onPrefetch={prefetchPlayerDetail}
                player={player}
              />
            ))}
          </div>
        )}

        {/* Paginação */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between rounded-xl border border-slate-700/80 bg-slate-900/80 px-5 py-3 backdrop-blur-sm">
            <button
              className="rounded-md border border-slate-600 bg-slate-800 px-3 py-1.5 text-xs font-medium text-slate-300 transition-colors hover:border-emerald-500/60 hover:text-emerald-300 disabled:opacity-40"
              disabled={pageIndex === 0}
              onClick={() => { setPageIndex((p) => p - 1); }}
              type="button"
            >
              ← Anterior
            </button>
            <span className="text-xs text-slate-400">Página {pageIndex + 1} de {totalPages}</span>
            <button
              className="rounded-md border border-slate-600 bg-slate-800 px-3 py-1.5 text-xs font-medium text-slate-300 transition-colors hover:border-emerald-500/60 hover:text-emerald-300 disabled:opacity-40"
              disabled={pageIndex + 1 >= totalPages}
              onClick={() => { setPageIndex((p) => p + 1); }}
              type="button"
            >
              Próxima →
            </button>
          </div>
        )}

      </div>
    </div>
  );
}
