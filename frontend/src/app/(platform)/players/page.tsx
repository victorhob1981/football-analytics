"use client";

import { useCallback, useMemo, useState } from "react";

import Link from "next/link";

import { useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";

import { usePlayersList } from "@/features/players/hooks";
import { playersQueryKeys } from "@/features/players/queryKeys";
import { fetchPlayerProfile } from "@/features/players/services/players.service";
import type { PlayerListItem, PlayerProfileFilters } from "@/features/players/types";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { DataTable } from "@/shared/components/data-display/DataTable";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import { useComparisonStore } from "@/shared/stores/comparison.store";
import { formatMetricValue } from "@/config/metrics.registry";

function parseMinMinutes(value: string): number | null {
  const normalizedValue = value.trim();

  if (normalizedValue.length === 0) {
    return null;
  }

  const parsedValue = Number.parseInt(normalizedValue, 10);

  if (!Number.isInteger(parsedValue) || parsedValue < 0) {
    return null;
  }

  return parsedValue;
}

export default function PlayersPage() {
  const [search, setSearch] = useState("");
  const [minMinutesInput, setMinMinutesInput] = useState("");
  const queryClient = useQueryClient();
  const { competitionId, seasonId, venue } = useGlobalFiltersState();
  const { params: timeRangeParams } = useTimeRange();
  const comparisonEntityType = useComparisonStore((state) => state.entityType);
  const selectedIds = useComparisonStore((state) => state.selectedIds);
  const addToComparison = useComparisonStore((state) => state.add);
  const removeFromComparison = useComparisonStore((state) => state.remove);
  const setComparisonEntityType = useComparisonStore((state) => state.setEntityType);

  const normalizedMinMinutes = useMemo(() => parseMinMinutes(minMinutesInput), [minMinutesInput]);
  const selectedIdsSet = useMemo(() => new Set(selectedIds), [selectedIds]);

  const playersQuery = usePlayersList({
    search,
    minMinutes: normalizedMinMinutes,
  });

  const detailPrefetchFilters = useMemo<PlayerProfileFilters>(
    () => ({
      competitionId,
      seasonId,
      roundId: timeRangeParams.roundId,
      venue,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
      includeRecentMatches: true,
    }),
    [
      competitionId,
      seasonId,
      timeRangeParams.dateRangeEnd,
      timeRangeParams.dateRangeStart,
      timeRangeParams.lastN,
      timeRangeParams.roundId,
      venue,
    ],
  );

  const prefetchPlayerDetail = useCallback(
    (playerId: string) => {
      const normalizedPlayerId = playerId.trim();

      if (normalizedPlayerId.length === 0) {
        return;
      }

      void queryClient.prefetchQuery({
        queryKey: playersQueryKeys.profile(normalizedPlayerId, detailPrefetchFilters),
        queryFn: () => fetchPlayerProfile(normalizedPlayerId, detailPrefetchFilters),
        staleTime: 5 * 60 * 1000,
      });
    },
    [detailPrefetchFilters, queryClient],
  );

  const handleCompareAction = useCallback(
    (playerId: string) => {
      if (comparisonEntityType !== "player") {
        setComparisonEntityType("player");
      }

      if (selectedIdsSet.has(playerId)) {
        removeFromComparison(playerId);
        return;
      }

      addToComparison(playerId);
    },
    [addToComparison, comparisonEntityType, removeFromComparison, selectedIdsSet, setComparisonEntityType],
  );

  const tableData = useMemo(() => {
    const items = playersQuery.data?.items ?? [];
    const normalizedSearch = search.trim().toLowerCase();

    return items.filter((item) => {
      const playerName = item.playerName?.toLowerCase() ?? "";
      const passesSearchFilter = normalizedSearch.length === 0 || playerName.includes(normalizedSearch);
      const playerMinutes = item.minutesPlayed ?? 0;
      const passesMinMinutesFilter = normalizedMinMinutes === null || playerMinutes >= normalizedMinMinutes;

      return passesSearchFilter && passesMinMinutesFilter;
    });
  }, [normalizedMinMinutes, playersQuery.data?.items, search]);

  const columns = useMemo<Array<ColumnDef<PlayerListItem, unknown>>>(
    () => [
      {
        accessorKey: "playerName",
        header: "Jogador",
        cell: ({ row }) => {
          const player = row.original;

          return (
            <div className="flex flex-col gap-1">
              <Link
                className="font-medium text-slate-900 hover:underline"
                href={`/players/${player.playerId}`}
                onFocus={() => {
                  prefetchPlayerDetail(player.playerId);
                }}
                onMouseEnter={() => {
                  prefetchPlayerDetail(player.playerId);
                }}
              >
                {player.playerName}
              </Link>
              <span className="text-xs text-slate-500">{player.nationality ?? "Nacionalidade nao informada"}</span>
            </div>
          );
        },
      },
      {
        accessorKey: "teamName",
        header: "Time",
        cell: ({ row }) => row.original.teamName ?? "-",
      },
      {
        accessorKey: "position",
        header: "Posicao",
        cell: ({ row }) => row.original.position ?? "-",
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
      {
        id: "compare",
        header: "Comparar",
        enableSorting: false,
        cell: ({ row }) => {
          const player = row.original;
          const isSelected = selectedIdsSet.has(player.playerId);
          const canAddMore = selectedIds.length < 2;
          const isDisabled = !isSelected && !canAddMore;

          return (
            <button
              className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 disabled:cursor-not-allowed disabled:opacity-50"
              disabled={isDisabled}
              onClick={() => {
                handleCompareAction(player.playerId);
              }}
              type="button"
            >
              {isSelected ? "Remove from compare" : "Add to compare"}
            </button>
          );
        },
      },
    ],
    [handleCompareAction, prefetchPlayerDetail, selectedIds.length, selectedIdsSet],
  );

  if (playersQuery.isError && tableData.length === 0) {
    return (
      <main className="space-y-4">
        <h1 className="text-xl font-semibold">Jogadores</h1>
        <section className="rounded-md border border-rose-300 bg-rose-50 p-4 text-sm text-rose-700">
          <p>Falha ao carregar lista de jogadores.</p>
          <p>{playersQuery.error?.message}</p>
        </section>
      </main>
    );
  }

  return (
    <main className="space-y-4">
      <header className="space-y-2">
        <h1 className="text-xl font-semibold">Jogadores</h1>
        <p className="text-sm text-slate-600">MVP da lista de jogadores com filtros locais e globais aplicados.</p>
      </header>

      <section className="grid gap-3 rounded-md border border-slate-200 bg-white p-4 md:grid-cols-2">
        <label className="flex flex-col gap-1 text-sm text-slate-700">
          Buscar jogador
          <input
            className="rounded border border-slate-300 px-2 py-1"
            onChange={(event) => {
              setSearch(event.target.value);
            }}
            placeholder="Ex.: Arrascaeta"
            type="text"
            value={search}
          />
        </label>

        <label className="flex flex-col gap-1 text-sm text-slate-700">
          Minimo de minutos
          <input
            className="rounded border border-slate-300 px-2 py-1"
            min={0}
            onChange={(event) => {
              setMinMinutesInput(event.target.value);
            }}
            placeholder="Ex.: 300"
            type="number"
            value={minMinutesInput}
          />
        </label>
      </section>

      {playersQuery.isError ? (
        <section className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
          Dados carregados com alerta: {playersQuery.error?.message}
        </section>
      ) : null}

      {playersQuery.isPartial ? <PartialDataBanner coverage={playersQuery.coverage} /> : null}

      {!playersQuery.isLoading ? (
        <div className="flex items-center gap-2">
          <span className="text-sm text-slate-600">Status de cobertura:</span>
          <CoverageBadge coverage={playersQuery.coverage} />
        </div>
      ) : null}

      <DataTable<PlayerListItem>
        columns={columns}
        data={tableData}
        emptyDescription="Nenhum jogador encontrado para o recorte atual."
        emptyTitle="Sem jogadores"
        loading={playersQuery.isLoading}
      />
    </main>
  );
}
