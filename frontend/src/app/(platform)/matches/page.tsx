"use client";

import { useCallback, useMemo, useState } from "react";

import Link from "next/link";

import { useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";

import { useMatchesList } from "@/features/matches/hooks";
import { matchesQueryKeys } from "@/features/matches/queryKeys";
import { fetchMatchCenter } from "@/features/matches/services/matches.service";
import type { MatchCenterFilters, MatchListItem, MatchesListSortDirection } from "@/features/matches/types";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { DataTable } from "@/shared/components/data-display/DataTable";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import { formatDate } from "@/shared/utils/formatters";
import { formatMetricValue } from "@/config/metrics.registry";

export default function MatchesPage() {
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState("");
  const [sortDirection, setSortDirection] = useState<MatchesListSortDirection>("desc");
  const queryClient = useQueryClient();
  const { competitionId, seasonId, venue } = useGlobalFiltersState();
  const { params: timeRangeParams } = useTimeRange();

  const matchesQuery = useMatchesList({
    search,
    status,
    sortBy: "kickoffAt",
    sortDirection,
  });

  const detailPrefetchFilters = useMemo<MatchCenterFilters>(
    () => ({
      competitionId,
      seasonId,
      roundId: timeRangeParams.roundId,
      venue,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
      includeTimeline: true,
      includeLineups: true,
      includePlayerStats: true,
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

  const prefetchMatchDetail = useCallback(
    (matchId: string) => {
      const normalizedMatchId = matchId.trim();

      if (normalizedMatchId.length === 0) {
        return;
      }

      void queryClient.prefetchQuery({
        queryKey: matchesQueryKeys.center(normalizedMatchId, detailPrefetchFilters),
        queryFn: () => fetchMatchCenter(normalizedMatchId, detailPrefetchFilters),
        staleTime: 5 * 60 * 1000,
      });
    },
    [detailPrefetchFilters, queryClient],
  );

  const tableData = useMemo(() => matchesQuery.data?.items ?? [], [matchesQuery.data?.items]);

  const columns = useMemo<Array<ColumnDef<MatchListItem, unknown>>>(
    () => [
      {
        accessorKey: "kickoffAt",
        header: "Data",
        cell: ({ row }) => formatDate(row.original.kickoffAt),
      },
      {
        id: "match",
        header: "Partida",
        accessorFn: (row) => `${row.homeTeamName ?? "Mandante"} vs ${row.awayTeamName ?? "Visitante"}`,
        cell: ({ row }) => {
          const match = row.original;

          return (
            <div className="flex flex-col gap-1">
              <Link
                className="font-medium text-slate-900 hover:underline"
                href={`/matches/${match.matchId}`}
                onFocus={() => {
                  prefetchMatchDetail(match.matchId);
                }}
                onMouseEnter={() => {
                  prefetchMatchDetail(match.matchId);
                }}
              >
                {match.homeTeamName ?? "Mandante"} vs {match.awayTeamName ?? "Visitante"}
              </Link>
              <span className="text-xs text-slate-500">ID: {match.matchId}</span>
            </div>
          );
        },
      },
      {
        id: "score",
        header: "Placar",
        accessorFn: (row) => `${row.homeScore ?? "-"} x ${row.awayScore ?? "-"}`,
        cell: ({ row }) => (
          <span>
            {formatMetricValue("goals", row.original.homeScore)} x {formatMetricValue("goals", row.original.awayScore)}
          </span>
        ),
      },
      {
        accessorKey: "status",
        header: "Status",
        cell: ({ row }) => row.original.status ?? "-",
      },
      {
        accessorKey: "competitionName",
        header: "Competicao",
        cell: ({ row }) => row.original.competitionName ?? "-",
      },
      {
        accessorKey: "venueName",
        header: "Venue",
        cell: ({ row }) => row.original.venueName ?? "-",
      },
    ],
    [prefetchMatchDetail],
  );

  if (matchesQuery.isError && tableData.length === 0) {
    return (
      <main className="space-y-4">
        <h1 className="text-xl font-semibold">Partidas</h1>
        <section className="rounded-md border border-rose-300 bg-rose-50 p-4 text-sm text-rose-700">
          <p>Falha ao carregar lista de partidas.</p>
          <p>{matchesQuery.error?.message}</p>
        </section>
      </main>
    );
  }

  return (
    <main className="space-y-4">
      <header className="space-y-1">
        <h1 className="text-xl font-semibold">Partidas</h1>
        <p className="text-sm text-slate-600">Lista paginada com ordenacao basica no cliente usando DataTable.</p>
      </header>

      <section className="grid gap-3 rounded-md border border-slate-200 bg-white p-4 md:grid-cols-3">
        <label className="flex flex-col gap-1 text-sm text-slate-700">
          Buscar partida
          <input
            className="rounded border border-slate-300 px-2 py-1"
            onChange={(event) => {
              setSearch(event.target.value);
            }}
            placeholder="Ex.: Flamengo"
            type="text"
            value={search}
          />
        </label>

        <label className="flex flex-col gap-1 text-sm text-slate-700">
          Status
          <select
            className="rounded border border-slate-300 bg-white px-2 py-1"
            onChange={(event) => {
              setStatus(event.target.value);
            }}
            value={status}
          >
            <option value="">Todos</option>
            <option value="scheduled">Agendada</option>
            <option value="live">Ao vivo</option>
            <option value="finished">Finalizada</option>
            <option value="cancelled">Cancelada</option>
          </select>
        </label>

        <label className="flex flex-col gap-1 text-sm text-slate-700">
          Ordenacao por data
          <select
            className="rounded border border-slate-300 bg-white px-2 py-1"
            onChange={(event) => {
              setSortDirection(event.target.value as MatchesListSortDirection);
            }}
            value={sortDirection}
          >
            <option value="desc">Mais recentes primeiro</option>
            <option value="asc">Mais antigas primeiro</option>
          </select>
        </label>
      </section>

      {matchesQuery.isError ? (
        <section className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
          Dados carregados com alerta: {matchesQuery.error?.message}
        </section>
      ) : null}

      {matchesQuery.isPartial ? <PartialDataBanner coverage={matchesQuery.coverage} /> : null}

      {!matchesQuery.isLoading ? (
        <div className="flex items-center gap-2">
          <span className="text-sm text-slate-600">Status de cobertura:</span>
          <CoverageBadge coverage={matchesQuery.coverage} />
        </div>
      ) : null}

      <DataTable<MatchListItem>
        columns={columns}
        data={tableData}
        emptyDescription="Nenhuma partida encontrada para o recorte atual."
        emptyTitle="Sem partidas"
        loading={matchesQuery.isLoading}
      />
    </main>
  );
}
