"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import { Calendar, ChevronLeft, ChevronRight, Filter, List, Search } from "lucide-react";
import Link from "next/link";

import { useQueryClient } from "@tanstack/react-query";

import { matchesQueryKeys } from "@/features/matches/queryKeys";
import { fetchMatchCenter } from "@/features/matches/services/matches.service";
import type { MatchCenterFilters, MatchListItem, MatchesListSortDirection } from "@/features/matches/types";
import { useMatchesList } from "@/features/matches/hooks";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import { formatDate } from "@/shared/utils/formatters";

const STATUS_LABEL: Record<string, string> = {
  finished: "Encerrada",
  scheduled: "Agendada",
  live: "Ao vivo",
  cancelled: "Cancelada",
};

const STATUS_STYLE: Record<string, string> = {
  finished: "border-slate-600 bg-slate-800 text-slate-400",
  scheduled: "border-sky-500/40 bg-sky-500/10 text-sky-300",
  live: "border-emerald-500/40 bg-emerald-500/20 text-emerald-300",
  cancelled: "border-red-500/40 bg-red-500/10 text-red-400",
};

const WEEKDAY_LABELS = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sab", "Dom"] as const;

const LIST_PAGE_SIZE = 20;
const CALENDAR_PAGE_SIZE = 200;

type ViewMode = "list" | "calendar";
type TeamScope = "any" | "home" | "away";

type CalendarCell = {
  key: string;
  dayNumber: number | null;
  matches: MatchListItem[];
};

function matchStatusLabel(status?: string | null): string {
  return STATUS_LABEL[status ?? ""] ?? status ?? "-";
}

function kickoffDate(match: MatchListItem): Date | null {
  if (!match.kickoffAt) {
    return null;
  }

  const parsedDate = new Date(match.kickoffAt);
  if (Number.isNaN(parsedDate.getTime())) {
    return null;
  }

  return parsedDate;
}

function toMonthKey(date: Date): string {
  const year = String(date.getFullYear());
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

function parseMonthKey(monthKey: string): { year: number; month: number } | null {
  const match = /^(\d{4})-(\d{2})$/.exec(monthKey);
  if (!match) {
    return null;
  }

  const year = Number.parseInt(match[1], 10);
  const month = Number.parseInt(match[2], 10);

  if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
    return null;
  }

  return { year, month };
}

function shiftMonth(monthKey: string, delta: number): string {
  const parsed = parseMonthKey(monthKey);
  if (!parsed) {
    return toMonthKey(new Date());
  }

  const next = new Date(parsed.year, parsed.month - 1 + delta, 1);
  return toMonthKey(next);
}

function toDateKey(date: Date): string {
  const year = String(date.getFullYear());
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function resolveMatchScoreLabel(match: MatchListItem): string {
  if (
    typeof match.homeScore === "number" &&
    Number.isFinite(match.homeScore) &&
    typeof match.awayScore === "number" &&
    Number.isFinite(match.awayScore)
  ) {
    return `${match.homeScore}-${match.awayScore}`;
  }

  return matchStatusLabel(match.status);
}

function groupByRound(items: MatchListItem[]): Map<string, MatchListItem[]> {
  const groupedMap = new Map<string, MatchListItem[]>();

  for (const item of items) {
    const key = item.roundId ?? "Sem rodada";
    if (!groupedMap.has(key)) {
      groupedMap.set(key, []);
    }

    groupedMap.get(key)?.push(item);
  }

  return groupedMap;
}

function buildCalendarCells(monthKey: string, items: MatchListItem[]): CalendarCell[] {
  const parsed = parseMonthKey(monthKey);
  if (!parsed) {
    return [];
  }

  const { year, month } = parsed;
  const matchesByDay = new Map<string, MatchListItem[]>();

  for (const item of items) {
    const date = kickoffDate(item);
    if (!date) {
      continue;
    }

    if (date.getFullYear() !== year || date.getMonth() + 1 !== month) {
      continue;
    }

    const dayKey = toDateKey(date);
    if (!matchesByDay.has(dayKey)) {
      matchesByDay.set(dayKey, []);
    }

    matchesByDay.get(dayKey)?.push(item);
  }

  const firstDay = new Date(year, month - 1, 1);
  const daysInMonth = new Date(year, month, 0).getDate();
  const firstWeekdayMondayBased = (firstDay.getDay() + 6) % 7;
  const totalCells = Math.ceil((firstWeekdayMondayBased + daysInMonth) / 7) * 7;

  const cells: CalendarCell[] = [];

  for (let index = 0; index < totalCells; index += 1) {
    const dayNumber = index - firstWeekdayMondayBased + 1;

    if (dayNumber < 1 || dayNumber > daysInMonth) {
      cells.push({
        key: `empty-${index}`,
        dayNumber: null,
        matches: [],
      });
      continue;
    }

    const date = new Date(year, month - 1, dayNumber);
    const dayKey = toDateKey(date);

    cells.push({
      key: dayKey,
      dayNumber,
      matches: matchesByDay.get(dayKey) ?? [],
    });
  }

  return cells;
}

function MatchCardSkeleton() {
  return (
    <div className="flex h-16 items-center gap-4 rounded-lg border border-slate-700/50 bg-slate-800/40 px-4">
      <LoadingSkeleton className="bg-slate-700" height={14} width="34%" />
      <LoadingSkeleton className="bg-slate-700" height={20} width={56} />
      <LoadingSkeleton className="bg-slate-700" height={14} width="34%" />
      <LoadingSkeleton className="hidden bg-slate-700 sm:block" height={12} width={120} />
    </div>
  );
}

function MatchListCard({
  match,
  onPrefetch,
}: {
  match: MatchListItem;
  onPrefetch: (matchId: string) => void;
}) {
  const hasScore =
    match.homeScore !== null &&
    match.homeScore !== undefined &&
    match.awayScore !== null &&
    match.awayScore !== undefined;
  const homeWon = hasScore && match.homeScore! > match.awayScore!;
  const awayWon = hasScore && match.awayScore! > match.homeScore!;
  const statusStyle = STATUS_STYLE[match.status ?? ""] ?? "border-slate-600 bg-slate-800 text-slate-400";

  return (
    <Link
      className="group flex items-center gap-3 rounded-lg border border-slate-700/50 bg-slate-800/40 px-4 py-3 no-underline transition-all hover:border-emerald-500/30 hover:bg-slate-800/70"
      href={`/matches/${match.matchId}`}
      onFocus={() => {
        onPrefetch(match.matchId);
      }}
      onMouseEnter={() => {
        onPrefetch(match.matchId);
      }}
    >
      <span
        className={`flex-1 truncate text-right text-sm font-medium transition-colors ${homeWon ? "text-emerald-300" : "text-slate-200 group-hover:text-slate-100"
          }`}
      >
        {match.homeTeamName ?? "Mandante"}
      </span>

      <div className="shrink-0 text-center">
        {hasScore ? (
          <div className="flex items-center gap-1 font-mono font-bold">
            <span className={homeWon ? "text-emerald-300" : !awayWon ? "text-amber-300" : "text-slate-500"}>
              {match.homeScore}
            </span>
            <span className="text-slate-600">-</span>
            <span className={awayWon ? "text-emerald-300" : !homeWon ? "text-amber-300" : "text-slate-500"}>
              {match.awayScore}
            </span>
          </div>
        ) : (
          <span className={`rounded border px-2.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${statusStyle}`}>
            {matchStatusLabel(match.status)}
          </span>
        )}
      </div>

      <span
        className={`flex-1 truncate text-left text-sm font-medium transition-colors ${awayWon ? "text-emerald-300" : "text-slate-200 group-hover:text-slate-100"
          }`}
      >
        {match.awayTeamName ?? "Visitante"}
      </span>

      <div className="hidden shrink-0 flex-col items-end gap-0.5 sm:flex">
        <span className="text-[10px] text-slate-500">{formatDate(match.kickoffAt)}</span>
        <span className="max-w-[180px] truncate text-[10px] text-slate-600">
          Estádio: {match.venueName ?? "Não informado"}
        </span>
        <span className="max-w-[180px] truncate text-[10px] text-slate-600">
          Árbitro: {match.refereeName ?? "Não informado"}
        </span>
      </div>
    </Link>
  );
}

function CalendarMonthView({
  cells,
  onPrefetch,
}: {
  cells: CalendarCell[];
  onPrefetch: (matchId: string) => void;
}) {
  if (cells.length === 0) {
    return <p className="py-8 text-center text-sm text-slate-400">Não foi possível montar o calendário.</p>;
  }

  return (
    <div className="space-y-2">
      <div className="grid grid-cols-7 gap-2">
        {WEEKDAY_LABELS.map((weekday) => (
          <div
            className="rounded-md border border-slate-700/60 bg-slate-900/70 px-2 py-1 text-center text-[10px] font-semibold uppercase tracking-wider text-slate-400"
            key={weekday}
          >
            {weekday}
          </div>
        ))}
      </div>

      <div className="grid grid-cols-7 gap-2">
        {cells.map((cell) => (
          <div
            className={`min-h-[112px] rounded-md border p-2 ${cell.dayNumber === null
                ? "border-dashed border-slate-800 bg-slate-900/30"
                : "border-slate-700/60 bg-slate-800/50"
              }`}
            key={cell.key}
          >
            {cell.dayNumber !== null ? (
              <>
                <p className="mb-1 text-[11px] font-semibold text-slate-300">{cell.dayNumber}</p>
                <div className="space-y-1">
                  {cell.matches.slice(0, 3).map((match) => (
                    <Link
                      className="block rounded border border-slate-700/70 bg-slate-900/80 px-1.5 py-1 text-[10px] text-slate-300 no-underline transition-colors hover:border-emerald-500/40 hover:text-emerald-300"
                      href={`/matches/${match.matchId}`}
                      key={match.matchId}
                      onFocus={() => {
                        onPrefetch(match.matchId);
                      }}
                      onMouseEnter={() => {
                        onPrefetch(match.matchId);
                      }}
                    >
                      <p className="truncate">{match.homeTeamName ?? "Casa"} x {match.awayTeamName ?? "Fora"}</p>
                      <p className="text-slate-500">{resolveMatchScoreLabel(match)}</p>
                    </Link>
                  ))}
                  {cell.matches.length > 3 ? (
                    <p className="text-[10px] text-slate-500">+{cell.matches.length - 3} partidas</p>
                  ) : null}
                </div>
              </>
            ) : null}
          </div>
        ))}
      </div>
    </div>
  );
}

export default function MatchesPage() {
  const [search, setSearch] = useState("");
  const [teamScope, setTeamScope] = useState<TeamScope>("any");
  const [status, setStatus] = useState("");
  const [sortDirection, setSortDirection] = useState<MatchesListSortDirection>("desc");
  const [pageIndex, setPageIndex] = useState(0);
  const [viewMode, setViewMode] = useState<ViewMode>("list");
  const [calendarMonthKey, setCalendarMonthKey] = useState("");

  const queryClient = useQueryClient();
  const { competitionId, seasonId, venue } = useGlobalFiltersState();
  const { params: timeRangeParams } = useTimeRange();

  const effectivePage = viewMode === "calendar" ? 1 : pageIndex + 1;
  const effectivePageSize = viewMode === "calendar" ? CALENDAR_PAGE_SIZE : LIST_PAGE_SIZE;

  const matchesQuery = useMatchesList({
    search,
    teamScope,
    status,
    page: effectivePage,
    pageSize: effectivePageSize,
    sortBy: "kickoffAt",
    sortDirection,
  });

  const detailPrefetchFilters = useMemo<MatchCenterFilters>(
    () => ({
      competitionId,
      seasonId,
      roundId: timeRangeParams.roundId,
      monthKey: timeRangeParams.monthKey,
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
      timeRangeParams.monthKey,
      timeRangeParams.roundId,
      venue,
    ],
  );

  const prefetchMatchDetail = useCallback(
    (matchId: string) => {
      const normalizedId = matchId.trim();
      if (!normalizedId) {
        return;
      }

      void queryClient.prefetchQuery({
        queryKey: matchesQueryKeys.center(normalizedId, detailPrefetchFilters),
        queryFn: () => fetchMatchCenter(normalizedId, detailPrefetchFilters),
        staleTime: 5 * 60 * 1000,
      });
    },
    [detailPrefetchFilters, queryClient],
  );

  const items = useMemo(() => matchesQuery.data?.items ?? [], [matchesQuery.data?.items]);
  const totalCount = matchesQuery.pagination?.totalCount ?? items.length;
  const groupedByRound = useMemo(() => groupByRound(items), [items]);
  const totalPages = Math.ceil(totalCount / LIST_PAGE_SIZE);

  const defaultMonthKey = useMemo(() => {
    if (timeRangeParams.monthKey) {
      return timeRangeParams.monthKey;
    }

    const firstWithDate = items.find((item) => kickoffDate(item) !== null);
    if (firstWithDate) {
      const date = kickoffDate(firstWithDate);
      if (date) {
        return toMonthKey(date);
      }
    }

    return toMonthKey(new Date());
  }, [items, timeRangeParams.monthKey]);

  useEffect(() => {
    if (!calendarMonthKey) {
      setCalendarMonthKey(defaultMonthKey);
    }
  }, [calendarMonthKey, defaultMonthKey]);

  useEffect(() => {
    if (timeRangeParams.monthKey) {
      setCalendarMonthKey(timeRangeParams.monthKey);
    }
  }, [timeRangeParams.monthKey]);

  const calendarCells = useMemo(
    () => buildCalendarCells(calendarMonthKey, items),
    [calendarMonthKey, items],
  );

  const monthTitle = useMemo(() => {
    const parsed = parseMonthKey(calendarMonthKey);
    if (!parsed) {
      return "Mês inválido";
    }

    const date = new Date(parsed.year, parsed.month - 1, 1);
    return new Intl.DateTimeFormat("pt-BR", { month: "long", year: "numeric" }).format(date);
  }, [calendarMonthKey]);

  return (
    <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
      <div className="mx-auto max-w-7xl space-y-6">
        <header className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-slate-100">Partidas</h1>
            <p className="mt-1 text-sm text-slate-400">
              {totalCount > 0 ? `${totalCount} partidas encontradas` : "Lista de partidas com filtros locais"}
            </p>
          </div>

          <div className="flex rounded-lg border border-slate-700 bg-slate-900/80 p-1">
            <button
              aria-label="Modo lista"
              className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${viewMode === "list" ? "bg-emerald-500/15 text-emerald-300" : "text-slate-400 hover:text-slate-200"
                }`}
              onClick={() => {
                setViewMode("list");
              }}
              type="button"
            >
              <List className="h-3.5 w-3.5" /> Lista
            </button>
            <button
              aria-label="Modo calendario"
              className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${viewMode === "calendar"
                  ? "bg-emerald-500/15 text-emerald-300"
                  : "text-slate-400 hover:text-slate-200"
                }`}
              onClick={() => {
                setViewMode("calendar");
                setPageIndex(0);
              }}
              type="button"
            >
              <Calendar className="h-3.5 w-3.5" /> Calendário mensal
            </button>
          </div>
        </header>

        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-4 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
          <div className="flex flex-wrap items-end gap-4">
            <label className="flex flex-1 flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
              <span className="flex items-center gap-1.5">
                <Search className="h-3 w-3" /> Buscar time
              </span>
              <input
                className="w-full min-w-[180px] rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 placeholder:text-slate-500 focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
                onChange={(event) => {
                  setSearch(event.target.value);
                  setPageIndex(0);
                }}
                placeholder="Ex.: Flamengo"
                type="text"
                value={search}
              />
            </label>

            <label className="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
              <span className="flex items-center gap-1.5">
                <Filter className="h-3 w-3" /> Escopo do time
              </span>
              <select
                className="rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
                onChange={(event) => {
                  setTeamScope(event.target.value as TeamScope);
                  setPageIndex(0);
                }}
                value={teamScope}
              >
                <option value="any">Casa ou fora</option>
                <option value="home">Somente mandante</option>
                <option value="away">Somente visitante</option>
              </select>
            </label>

            <label className="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
              <span className="flex items-center gap-1.5">
                <Filter className="h-3 w-3" /> Status
              </span>
              <select
                className="rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
                onChange={(event) => {
                  setStatus(event.target.value);
                  setPageIndex(0);
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

            <label className="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
              <span className="flex items-center gap-1.5">
                <Calendar className="h-3 w-3" /> Ordenação
              </span>
              <select
                className="rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
                onChange={(event) => {
                  setSortDirection(event.target.value as MatchesListSortDirection);
                  setPageIndex(0);
                }}
                value={sortDirection}
              >
                <option value="desc">Mais recentes primeiro</option>
                <option value="asc">Mais antigas primeiro</option>
              </select>
            </label>
          </div>
        </section>

        {matchesQuery.isPartial ? <PartialDataBanner coverage={matchesQuery.coverage} /> : null}
        {!matchesQuery.isLoading ? (
          <div className="flex items-center gap-2">
            <span className="text-xs text-slate-500">Cobertura:</span>
            <CoverageBadge coverage={matchesQuery.coverage} />
          </div>
        ) : null}

        {matchesQuery.isError && items.length === 0 ? (
          <section className="rounded-xl border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-400">
            <p className="font-medium">Falha ao carregar lista de partidas.</p>
            <p className="mt-1 text-red-500/70">{matchesQuery.error?.message}</p>
          </section>
        ) : null}

        {matchesQuery.isLoading ? (
          <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-5 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
            <div className="space-y-2">
              {Array.from({ length: 8 }).map((_, index) => (
                <MatchCardSkeleton key={`match-skeleton-${index}`} />
              ))}
            </div>
          </section>
        ) : null}

        {!matchesQuery.isLoading ? (
          <>
            {viewMode === "list" ? (
              <div className="space-y-4">
                {groupedByRound.size === 0 ? (
                  <section className="rounded-xl border border-slate-700 bg-slate-900 p-10 text-center">
                    <p className="text-sm text-slate-400">Nenhuma partida encontrada para o recorte atual.</p>
                  </section>
                ) : (
                  Array.from(groupedByRound.entries()).map(([roundId, matches]) => (
                    <section
                      className="rounded-xl border border-slate-700/80 bg-slate-900/80 shadow-lg shadow-slate-950/30 backdrop-blur-sm"
                      key={roundId}
                    >
                      <header className="flex items-center gap-3 border-b border-slate-700/50 px-5 py-3">
                        <Calendar className="h-4 w-4 text-emerald-400" />
                        <h2 className="text-sm font-semibold text-slate-200">
                          Rodada {roundId === "Sem rodada" ? "-" : roundId}
                        </h2>
                        <span className="ml-auto text-xs text-slate-500">{matches.length} partidas</span>
                      </header>
                      <div className="space-y-1.5 p-3">
                        {matches.map((match) => (
                          <MatchListCard key={match.matchId} match={match} onPrefetch={prefetchMatchDetail} />
                        ))}
                      </div>
                    </section>
                  ))
                )}
              </div>
            ) : (
              <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-5 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
                <header className="mb-4 flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <h2 className="text-base font-semibold text-slate-100">Calendário mensal</h2>
                    <p className="text-xs text-slate-500">{monthTitle}</p>
                  </div>

                  <div className="flex items-center gap-2">
                    <button
                      className="rounded-md border border-slate-600 bg-slate-800 p-1.5 text-slate-300 transition-colors hover:border-emerald-500/60 hover:text-emerald-300"
                      onClick={() => {
                        setCalendarMonthKey((current) => shiftMonth(current, -1));
                      }}
                      type="button"
                    >
                      <ChevronLeft className="h-4 w-4" />
                    </button>
                    <input
                      className="rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-1.5 text-xs text-slate-100 [color-scheme:dark] focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
                      onChange={(event) => {
                        setCalendarMonthKey(event.target.value);
                      }}
                      type="month"
                      value={calendarMonthKey}
                    />
                    <button
                      className="rounded-md border border-slate-600 bg-slate-800 p-1.5 text-slate-300 transition-colors hover:border-emerald-500/60 hover:text-emerald-300"
                      onClick={() => {
                        setCalendarMonthKey((current) => shiftMonth(current, 1));
                      }}
                      type="button"
                    >
                      <ChevronRight className="h-4 w-4" />
                    </button>
                  </div>
                </header>

                <CalendarMonthView cells={calendarCells} onPrefetch={prefetchMatchDetail} />
              </section>
            )}

            {viewMode === "list" && totalPages > 1 ? (
              <div className="flex items-center justify-between rounded-xl border border-slate-700/80 bg-slate-900/80 px-5 py-3 backdrop-blur-sm">
                <button
                  className="rounded-md border border-slate-600 bg-slate-800 px-3 py-1.5 text-xs font-medium text-slate-300 transition-colors hover:border-emerald-500/60 hover:text-emerald-300 disabled:opacity-40"
                  disabled={pageIndex === 0}
                  onClick={() => {
                    setPageIndex((current) => current - 1);
                  }}
                  type="button"
                >
                  Anterior
                </button>
                <span className="text-xs text-slate-400">
                  Página {pageIndex + 1} de {totalPages}
                </span>
                <button
                  className="rounded-md border border-slate-600 bg-slate-800 px-3 py-1.5 text-xs font-medium text-slate-300 transition-colors hover:border-emerald-500/60 hover:text-emerald-300 disabled:opacity-40"
                  disabled={pageIndex + 1 >= totalPages}
                  onClick={() => {
                    setPageIndex((current) => current + 1);
                  }}
                  type="button"
                >
                  Próxima
                </button>
              </div>
            ) : null}
          </>
        ) : null}
      </div>
    </div>
  );
}
