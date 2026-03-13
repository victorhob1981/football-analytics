"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { usePathname, useRouter, useSearchParams } from "next/navigation";

import {
  isFilterEnabled,
  resolvePageFilterConfig,
  type PageFilterConfig,
  type PageFilterState,
} from "@/config/page-filters.config";
import { useGlobalFilters } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import type { GlobalFiltersActions, GlobalFiltersState, VenueFilter } from "@/shared/types/filters.types";

const FILTER_QUERY_KEYS = [
  "competitionId",
  "seasonId",
  "roundId",
  "monthKey",
  "month",
  "venue",
  "lastN",
  "dateRangeStart",
  "dateRangeEnd",
] as const;

type SearchParamsLike = Pick<URLSearchParams, "get">;

type ApplyFiltersActions = {
  setCompetitionId: (competitionId: string | null) => void;
  setSeasonId: (seasonId: string | null) => void;
  setVenue: (venue: VenueFilter) => void;
  setTimeRange: GlobalFiltersActions["setTimeRange"];
};

function parseVenue(value: string | null): VenueFilter {
  if (value === "home" || value === "away" || value === "all") {
    return value;
  }

  return "all";
}

function parseNullableText(value: string | null): string | null {
  if (!value) {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function parseLastN(value: string | null): number | null {
  if (!value) {
    return null;
  }

  const parsed = Number.parseInt(value, 10);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }

  return parsed;
}

function parseMonthKey(value: string | null): string | null {
  if (!value) {
    return null;
  }

  const trimmed = value.trim();
  const monthKeyPattern = /^(\d{4})-(\d{2})$/;
  const parsed = monthKeyPattern.exec(trimmed);

  if (!parsed) {
    return null;
  }

  const month = Number.parseInt(parsed[2], 10);
  if (month < 1 || month > 12) {
    return null;
  }

  return trimmed;
}

function parseFiltersFromSearchParams(searchParams: SearchParamsLike): GlobalFiltersState {
  const competitionId = parseNullableText(searchParams.get("competitionId"));
  const seasonId = parseNullableText(searchParams.get("seasonId"));
  const roundId = parseNullableText(searchParams.get("roundId"));
  const monthKeyFromQuery = parseMonthKey(searchParams.get("monthKey")) ?? parseMonthKey(searchParams.get("month"));
  const venue = parseVenue(searchParams.get("venue"));
  const lastN = parseLastN(searchParams.get("lastN"));
  const dateRangeStartFromQuery = parseNullableText(searchParams.get("dateRangeStart"));
  const dateRangeEndFromQuery = parseNullableText(searchParams.get("dateRangeEnd"));

  return {
    competitionId,
    seasonId,
    roundId,
    monthKey: monthKeyFromQuery,
    venue,
    lastN,
    dateRangeStart: dateRangeStartFromQuery,
    dateRangeEnd: dateRangeEndFromQuery,
  };
}

function normalizeTemporalFilters(filters: GlobalFiltersState): GlobalFiltersState {
  if (filters.roundId !== null) {
    return {
      ...filters,
      monthKey: null,
      lastN: null,
      dateRangeStart: null,
      dateRangeEnd: null,
    };
  }

  if (filters.monthKey !== null) {
    return {
      ...filters,
      roundId: null,
      lastN: null,
      dateRangeStart: null,
      dateRangeEnd: null,
    };
  }

  if (filters.lastN !== null) {
    return {
      ...filters,
      roundId: null,
      monthKey: null,
      dateRangeStart: null,
      dateRangeEnd: null,
    };
  }

  if (filters.dateRangeStart !== null || filters.dateRangeEnd !== null) {
    return {
      ...filters,
      roundId: null,
      monthKey: null,
      lastN: null,
    };
  }

  return {
    ...filters,
    roundId: null,
    monthKey: null,
    lastN: null,
    dateRangeStart: null,
    dateRangeEnd: null,
  };
}

function applyPageFilterConfig(filters: GlobalFiltersState, pageFilterConfig: PageFilterConfig): GlobalFiltersState {
  const nextFilters: GlobalFiltersState = { ...filters };

  if (!isFilterEnabled(pageFilterConfig.season)) {
    nextFilters.seasonId = null;
  }

  if (!isFilterEnabled(pageFilterConfig.round)) {
    nextFilters.roundId = null;
  }

  if (!isFilterEnabled(pageFilterConfig.month)) {
    nextFilters.monthKey = null;
  }

  if (!isFilterEnabled(pageFilterConfig.lastN)) {
    nextFilters.lastN = null;
  }

  if (!isFilterEnabled(pageFilterConfig.dateRange)) {
    nextFilters.dateRangeStart = null;
    nextFilters.dateRangeEnd = null;
  }

  if (!isFilterEnabled(pageFilterConfig.venue)) {
    nextFilters.venue = "all";
  }

  return normalizeTemporalFilters(nextFilters);
}

function areFiltersEqual(a: GlobalFiltersState, b: GlobalFiltersState): boolean {
  return (
    a.competitionId === b.competitionId &&
    a.seasonId === b.seasonId &&
    a.roundId === b.roundId &&
    a.monthKey === b.monthKey &&
    a.venue === b.venue &&
    a.lastN === b.lastN &&
    a.dateRangeStart === b.dateRangeStart &&
    a.dateRangeEnd === b.dateRangeEnd
  );
}

function applyFiltersToStore(filters: GlobalFiltersState, actions: ApplyFiltersActions): void {
  actions.setCompetitionId(filters.competitionId);
  actions.setSeasonId(filters.seasonId);
  actions.setVenue(filters.venue);

  if (filters.roundId !== null) {
    actions.setTimeRange({
      mode: "round",
      roundId: filters.roundId,
    });
    return;
  }

  if (filters.monthKey !== null) {
    actions.setTimeRange({
      mode: "month",
      monthKey: filters.monthKey,
    });
    return;
  }

  if (filters.lastN !== null) {
    actions.setTimeRange({
      mode: "lastN",
      lastN: filters.lastN,
    });
    return;
  }

  if (filters.dateRangeStart !== null || filters.dateRangeEnd !== null) {
    actions.setTimeRange({
      mode: "dateRange",
      dateRangeStart: filters.dateRangeStart,
      dateRangeEnd: filters.dateRangeEnd,
    });
    return;
  }

  actions.setTimeRange({
    mode: "none",
  });
}

function upsertQueryParams(
  currentSearchParams: URLSearchParams,
  filters: GlobalFiltersState,
  pageFilterConfig: PageFilterConfig,
): URLSearchParams {
  const nextSearchParams = new URLSearchParams(currentSearchParams.toString());

  for (const key of FILTER_QUERY_KEYS) {
    nextSearchParams.delete(key);
  }

  if (filters.competitionId) {
    nextSearchParams.set("competitionId", filters.competitionId);
  }

  if (filters.seasonId && isFilterEnabled(pageFilterConfig.season)) {
    nextSearchParams.set("seasonId", filters.seasonId);
  }

  if (filters.roundId && isFilterEnabled(pageFilterConfig.round)) {
    nextSearchParams.set("roundId", filters.roundId);
  }

  if (filters.monthKey && isFilterEnabled(pageFilterConfig.month)) {
    nextSearchParams.set("monthKey", filters.monthKey);
  }

  if (filters.venue !== "all" && isFilterEnabled(pageFilterConfig.venue)) {
    nextSearchParams.set("venue", filters.venue);
  }

  if (filters.lastN !== null && isFilterEnabled(pageFilterConfig.lastN)) {
    nextSearchParams.set("lastN", String(filters.lastN));
  } else if (isFilterEnabled(pageFilterConfig.dateRange)) {
    if (filters.dateRangeStart) {
      nextSearchParams.set("dateRangeStart", filters.dateRangeStart);
    }

    if (filters.dateRangeEnd) {
      nextSearchParams.set("dateRangeEnd", filters.dateRangeEnd);
    }
  }

  return nextSearchParams;
}

function resolveActiveModeLabel(activeMode: ReturnType<typeof useTimeRange>["activeMode"]): string {
  if (activeMode === "round") {
    return "Rodada";
  }

  if (activeMode === "month") {
    return "Mês";
  }

  if (activeMode === "lastN") {
    return "Últimos N jogos";
  }

  if (activeMode === "dateRange") {
    return "Intervalo customizado";
  }

  return "Temporada completa";
}

function resolveFilterStateTag(filterState: PageFilterState): string | null {
  if (filterState === "partial") {
    return "Parcial";
  }

  if (filterState === "disabled") {
    return "Desabilitado";
  }

  return null;
}

type FilterLabelProps = {
  htmlFor: string;
  label: string;
  state: PageFilterState;
};

const FIELD_CLASS =
  "w-full rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 placeholder:text-slate-500 transition-colors focus:border-emerald-400 focus:ring-2 focus:ring-emerald-500/30 focus:ring-offset-0 disabled:cursor-not-allowed disabled:border-slate-700 disabled:bg-slate-800 disabled:text-slate-500";

function FilterLabel({ htmlFor, label, state }: FilterLabelProps) {
  const filterTag = resolveFilterStateTag(state);

  return (
    <label className="mb-1 flex items-center gap-2 text-xs font-semibold uppercase tracking-wide text-slate-300" htmlFor={htmlFor}>
      <span>{label}</span>
      {filterTag ? (
        <span
          className={`rounded px-1.5 py-0.5 text-[10px] font-bold ${state === "partial" ? "bg-amber-500/20 text-amber-200" : "bg-slate-700 text-slate-300"
            }`}
        >
          {filterTag}
        </span>
      ) : null}
    </label>
  );
}

export function GlobalFilterBar() {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const hasInitializedFromUrlRef = useRef(false);
  const [isUrlHydrated, setIsUrlHydrated] = useState(false);

  const pageFilterConfig = useMemo(() => resolvePageFilterConfig(pathname), [pathname]);

  const {
    competitionId,
    seasonId,
    roundId,
    monthKey,
    venue,
    lastN,
    dateRangeStart,
    dateRangeEnd,
    setCompetitionId,
    setSeasonId,
    setVenue,
    setTimeRange,
    reset,
  } = useGlobalFilters();

  const { activeMode } = useTimeRange();

  const rawFilters = useMemo(
    () => ({
      competitionId,
      seasonId,
      roundId,
      monthKey,
      venue,
      lastN,
      dateRangeStart,
      dateRangeEnd,
    }),
    [competitionId, dateRangeEnd, dateRangeStart, lastN, monthKey, roundId, seasonId, venue],
  );

  const effectiveFilters = useMemo(
    () => applyPageFilterConfig(rawFilters, pageFilterConfig),
    [pageFilterConfig, rawFilters],
  );

  useEffect(() => {
    hasInitializedFromUrlRef.current = false;
    setIsUrlHydrated(false);
  }, [pathname]);

  useEffect(() => {
    if (hasInitializedFromUrlRef.current) {
      return;
    }

    const parsedFilters = parseFiltersFromSearchParams(searchParams);
    const effectiveParsedFilters = applyPageFilterConfig(parsedFilters, pageFilterConfig);

    if (!areFiltersEqual(effectiveFilters, effectiveParsedFilters)) {
      applyFiltersToStore(effectiveParsedFilters, {
        setCompetitionId,
        setSeasonId,
        setVenue,
        setTimeRange,
      });
    }

    hasInitializedFromUrlRef.current = true;
    setIsUrlHydrated(true);
  }, [effectiveFilters, pageFilterConfig, searchParams, setCompetitionId, setSeasonId, setTimeRange, setVenue]);

  useEffect(() => {
    if (!hasInitializedFromUrlRef.current) {
      return;
    }

    if (areFiltersEqual(rawFilters, effectiveFilters)) {
      return;
    }

    applyFiltersToStore(effectiveFilters, {
      setCompetitionId,
      setSeasonId,
      setVenue,
      setTimeRange,
    });
  }, [effectiveFilters, rawFilters, setCompetitionId, setSeasonId, setTimeRange, setVenue]);

  useEffect(() => {
    if (!hasInitializedFromUrlRef.current || !isUrlHydrated) {
      return;
    }

    const currentSearchParams = new URLSearchParams(searchParams.toString());
    const nextSearchParams = upsertQueryParams(currentSearchParams, effectiveFilters, pageFilterConfig);
    const currentQuery = currentSearchParams.toString();
    const nextQuery = nextSearchParams.toString();

    if (currentQuery === nextQuery) {
      return;
    }

    const nextUrl = nextQuery.length > 0 ? `${pathname}?${nextQuery}` : pathname;
    router.replace(nextUrl, { scroll: false });
  }, [effectiveFilters, isUrlHydrated, pageFilterConfig, pathname, router, searchParams]);

  const isSeasonEnabled = isFilterEnabled(pageFilterConfig.season);
  const isRoundEnabled = isFilterEnabled(pageFilterConfig.round);
  const isMonthEnabled = isFilterEnabled(pageFilterConfig.month);
  const isVenueEnabled = isFilterEnabled(pageFilterConfig.venue);
  const isLastNEnabled = isFilterEnabled(pageFilterConfig.lastN);
  const isDateRangeEnabled = isFilterEnabled(pageFilterConfig.dateRange);

  return (
    <section
      aria-label="Barra de filtros globais"
      className="mx-4 mt-4 rounded-xl border border-slate-700/80 bg-slate-900/80 p-4 shadow-lg shadow-slate-950/30 backdrop-blur-sm md:mx-6"
    >
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-bold uppercase tracking-wide text-slate-100">Filtros globais</h2>
        <span className="text-xs font-medium text-slate-400">Recorte ativo: {resolveActiveModeLabel(activeMode)}</span>
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div>
          <FilterLabel htmlFor="global-filter-competition-id" label="Competição" state="enabled" />
          <input
            className={FIELD_CLASS}
            id="global-filter-competition-id"
            onChange={(event) => setCompetitionId(parseNullableText(event.target.value))}
            placeholder="competitionId"
            type="text"
            value={competitionId ?? ""}
          />
        </div>

        <div>
          <FilterLabel htmlFor="global-filter-season-id" label="Temporada" state={pageFilterConfig.season} />
          <input
            className={FIELD_CLASS}
            disabled={!isSeasonEnabled}
            id="global-filter-season-id"
            onChange={(event) => setSeasonId(parseNullableText(event.target.value))}
            placeholder="seasonId"
            type="text"
            value={seasonId ?? ""}
          />
        </div>

        <div>
          <FilterLabel htmlFor="global-filter-round-id" label="Rodada" state={pageFilterConfig.round} />
          <input
            className={FIELD_CLASS}
            disabled={!isRoundEnabled}
            id="global-filter-round-id"
            onChange={(event) =>
              setTimeRange({
                mode: "round",
                roundId: parseNullableText(event.target.value),
              })
            }
            placeholder="roundId"
            type="text"
            value={roundId ?? ""}
          />
        </div>

        <div>
          <FilterLabel htmlFor="global-filter-month-key" label="Mês" state={pageFilterConfig.month} />
          <input
            className={`${FIELD_CLASS} [color-scheme:dark]`}
            disabled={!isMonthEnabled}
            id="global-filter-month-key"
            onChange={(event) =>
              setTimeRange({
                mode: "month",
                monthKey: parseMonthKey(event.target.value),
              })
            }
            type="month"
            value={monthKey ?? ""}
          />
        </div>

        <div>
          <FilterLabel htmlFor="global-filter-venue" label="Mando de Campo" state={pageFilterConfig.venue} />
          <select
            className={FIELD_CLASS}
            disabled={!isVenueEnabled}
            id="global-filter-venue"
            onChange={(event) => setVenue(event.target.value as VenueFilter)}
            value={venue}
          >
            <option value="all">Todos</option>
            <option value="home">Casa</option>
            <option value="away">Fora</option>
          </select>
        </div>

        <div>
          <FilterLabel htmlFor="global-filter-last-n" label="Últimos N jogos" state={pageFilterConfig.lastN} />
          <input
            className={FIELD_CLASS}
            disabled={!isLastNEnabled}
            id="global-filter-last-n"
            min={1}
            onChange={(event) => {
              const normalizedValue = event.target.value.trim();

              if (normalizedValue.length === 0) {
                setTimeRange({ mode: "none" });
                return;
              }

              const parsedValue = Number.parseInt(normalizedValue, 10);
              if (!Number.isInteger(parsedValue) || parsedValue <= 0) {
                return;
              }

              setTimeRange({
                mode: "lastN",
                lastN: parsedValue,
              });
            }}
            placeholder="Ex.: 5"
            type="number"
            value={lastN ?? ""}
          />
        </div>

        <div>
          <FilterLabel htmlFor="global-filter-date-start" label="Data inicial" state={pageFilterConfig.dateRange} />
          <input
            className={`${FIELD_CLASS} [color-scheme:dark]`}
            disabled={!isDateRangeEnabled}
            id="global-filter-date-start"
            onChange={(event) =>
              setTimeRange({
                mode: "dateRange",
                dateRangeStart: parseNullableText(event.target.value),
                dateRangeEnd,
              })
            }
            type="date"
            value={dateRangeStart ?? ""}
          />
        </div>

        <div>
          <FilterLabel htmlFor="global-filter-date-end" label="Data final" state={pageFilterConfig.dateRange} />
          <input
            className={`${FIELD_CLASS} [color-scheme:dark]`}
            disabled={!isDateRangeEnabled}
            id="global-filter-date-end"
            onChange={(event) =>
              setTimeRange({
                mode: "dateRange",
                dateRangeStart,
                dateRangeEnd: parseNullableText(event.target.value),
              })
            }
            type="date"
            value={dateRangeEnd ?? ""}
          />
        </div>
      </div>

      <div className="mt-3 flex justify-end">
        <button
          className="rounded-md border border-slate-600 bg-slate-800 px-3 py-1.5 text-xs font-semibold text-slate-200 transition-colors hover:border-slate-500 hover:bg-slate-700"
          onClick={() => {
            reset();
          }}
          type="button"
        >
          Limpar filtros
        </button>
      </div>
    </section>
  );
}
