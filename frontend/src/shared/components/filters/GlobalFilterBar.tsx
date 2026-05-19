"use client";

import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { usePathname, useRouter, useSearchParams } from "next/navigation";

import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";
import { apiRequest } from "@/shared/services/api-client";
import { useGlobalFilters } from "@/shared/hooks/useGlobalFilters";
import { useGlobalFiltersStore } from "@/shared/stores/globalFilters.store";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import type { GlobalFiltersState, VenueFilter } from "@/shared/types/filters.types";
import { getCompetitionById, SUPPORTED_COMPETITIONS } from "@/config/competitions.registry";
import {
  getSeasonById,
  getSeasonByQueryId,
  listSeasonsForCompetition,
  resolveSeasonForCompetition,
  SUPPORTED_SEASONS,
} from "@/config/seasons.registry";
import {
  buildCompetitionHubPath,
  buildSeasonHubPath,
  getContextQueryKeysToLockForPath,
  getContextQueryKeysToOmitForPath,
  resolveCompetitionSeasonContextFromPathname,
} from "@/shared/utils/context-routing";
import { describeTimeWindowLabel, describeVenueLabel } from "@/shared/utils/filter-descriptions";

const FILTER_QUERY_KEYS = [
  "competitionId",
  "seasonId",
  "teamId",
  "roundId",
  "venue",
  "lastN",
  "dateRangeStart",
  "dateRangeEnd",
] as const;

type SearchParamsLike = Pick<URLSearchParams, "get">;
type SeasonSelectOption = {
  value: string;
  label: string;
};
type TeamSelectOption = {
  teamId: string;
  teamName: string;
};
type TeamFilterOptionsData = {
  items: TeamSelectOption[];
};

type TeamFilterOptionsApiResponse =
  | {
      data?: TeamFilterOptionsData | TeamSelectOption[] | null;
    }
  | undefined;

function isSeasonHubRootPathname(pathname: string): boolean {
  return /^\/competitions\/[^/]+\/seasons\/[^/]+\/?$/.test(pathname);
}

function describeFilterMode(mode: string): string {
  if (mode === "dateRange") {
    return "Intervalo de datas";
  }

  if (mode === "lastN") {
    return "Últimas partidas";
  }

  return "Temporada inteira";
}

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

function buildSeasonOptions(
  competitionId: string | null,
  selectedSeasonId: string | null,
): SeasonSelectOption[] {
  const competition = getCompetitionById(competitionId);

  if (competition) {
    const options = listSeasonsForCompetition(competition).map((season) => ({
      value: season.queryId,
      label: season.label,
    }));

    const selectedSeason = getSeasonByQueryId(selectedSeasonId, competition.seasonCalendar);

    if (
      selectedSeason &&
      !options.some(
        (option) =>
          option.value === selectedSeason.queryId && option.label === selectedSeason.label,
      )
    ) {
      options.unshift({
        value: selectedSeason.queryId,
        label: selectedSeason.label,
      });
    }

    return options;
  }

  const options: SeasonSelectOption[] = [];
  const seenQueryIds = new Set<string>();

  for (const season of SUPPORTED_SEASONS) {
    if (seenQueryIds.has(season.queryId)) {
      continue;
    }

    seenQueryIds.add(season.queryId);
    options.push({
      value: season.queryId,
      label: season.queryId,
    });
  }

  return options;
}

async function fetchTeamFilterOptions(filters: {
  competitionId: string;
  seasonId: string;
}): Promise<{ data: TeamFilterOptionsData }> {
  const response = await apiRequest<TeamFilterOptionsApiResponse>("/api/v1/teams", {
    method: "GET",
    params: {
      competitionId: filters.competitionId,
      seasonId: filters.seasonId,
      page: 1,
      pageSize: 64,
      sortBy: "teamName",
      sortDirection: "asc",
    },
  });

  const payload = response?.data;
  const items = Array.isArray(payload) ? payload : payload?.items;

  return {
    data: {
      items: Array.isArray(items) ? items : [],
    },
  };
}

function parseFiltersFromSearchParams(searchParams: SearchParamsLike): GlobalFiltersState {
  const competitionId = parseNullableText(searchParams.get("competitionId"));
  const seasonId = parseNullableText(searchParams.get("seasonId"));
  const teamId = parseNullableText(searchParams.get("teamId"));
  const roundId = parseNullableText(searchParams.get("roundId"));
  const venue = parseVenue(searchParams.get("venue"));
  const lastN = parseLastN(searchParams.get("lastN"));
  const dateRangeStartFromQuery = parseNullableText(searchParams.get("dateRangeStart"));
  const dateRangeEndFromQuery = parseNullableText(searchParams.get("dateRangeEnd"));
  const hasLastN = lastN !== null;

  return {
    competitionId,
    seasonId,
    teamId,
    roundId,
    venue,
    lastN,
    dateRangeStart: hasLastN ? null : dateRangeStartFromQuery,
    dateRangeEnd: hasLastN ? null : dateRangeEndFromQuery,
  };
}

function areFiltersEqual(a: GlobalFiltersState, b: GlobalFiltersState): boolean {
  return (
    a.competitionId === b.competitionId &&
    a.seasonId === b.seasonId &&
    a.teamId === b.teamId &&
    a.roundId === b.roundId &&
    a.venue === b.venue &&
    a.lastN === b.lastN &&
    a.dateRangeStart === b.dateRangeStart &&
    a.dateRangeEnd === b.dateRangeEnd
  );
}

function readGlobalFiltersSnapshot(): GlobalFiltersState {
  const state = useGlobalFiltersStore.getState();

  return {
    competitionId: state.competitionId,
    seasonId: state.seasonId,
    teamId: state.teamId,
    roundId: state.roundId,
    venue: state.venue,
    lastN: state.lastN,
    dateRangeStart: state.dateRangeStart,
    dateRangeEnd: state.dateRangeEnd,
  };
}

function upsertQueryParams(
  currentSearchParams: URLSearchParams,
  filters: GlobalFiltersState,
  omittedContextKeys: ReadonlySet<string>,
): URLSearchParams {
  const nextSearchParams = new URLSearchParams(currentSearchParams.toString());

  for (const key of FILTER_QUERY_KEYS) {
    nextSearchParams.delete(key);
  }

  if (filters.competitionId && !omittedContextKeys.has("competitionId")) {
    nextSearchParams.set("competitionId", filters.competitionId);
  }

  if (filters.seasonId && !omittedContextKeys.has("seasonId")) {
    nextSearchParams.set("seasonId", filters.seasonId);
  }

  if (filters.teamId) {
    nextSearchParams.set("teamId", filters.teamId);
  }

  if (filters.roundId) {
    nextSearchParams.set("roundId", filters.roundId);
  }

  if (filters.venue !== "all") {
    nextSearchParams.set("venue", filters.venue);
  }

  if (filters.lastN !== null) {
    nextSearchParams.set("lastN", String(filters.lastN));
  } else {
    if (filters.dateRangeStart) {
      nextSearchParams.set("dateRangeStart", filters.dateRangeStart);
    }

    if (filters.dateRangeEnd) {
      nextSearchParams.set("dateRangeEnd", filters.dateRangeEnd);
    }
  }

  return nextSearchParams;
}

function joinClasses(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

function SummaryChip({
  label,
  value,
  tone = "default",
}: {
  label: string;
  value: string;
  tone?: "default" | "locked";
}) {
  return (
    <span
      className={joinClasses(
        "inline-flex min-w-0 items-center gap-2 rounded-full border px-2.5 py-1.5 text-[0.68rem] leading-none transition-colors",
        tone === "locked"
          ? "border-[rgba(20,122,87,0.18)] bg-[rgba(227,247,238,0.95)] text-[#0b5a42]"
          : "border-[rgba(199,210,226,0.9)] bg-[rgba(255,255,255,0.94)] text-[#39485d]",
      )}
    >
      <span className="font-semibold uppercase tracking-[0.16em] text-[#6d7b8f]">{label}</span>
      <span
        className={joinClasses(
          "max-w-[14rem] truncate font-semibold",
          tone === "locked" ? "text-[#0b5a42]" : "text-[#162235]",
        )}
      >
        {value}
      </span>
    </span>
  );
}

function MicroBadge({
  children,
  tone = "default",
}: {
  children: ReactNode;
  tone?: "default" | "locked" | "active";
}) {
  return (
    <span
      className={joinClasses(
        "inline-flex items-center rounded-full px-2 py-0.5 text-[0.62rem] font-semibold uppercase tracking-[0.14em]",
        tone === "locked"
          ? "bg-[rgba(15,92,67,0.1)] text-[#0f5c43]"
          : tone === "active"
            ? "bg-[rgba(23,92,67,0.12)] text-[#184d3b]"
            : "bg-[rgba(227,232,240,0.78)] text-[#6c7890]",
      )}
    >
      {children}
    </span>
  );
}

function FieldHeader({ label, badge }: { label: string; badge?: ReactNode }) {
  return (
    <div className="mb-1.5 flex items-center justify-between gap-2">
      <span className="text-[0.7rem] font-semibold uppercase tracking-[0.16em] text-[#5f6f86]">
        {label}
      </span>
      {badge}
    </div>
  );
}

function FilterField({
  label,
  badge,
  children,
}: {
  label: string;
  badge?: ReactNode;
  children: ReactNode;
}) {
  return (
    <label className="flex min-w-0 flex-col text-sm text-[#223146]">
      <FieldHeader badge={badge} label={label} />
      {children}
    </label>
  );
}

function StaticField({
  controlId,
  controlValue,
  label,
  value,
  badge,
}: {
  controlId?: string;
  controlValue?: string;
  label: string;
  value: string;
  badge?: ReactNode;
}) {
  return (
    <div className="flex min-w-0 flex-col text-sm text-[#223146]">
      <FieldHeader badge={badge} label={label} />
      <div className="flex h-10 items-center rounded-[0.95rem] border border-[rgba(188,203,220,0.9)] bg-[linear-gradient(180deg,rgba(249,252,255,0.96)_0%,rgba(243,247,251,0.92)_100%)] px-3.5 text-sm font-medium text-[#142033] shadow-[inset_0_1px_0_rgba(255,255,255,0.92)]">
        <span className="truncate">{value}</span>
      </div>
      {controlId ? (
        <input
          aria-hidden="true"
          className="sr-only"
          disabled
          id={controlId}
          readOnly
          tabIndex={-1}
          type="text"
          value={controlValue ?? ""}
        />
      ) : null}
    </div>
  );
}

export function GlobalFilterBar() {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const hydratedUrlRef = useRef<string | null>(null);
  const pendingUrlRef = useRef<string | null>(null);
  const [isUrlHydrated, setIsUrlHydrated] = useState(false);

  const {
    competitionId,
    seasonId,
    teamId,
    roundId,
    venue,
    lastN,
    dateRangeStart,
    dateRangeEnd,
    setCompetitionId,
    setSeasonId,
    setTeamId,
    setRoundId,
    setVenue,
    setTimeRange,
  } = useGlobalFilters();

  const { activeMode } = useTimeRange();
  const omittedContextKeys = useMemo(
    () => new Set(getContextQueryKeysToOmitForPath(pathname)),
    [pathname],
  );
  const lockedContextKeys = useMemo(
    () => new Set(getContextQueryKeysToLockForPath(pathname)),
    [pathname],
  );
  const isCompetitionContextLocked = lockedContextKeys.has("competitionId");
  const isSeasonContextLocked = lockedContextKeys.has("seasonId");
  const hasLockedRouteContext = isCompetitionContextLocked || isSeasonContextLocked;
  const isCompetitionScopedPath = useMemo(
    () => /^\/competitions\/[^/]+(?:\/|$)/.test(pathname),
    [pathname],
  );
  const isCompetitionSeasonScopedPath = useMemo(
    () => /^\/competitions\/[^/]+\/seasons\/[^/]+(?:\/|$)/.test(pathname),
    [pathname],
  );
  const isSeasonHubRootPath = useMemo(() => isSeasonHubRootPathname(pathname), [pathname]);
  const pathnameContext = useMemo(
    () => resolveCompetitionSeasonContextFromPathname(pathname),
    [pathname],
  );
  const effectiveCompetitionId = competitionId ?? pathnameContext?.competitionId ?? null;
  const effectiveSeasonId = seasonId ?? pathnameContext?.seasonId ?? null;
  const selectedCompetition = useMemo(
    () => getCompetitionById(effectiveCompetitionId),
    [effectiveCompetitionId],
  );
  const selectedSeason = useMemo(() => {
    if (selectedCompetition) {
      return (
        getSeasonByQueryId(effectiveSeasonId, selectedCompetition.seasonCalendar) ??
        getSeasonById(effectiveSeasonId)
      );
    }

    return getSeasonById(effectiveSeasonId);
  }, [effectiveSeasonId, selectedCompetition]);
  const seasonOptions = useMemo(
    () => buildSeasonOptions(effectiveCompetitionId, effectiveSeasonId),
    [effectiveCompetitionId, effectiveSeasonId],
  );
  const teamsQuery = useQueryWithCoverage<TeamFilterOptionsData>({
    queryKey: [
      "global-filter-teams",
      effectiveCompetitionId ?? "none",
      effectiveSeasonId ?? "none",
    ],
    queryFn: () =>
      fetchTeamFilterOptions({
        competitionId: effectiveCompetitionId ?? "",
        seasonId: effectiveSeasonId ?? "",
      }),
    enabled: Boolean(effectiveCompetitionId && effectiveSeasonId),
    staleTime: 10 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    isDataEmpty: (data) => !Array.isArray(data.items) || data.items.length === 0,
  });
  const teamOptions = useMemo(() => teamsQuery.data?.items ?? [], [teamsQuery.data?.items]);
  const selectedTeam = useMemo(
    () => teamOptions.find((team) => team.teamId === teamId) ?? null,
    [teamId, teamOptions],
  );
  const resetButtonLabel =
    isSeasonHubRootPath || hasLockedRouteContext ? "Limpar filtros extras" : "Limpar filtros";
  const inputClasses =
    "h-10 w-full rounded-[0.95rem] border border-[rgba(197,208,223,0.95)] bg-white px-3.5 text-sm text-[#162235] shadow-[0_1px_0_rgba(255,255,255,0.9),0_12px_28px_-26px_rgba(17,28,45,0.34)] outline-none transition-[border-color,box-shadow,background-color,transform] duration-150 ease-out placeholder:text-[#90a0b5] focus:border-[#184d3b] focus:bg-white focus:shadow-[0_0_0_3px_rgba(24,77,59,0.12)] disabled:cursor-not-allowed disabled:border-[rgba(210,219,231,0.94)] disabled:bg-[rgba(242,246,251,0.96)] disabled:text-[#6d7b8f]";
  const panelClasses =
    "rounded-[1.16rem] border border-[rgba(214,223,236,0.88)] bg-[rgba(255,255,255,0.78)] p-3.5 shadow-[0_18px_40px_-38px_rgba(17,28,45,0.3)]";
  const hasTeamFilter = Boolean(teamId);
  const activeReadModeLabel =
    activeMode === "none" && roundId !== null ? "Rodada fixa" : describeFilterMode(activeMode);
  const selectedCompetitionLabel =
    selectedCompetition?.shortName ?? selectedCompetition?.name ?? "Todas as competições";
  const selectedSeasonLabel = selectedSeason?.label ?? "Todas as temporadas";
  const selectedTeamLabel = selectedTeam?.teamName ?? "Todos os clubes";
  const venueLabel = describeVenueLabel(venue);
  const activeWindowSummary =
    lastN !== null
      ? `Últimos ${lastN} jogos`
      : dateRangeStart !== null || dateRangeEnd !== null
        ? `Período ${dateRangeStart ?? "aberto"} até ${dateRangeEnd ?? "aberto"}`
        : describeTimeWindowLabel({
            roundId,
            lastN,
            dateRangeStart,
            dateRangeEnd,
          });
  const currentViewSummary = [selectedTeamLabel, venueLabel, roundId ? `Rodada ${roundId}` : null]
    .filter(Boolean)
    .join(" · ");
  const hasSeasonHubExtraFilters = Boolean(
    teamId || roundId || venue !== "all" || lastN !== null || dateRangeStart || dateRangeEnd,
  );
  const hasResettableFilters = Boolean(
    isSeasonHubRootPath
      ? hasSeasonHubExtraFilters
      : (!isCompetitionContextLocked && competitionId) ||
          (!isSeasonContextLocked && seasonId) ||
          teamId ||
          roundId ||
          venue !== "all" ||
          lastN !== null ||
          dateRangeStart ||
          dateRangeEnd,
  );

  const currentFilters = useMemo(
    () => ({
      competitionId,
      seasonId,
      roundId,
      venue,
      teamId,
      lastN,
      dateRangeStart,
      dateRangeEnd,
    }),
    [competitionId, dateRangeEnd, dateRangeStart, lastN, roundId, seasonId, teamId, venue],
  );
  const currentUrlSignature = useMemo(
    () => `${pathname}?${searchParams.toString()}`,
    [pathname, searchParams],
  );
  const replaceFiltersInUrl = useCallback(
    (overrides: Partial<GlobalFiltersState>, nextPathname?: string) => {
      const targetPathname = nextPathname ?? pathname;
      const targetOmittedContextKeys = new Set(getContextQueryKeysToOmitForPath(targetPathname));

      if (!isUrlHydrated && hydratedUrlRef.current !== currentUrlSignature) {
        hydratedUrlRef.current = currentUrlSignature;
        setIsUrlHydrated(true);
      }

      const pendingUrl = pendingUrlRef.current;
      const pendingPathname = pendingUrl?.split("?")[0] ?? null;
      const baseSearchParams =
        pendingUrl && pendingPathname === targetPathname
          ? new URLSearchParams(pendingUrl.split("?")[1] ?? "")
          : new URLSearchParams(searchParams.toString());
      const nextFilters = {
        ...readGlobalFiltersSnapshot(),
        ...overrides,
      };
      const nextSearchParams = upsertQueryParams(
        baseSearchParams,
        nextFilters,
        targetOmittedContextKeys,
      );
      const nextQuery = nextSearchParams.toString();
      const nextUrl = nextQuery.length > 0 ? `${targetPathname}?${nextQuery}` : targetPathname;

      if ((pendingUrl ?? currentUrlSignature) === nextUrl) {
        return;
      }

      pendingUrlRef.current = nextUrl;
      router.replace(nextUrl, { scroll: false });
    },
    [currentUrlSignature, isUrlHydrated, pathname, router, searchParams],
  );
  const applySeasonHubContextChange = useCallback(
    ({
      nextCompetitionId,
      nextPathname,
      nextSeasonId,
    }: {
      nextCompetitionId: string | null;
      nextPathname: string;
      nextSeasonId: string | null;
    }) => {
      setCompetitionId(nextCompetitionId);
      setSeasonId(nextSeasonId);
      if (teamId !== null) {
        setTeamId(null);
      }
      if (roundId !== null) {
        setRoundId(null);
      }
      if (venue !== "all") {
        setVenue("all");
      }
      setTimeRange({ mode: "lastN", lastN: null });
      replaceFiltersInUrl(
        {
          competitionId: nextCompetitionId,
          seasonId: nextSeasonId,
          teamId: null,
          roundId: null,
          venue: "all",
          lastN: null,
          dateRangeStart: null,
          dateRangeEnd: null,
        },
        nextPathname,
      );
    },
    [
      replaceFiltersInUrl,
      roundId,
      setCompetitionId,
      setRoundId,
      setSeasonId,
      setTeamId,
      setTimeRange,
      setVenue,
      teamId,
      venue,
    ],
  );
  const handleReset = useCallback(() => {
    if (isSeasonHubRootPath) {
      if (teamId !== null) {
        setTeamId(null);
      }
      if (roundId !== null) {
        setRoundId(null);
      }
      if (venue !== "all") {
        setVenue("all");
      }
      setTimeRange({ mode: "lastN", lastN: null });
      replaceFiltersInUrl({
        competitionId: effectiveCompetitionId,
        seasonId: effectiveSeasonId,
        teamId: null,
        roundId: null,
        venue: "all",
        lastN: null,
        dateRangeStart: null,
        dateRangeEnd: null,
      });
      return;
    }

    const nextCompetitionId = isCompetitionContextLocked ? competitionId : null;
    const nextSeasonId = isSeasonContextLocked ? seasonId : null;

    setCompetitionId(nextCompetitionId);
    setSeasonId(nextSeasonId);
    setTeamId(null);
    setRoundId(null);
    setVenue("all");
    setTimeRange({ mode: "lastN", lastN: null });
    replaceFiltersInUrl({
      competitionId: nextCompetitionId,
      seasonId: nextSeasonId,
      teamId: null,
      roundId: null,
      venue: "all",
      lastN: null,
      dateRangeStart: null,
      dateRangeEnd: null,
    });
  }, [
    competitionId,
    effectiveCompetitionId,
    effectiveSeasonId,
    isSeasonHubRootPath,
    isCompetitionContextLocked,
    isSeasonContextLocked,
    replaceFiltersInUrl,
    roundId,
    seasonId,
    setCompetitionId,
    setRoundId,
    setSeasonId,
    setTeamId,
    setTimeRange,
    setVenue,
    teamId,
    venue,
  ]);

  useEffect(() => {
    if (!selectedCompetition || !seasonId) {
      return;
    }

    const seasonStillAvailable = seasonOptions.some((option) => option.value === seasonId);
    if (!seasonStillAvailable) {
      setSeasonId(null);
      if (roundId !== null) {
        setRoundId(null);
      }
    }
  }, [roundId, seasonId, seasonOptions, selectedCompetition, setRoundId, setSeasonId]);

  useLayoutEffect(() => {
    if (hydratedUrlRef.current === currentUrlSignature) {
      return;
    }

    const pendingUrl = pendingUrlRef.current;
    const pendingPathname = pendingUrl?.split("?")[0] ?? null;

    if (pendingUrl && pendingPathname === pathname) {
      hydratedUrlRef.current = currentUrlSignature;
      setIsUrlHydrated(true);

      if (currentUrlSignature === pendingUrl) {
        pendingUrlRef.current = null;
      }

      return;
    }

    if (pendingUrl && pendingPathname !== pathname) {
      pendingUrlRef.current = null;
    }

    const parsedFilters = parseFiltersFromSearchParams(searchParams);
    if (pathnameContext) {
      parsedFilters.competitionId = pathnameContext.competitionId;
      parsedFilters.seasonId = pathnameContext.seasonId;
    }
    const currentStoreFilters = readGlobalFiltersSnapshot();

    if (!areFiltersEqual(currentStoreFilters, parsedFilters)) {
      setCompetitionId(parsedFilters.competitionId);
      setSeasonId(parsedFilters.seasonId);
      setTeamId(parsedFilters.teamId);
      setRoundId(parsedFilters.roundId);
      setVenue(parsedFilters.venue);

      if (parsedFilters.lastN !== null) {
        setTimeRange({ mode: "lastN", lastN: parsedFilters.lastN });
      } else if (parsedFilters.dateRangeStart !== null || parsedFilters.dateRangeEnd !== null) {
        setTimeRange({
          mode: "dateRange",
          dateRangeStart: parsedFilters.dateRangeStart,
          dateRangeEnd: parsedFilters.dateRangeEnd,
        });
      } else {
        setTimeRange({ mode: "lastN", lastN: null });
      }
    }

    hydratedUrlRef.current = currentUrlSignature;
    setIsUrlHydrated(true);
  }, [
    currentUrlSignature,
    currentFilters,
    pathnameContext,
    pathname,
    searchParams,
    setCompetitionId,
    setRoundId,
    setSeasonId,
    setTeamId,
    setTimeRange,
    setVenue,
  ]);

  useEffect(() => {
    if (!isUrlHydrated) {
      return;
    }

    const currentSearchParams = new URLSearchParams(searchParams.toString());
    const nextSearchParams = upsertQueryParams(
      currentSearchParams,
      readGlobalFiltersSnapshot(),
      omittedContextKeys,
    );
    const currentQuery = currentSearchParams.toString();
    const nextQuery = nextSearchParams.toString();

    if (currentQuery === nextQuery) {
      return;
    }

    const nextUrl = nextQuery.length > 0 ? `${pathname}?${nextQuery}` : pathname;
    pendingUrlRef.current = nextUrl;
    router.replace(nextUrl, { scroll: false });
  }, [currentFilters, isUrlHydrated, omittedContextKeys, pathname, router, searchParams]);

  useEffect(() => {
    if (!effectiveCompetitionId || !effectiveSeasonId) {
      if (teamId !== null) {
        setTeamId(null);
        replaceFiltersInUrl({ teamId: null });
      }
      return;
    }

    if (!teamId || teamOptions.length === 0) {
      return;
    }

    const teamStillAvailable = teamOptions.some((team) => team.teamId === teamId);

    if (!teamStillAvailable) {
      setTeamId(null);
      replaceFiltersInUrl({ teamId: null });
    }
  }, [
    effectiveCompetitionId,
    effectiveSeasonId,
    replaceFiltersInUrl,
    setTeamId,
    teamId,
    teamOptions,
  ]);

  if (isSeasonHubRootPath) {
    return (
      <section
        aria-label="Contexto da edição"
        data-url-hydrated={isUrlHydrated ? "true" : "false"}
        className="rounded-[1.4rem] border border-[rgba(208,220,236,0.88)] bg-[linear-gradient(180deg,rgba(252,253,255,0.98)_0%,rgba(245,248,252,0.94)_100%)] px-4 py-4 shadow-[0_20px_48px_-44px_rgba(17,28,45,0.34)]"
      >
        <div className="flex flex-col gap-3 xl:flex-row xl:items-end">
          <div className="grid flex-1 gap-3 md:grid-cols-[minmax(0,1.5fr)_220px]">
            <FilterField label="Competição">
              <select
                className={inputClasses}
                id="global-filter-competition-id"
                onChange={(event) => {
                  const nextCompetitionId = parseNullableText(event.target.value);
                  const nextCompetition = getCompetitionById(nextCompetitionId);
                  const nextSeason = nextCompetition
                    ? resolveSeasonForCompetition(nextCompetition, {
                        seasonId: effectiveSeasonId,
                        seasonLabel: selectedSeason?.label ?? effectiveSeasonId,
                      })
                    : null;

                  applySeasonHubContextChange({
                    nextCompetitionId,
                    nextPathname: nextCompetition
                      ? nextSeason
                        ? buildSeasonHubPath({
                            competitionKey: nextCompetition.key,
                            seasonLabel: nextSeason.label,
                          })
                        : buildCompetitionHubPath(nextCompetition.key)
                      : "/competitions",
                    nextSeasonId: nextSeason?.queryId ?? null,
                  });
                }}
                value={effectiveCompetitionId ?? ""}
              >
                <option value="">Todas as competições</option>
                {SUPPORTED_COMPETITIONS.map((competition) => (
                  <option key={competition.id} value={competition.id}>
                    {competition.name}
                  </option>
                ))}
              </select>
            </FilterField>

            <FilterField label="Temporada">
              <select
                className={inputClasses}
                disabled={!selectedCompetition}
                id="global-filter-season-id"
                onChange={(event) => {
                  const nextSeasonId = parseNullableText(event.target.value);
                  const nextSeason =
                    selectedCompetition && nextSeasonId
                      ? resolveSeasonForCompetition(selectedCompetition, {
                          seasonId: nextSeasonId,
                          seasonLabel:
                            getSeasonByQueryId(nextSeasonId, selectedCompetition.seasonCalendar)
                              ?.label ?? nextSeasonId,
                        })
                      : null;

                  applySeasonHubContextChange({
                    nextCompetitionId: effectiveCompetitionId,
                    nextPathname: selectedCompetition
                      ? nextSeason
                        ? buildSeasonHubPath({
                            competitionKey: selectedCompetition.key,
                            seasonLabel: nextSeason.label,
                          })
                        : buildCompetitionHubPath(selectedCompetition.key)
                      : "/competitions",
                    nextSeasonId: nextSeason?.queryId ?? null,
                  });
                }}
                value={effectiveSeasonId ?? ""}
              >
                <option value="">Todas as temporadas</option>
                {seasonOptions.map((season) => (
                  <option key={`${season.value}-${season.label}`} value={season.value}>
                    {season.label}
                  </option>
                ))}
              </select>
            </FilterField>
          </div>

          <div className="flex xl:justify-end">
            <button
              className={joinClasses(
                "inline-flex h-10 items-center justify-center rounded-[1rem] border px-4 text-sm font-semibold transition-[border-color,background-color,color] duration-150 ease-out",
                hasResettableFilters
                  ? "border-[rgba(197,208,223,0.95)] bg-white text-[#184d3b] hover:border-[rgba(24,77,59,0.28)] hover:bg-[rgba(24,77,59,0.04)]"
                  : "border-[rgba(218,226,237,0.9)] bg-[rgba(248,250,253,0.92)] text-[#91a0b4]",
              )}
              disabled={!hasResettableFilters}
              onClick={handleReset}
              type="button"
            >
              {resetButtonLabel}
            </button>
          </div>
        </div>
      </section>
    );
  }

  return (
    <section
      aria-label="Filtros do produto"
      data-url-hydrated={isUrlHydrated ? "true" : "false"}
      className="rounded-[1.45rem] border border-[rgba(208,220,236,0.88)] bg-[linear-gradient(180deg,rgba(252,253,255,0.98)_0%,rgba(245,248,252,0.92)_100%)] px-4 py-4 shadow-[0_24px_56px_-48px_rgba(17,28,45,0.42)] backdrop-blur-xl"
    >
      <div className="flex flex-col gap-3">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div className="flex min-w-0 flex-col gap-2">
            <div className="flex flex-wrap items-center gap-2">
              <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#5f6f86]">
                Recorte global
              </p>
              {hasLockedRouteContext ? <MicroBadge tone="locked">Recorte da rota</MicroBadge> : null}
              <MicroBadge tone="active">{activeReadModeLabel}</MicroBadge>
            </div>
            <p className="text-sm font-medium text-[#223146]">
              {currentViewSummary}
              <span className="text-[#73839a]"> · {activeWindowSummary}</span>
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-2 lg:justify-end">
            <button
              className={joinClasses(
                "inline-flex items-center rounded-full border px-3.5 py-2 text-sm font-semibold transition-[border-color,background-color,color,transform] duration-150 ease-out active:scale-[0.98]",
                hasResettableFilters
                  ? "border-[rgba(194,206,222,0.94)] bg-white text-[#184d3b] hover:border-[rgba(24,77,59,0.28)] hover:bg-[rgba(24,77,59,0.04)]"
                  : "border-[rgba(218,226,237,0.9)] bg-[rgba(248,250,253,0.92)] text-[#91a0b4]",
              )}
              disabled={!hasResettableFilters}
              onClick={handleReset}
              type="button"
            >
              {resetButtonLabel}
            </button>
          </div>
        </div>

        <div className="grid items-start gap-3">
          <div className={panelClasses}>
            <div className="flex flex-col gap-3">
              <div className="flex flex-col gap-2 xl:flex-row xl:items-center xl:justify-between">
                <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#5f6f86]">
                  Contexto e principais
                </p>
                <div className="flex flex-wrap gap-2">
                  <SummaryChip
                    label="Competição"
                    tone={isCompetitionContextLocked ? "locked" : "default"}
                    value={selectedCompetitionLabel}
                  />
                  <SummaryChip
                    label="Temporada"
                    tone={isSeasonContextLocked ? "locked" : "default"}
                    value={selectedSeasonLabel}
                  />
                </div>
              </div>

              <div className="grid gap-4 xl:grid-cols-[minmax(280px,0.78fr)_minmax(0,1.58fr)]">
                <div className="space-y-3 xl:border-r xl:border-[rgba(220,229,239,0.82)] xl:pr-4">
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#5f6f86]">
                      Contexto
                    </p>
                    {hasLockedRouteContext ? (
                      <MicroBadge tone="locked">Definido pela rota</MicroBadge>
                    ) : null}
                  </div>

                  <div className="grid gap-3 sm:grid-cols-2">
                    {isCompetitionContextLocked ? (
                      <StaticField
                        badge={<MicroBadge tone="locked">Fixo</MicroBadge>}
                        controlId="global-filter-competition-id"
                        controlValue={competitionId ?? ""}
                        label="Competição"
                        value={selectedCompetitionLabel}
                      />
                    ) : (
                      <FilterField label="Competição">
                        <select
                          className={inputClasses}
                          disabled={isCompetitionContextLocked}
                          id="global-filter-competition-id"
                          onChange={(event) => {
                            const nextCompetitionId = parseNullableText(event.target.value);
                            const nextCompetition = getCompetitionById(nextCompetitionId);
                            const nextSeason = nextCompetition
                              ? resolveSeasonForCompetition(nextCompetition, {
                                  seasonId,
                                  seasonLabel: selectedSeason?.label ?? seasonId,
                                })
                              : null;
                            const nextSeasonId = nextSeason?.queryId ?? null;
                            const nextPathname = !isCompetitionScopedPath
                              ? undefined
                              : nextCompetition
                                ? isCompetitionSeasonScopedPath && nextSeason
                                  ? buildSeasonHubPath({
                                      competitionKey: nextCompetition.key,
                                      seasonLabel: nextSeason.label,
                                    })
                                  : buildCompetitionHubPath(nextCompetition.key)
                                : "/competitions";

                            setCompetitionId(nextCompetitionId);
                            setSeasonId(nextSeasonId);
                            if (teamId !== null) {
                              setTeamId(null);
                            }
                            if (roundId !== null) {
                              setRoundId(null);
                            }
                            replaceFiltersInUrl({
                              competitionId: nextCompetitionId,
                              seasonId: nextSeasonId,
                              teamId: null,
                              roundId: null,
                            }, nextPathname);
                          }}
                          value={competitionId ?? ""}
                        >
                          <option value="">Todas as competições</option>
                          {SUPPORTED_COMPETITIONS.map((comp) => (
                            <option key={comp.id} value={comp.id}>
                              {comp.name}
                            </option>
                          ))}
                        </select>
                      </FilterField>
                    )}

                    {isSeasonContextLocked ? (
                      <StaticField
                        badge={<MicroBadge tone="locked">Fixa</MicroBadge>}
                        controlId="global-filter-season-id"
                        controlValue={seasonId ?? ""}
                        label="Temporada"
                        value={selectedSeasonLabel}
                      />
                    ) : (
                      <FilterField label="Temporada">
                        <select
                          className={inputClasses}
                          disabled={isSeasonContextLocked}
                          id="global-filter-season-id"
                          onChange={(event) => {
                            const nextSeasonId = parseNullableText(event.target.value);
                            const nextRoundId = roundId !== null ? null : roundId;

                            setSeasonId(nextSeasonId);
                            if (teamId !== null) {
                              setTeamId(null);
                            }
                            if (roundId !== null) {
                              setRoundId(null);
                            }
                            replaceFiltersInUrl({
                              seasonId: nextSeasonId,
                              teamId: null,
                              roundId: nextRoundId,
                            });
                          }}
                          value={seasonId ?? ""}
                        >
                          <option value="">Todas as temporadas</option>
                          {seasonOptions.map((season) => (
                            <option key={`${season.value}-${season.label}`} value={season.value}>
                              {season.label}
                            </option>
                          ))}
                        </select>
                      </FilterField>
                    )}
                  </div>
                </div>

                <div className="space-y-3 xl:pl-4">
                  <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#5f6f86]">
                    Principais
                  </p>

                  <div className="grid gap-3 md:grid-cols-[minmax(0,1.7fr)_152px_176px]">
                    {competitionId && seasonId ? (
                      <FilterField
                        badge={
                          <MicroBadge tone={selectedTeam ? "active" : "default"}>
                            {teamsQuery.isLoading
                              ? "Carregando"
                              : hasTeamFilter
                                ? "Ativo"
                                : "Opcional"}
                          </MicroBadge>
                        }
                        label="Clube"
                      >
                        <select
                          className={inputClasses}
                          disabled={teamsQuery.isLoading || teamOptions.length === 0}
                          id="global-filter-team-id"
                          onChange={(event) => {
                            const nextTeamId = parseNullableText(event.target.value);

                            setTeamId(nextTeamId);
                            if (roundId !== null) {
                              setRoundId(null);
                            }
                            setTimeRange({ mode: "lastN", lastN: null });
                            replaceFiltersInUrl({
                              teamId: nextTeamId,
                              roundId: null,
                              lastN: null,
                              dateRangeStart: null,
                              dateRangeEnd: null,
                            });
                          }}
                          value={teamId ?? ""}
                        >
                          <option value="">
                            {teamsQuery.isLoading ? "Carregando clubes..." : "Todos os clubes"}
                          </option>
                          {teamOptions.map((team) => (
                            <option key={team.teamId} value={team.teamId}>
                              {team.teamName}
                            </option>
                          ))}
                        </select>
                      </FilterField>
                    ) : (
                      <StaticField
                        badge={<MicroBadge>Base</MicroBadge>}
                        label="Clube"
                        value="Selecione competição e temporada"
                      />
                    )}

                    <FilterField
                      badge={
                        roundId ? (
                          <MicroBadge tone="active">Ativa</MicroBadge>
                        ) : (
                          <MicroBadge>Opcional</MicroBadge>
                        )
                      }
                      label="Rodada"
                    >
                      <input
                        className={inputClasses}
                        id="global-filter-round-id"
                        onChange={(event) => {
                          const nextRoundId = parseNullableText(event.target.value);
                          setRoundId(nextRoundId);
                          replaceFiltersInUrl({ roundId: nextRoundId });
                        }}
                        placeholder="Ex: 1"
                        type="text"
                        value={roundId ?? ""}
                      />
                    </FilterField>

                    <FilterField label="Mando">
                      <select
                        className={inputClasses}
                        id="global-filter-venue"
                        onChange={(event) => {
                          const nextVenue = event.target.value as VenueFilter;
                          setVenue(nextVenue);
                          replaceFiltersInUrl({ venue: nextVenue });
                        }}
                        value={venue}
                      >
                        <option value="all">Todos</option>
                        <option value="home">Casa</option>
                        <option value="away">Fora</option>
                      </select>
                    </FilterField>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
