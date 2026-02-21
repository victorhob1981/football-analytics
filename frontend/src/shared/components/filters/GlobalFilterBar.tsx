"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { usePathname, useRouter, useSearchParams } from "next/navigation";

import { useGlobalFilters } from "@/shared/hooks/useGlobalFilters";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import type { GlobalFiltersState, VenueFilter } from "@/shared/types/filters.types";

const FILTER_QUERY_KEYS = [
  "competitionId",
  "seasonId",
  "roundId",
  "venue",
  "lastN",
  "dateRangeStart",
  "dateRangeEnd",
] as const;

type SearchParamsLike = Pick<URLSearchParams, "get">;

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

function parseFiltersFromSearchParams(searchParams: SearchParamsLike): GlobalFiltersState {
  const competitionId = parseNullableText(searchParams.get("competitionId"));
  const seasonId = parseNullableText(searchParams.get("seasonId"));
  const roundId = parseNullableText(searchParams.get("roundId"));
  const venue = parseVenue(searchParams.get("venue"));
  const lastN = parseLastN(searchParams.get("lastN"));
  const dateRangeStartFromQuery = parseNullableText(searchParams.get("dateRangeStart"));
  const dateRangeEndFromQuery = parseNullableText(searchParams.get("dateRangeEnd"));
  const hasLastN = lastN !== null;

  return {
    competitionId,
    seasonId,
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
    a.roundId === b.roundId &&
    a.venue === b.venue &&
    a.lastN === b.lastN &&
    a.dateRangeStart === b.dateRangeStart &&
    a.dateRangeEnd === b.dateRangeEnd
  );
}

function upsertQueryParams(currentSearchParams: URLSearchParams, filters: GlobalFiltersState): URLSearchParams {
  const nextSearchParams = new URLSearchParams(currentSearchParams.toString());

  for (const key of FILTER_QUERY_KEYS) {
    nextSearchParams.delete(key);
  }

  if (filters.competitionId) {
    nextSearchParams.set("competitionId", filters.competitionId);
  }

  if (filters.seasonId) {
    nextSearchParams.set("seasonId", filters.seasonId);
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

export function GlobalFilterBar() {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const hasInitializedFromUrlRef = useRef(false);
  const [isUrlHydrated, setIsUrlHydrated] = useState(false);

  const {
    competitionId,
    seasonId,
    roundId,
    venue,
    lastN,
    dateRangeStart,
    dateRangeEnd,
    setCompetitionId,
    setSeasonId,
    setRoundId,
    setVenue,
    setTimeRange,
    reset,
  } = useGlobalFilters();

  const { activeMode } = useTimeRange();

  const currentFilters = useMemo(
    () => ({
      competitionId,
      seasonId,
      roundId,
      venue,
      lastN,
      dateRangeStart,
      dateRangeEnd,
    }),
    [competitionId, dateRangeEnd, dateRangeStart, lastN, roundId, seasonId, venue],
  );

  useEffect(() => {
    if (hasInitializedFromUrlRef.current) {
      return;
    }

    const parsedFilters = parseFiltersFromSearchParams(searchParams);

    if (!areFiltersEqual(currentFilters, parsedFilters)) {
      setCompetitionId(parsedFilters.competitionId);
      setSeasonId(parsedFilters.seasonId);
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

    hasInitializedFromUrlRef.current = true;
    setIsUrlHydrated(true);
  }, [
    currentFilters,
    searchParams,
    setCompetitionId,
    setRoundId,
    setSeasonId,
    setTimeRange,
    setVenue,
  ]);

  useEffect(() => {
    if (!hasInitializedFromUrlRef.current || !isUrlHydrated) {
      return;
    }

    const currentSearchParams = new URLSearchParams(searchParams.toString());
    const nextSearchParams = upsertQueryParams(currentSearchParams, currentFilters);
    const currentQuery = currentSearchParams.toString();
    const nextQuery = nextSearchParams.toString();

    if (currentQuery === nextQuery) {
      return;
    }

    const nextUrl = nextQuery.length > 0 ? `${pathname}?${nextQuery}` : pathname;
    router.replace(nextUrl, { scroll: false });
  }, [currentFilters, isUrlHydrated, pathname, router, searchParams]);

  return (
    <section aria-label="GlobalFilterBar">
      <h2>Global Filters (placeholder)</h2>

      <div>
        <label htmlFor="global-filter-competition-id">competitionId</label>
        <input
          id="global-filter-competition-id"
          onChange={(event) => setCompetitionId(parseNullableText(event.target.value))}
          placeholder="competitionId"
          type="text"
          value={competitionId ?? ""}
        />
      </div>

      <div>
        <label htmlFor="global-filter-season-id">seasonId</label>
        <input
          id="global-filter-season-id"
          onChange={(event) => setSeasonId(parseNullableText(event.target.value))}
          placeholder="seasonId"
          type="text"
          value={seasonId ?? ""}
        />
      </div>

      <div>
        <label htmlFor="global-filter-round-id">roundId</label>
        <input
          id="global-filter-round-id"
          onChange={(event) => setRoundId(parseNullableText(event.target.value))}
          placeholder="roundId"
          type="text"
          value={roundId ?? ""}
        />
      </div>

      <div>
        <label htmlFor="global-filter-venue">venue</label>
        <select
          id="global-filter-venue"
          onChange={(event) => setVenue(event.target.value as VenueFilter)}
          value={venue}
        >
          <option value="all">all</option>
          <option value="home">home</option>
          <option value="away">away</option>
        </select>
      </div>

      <div>
        <label htmlFor="global-filter-last-n">lastN (mutualmente exclusivo com dateRange)</label>
        <input
          id="global-filter-last-n"
          min={1}
          onChange={(event) => {
            const value = event.target.value.trim();

            if (value.length === 0) {
              setTimeRange({ mode: "lastN", lastN: null });
              return;
            }

            const parsedValue = Number.parseInt(value, 10);

            if (!Number.isInteger(parsedValue) || parsedValue <= 0) {
              return;
            }

            setTimeRange({ mode: "lastN", lastN: parsedValue });
          }}
          placeholder="ex.: 5"
          type="number"
          value={lastN ?? ""}
        />
      </div>

      <div>
        <label htmlFor="global-filter-date-start">dateRangeStart</label>
        <input
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
        <label htmlFor="global-filter-date-end">dateRangeEnd</label>
        <input
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

      <div>
        <p>Active time range mode: {activeMode}</p>
        <button
          onClick={() => {
            reset();
          }}
          type="button"
        >
          Reset filters
        </button>
      </div>
    </section>
  );
}
