import type { ApiResponse } from "@/shared/types/api-response.types";
import { apiRequest, type QueryParams } from "@/shared/services/api-client";

import type {
  CoverageSummaryData,
  HomeGlobalFilters,
  LeagueKpiData,
  StandingsEvolutionData,
  TopPlayersData,
  TopTeamsData,
} from "@/features/home/types";

export const HOME_ENDPOINTS = {
  overview: "/api/v1/home/overview",
  topTeams: "/api/v1/home/top-teams",
  standingsEvolution: "/api/v1/home/standings-evolution",
  topPlayers: "/api/v1/home/top-players",
  coverageSummary: "/api/v1/home/coverage",
} as const;

function toQueryParams(filters: HomeGlobalFilters): QueryParams {
  const params: QueryParams = {};

  for (const [key, value] of Object.entries(filters as Record<string, unknown>)) {
    if (value === null || value === undefined) continue;
    if (typeof value === "string" && value.trim().length === 0) continue;
    if (key === "venue" && value === "all") continue;

    const normalizedKey =
      key === "dateRangeStart" ? "dateStart" : key === "dateRangeEnd" ? "dateEnd" : key;
    params[normalizedKey] = value as string | number | boolean;
  }

  return params;
}

export async function fetchLeagueKpi(
  filters: HomeGlobalFilters = {},
): Promise<ApiResponse<LeagueKpiData>> {
  return apiRequest<ApiResponse<LeagueKpiData>>(HOME_ENDPOINTS.overview, {
    method: "GET",
    params: toQueryParams(filters),
  });
}

export async function fetchTopTeams(
  filters: HomeGlobalFilters = {},
): Promise<ApiResponse<TopTeamsData>> {
  return apiRequest<ApiResponse<TopTeamsData>>(HOME_ENDPOINTS.topTeams, {
    method: "GET",
    params: toQueryParams(filters),
  });
}

export async function fetchStandingsEvolution(
  filters: HomeGlobalFilters = {},
): Promise<ApiResponse<StandingsEvolutionData>> {
  return apiRequest<ApiResponse<StandingsEvolutionData>>(HOME_ENDPOINTS.standingsEvolution, {
    method: "GET",
    params: toQueryParams(filters),
  });
}

export async function fetchTopPlayers(
  filters: HomeGlobalFilters = {},
): Promise<ApiResponse<TopPlayersData>> {
  return apiRequest<ApiResponse<TopPlayersData>>(HOME_ENDPOINTS.topPlayers, {
    method: "GET",
    params: toQueryParams(filters),
  });
}

export async function fetchCoverageSummary(
  filters: HomeGlobalFilters = {},
): Promise<ApiResponse<CoverageSummaryData>> {
  return apiRequest<ApiResponse<CoverageSummaryData>>(HOME_ENDPOINTS.coverageSummary, {
    method: "GET",
    params: toQueryParams(filters),
  });
}
