import type { ApiResponse } from "@/shared/types/api-response.types";
import { apiRequest, type QueryParams } from "@/shared/services/api-client";

import type { MatchCenterData, MatchCenterFilters, MatchesListData, MatchesListFilters } from "@/features/matches/types";

export const MATCHES_ENDPOINTS = {
  list: "/api/v1/matches",
  center: (matchId: string) => `/api/v1/matches/${matchId}`,
} as const;

function toQueryParams<TFilters extends object>(filters: TFilters): QueryParams {
  const queryParams: QueryParams = {};

  for (const [key, value] of Object.entries(filters as Record<string, unknown>)) {
    if (value === null || value === undefined) {
      continue;
    }

    if (typeof value === "string" && value.trim().length === 0) {
      continue;
    }

    if (key === "venue" && value === "all") {
      continue;
    }

    const normalizedKey = key === "dateRangeStart" ? "dateStart" : key === "dateRangeEnd" ? "dateEnd" : key;
    queryParams[normalizedKey] = value as string | number | boolean;
  }

  return queryParams;
}

export async function fetchMatchesList(filters: MatchesListFilters = {}): Promise<ApiResponse<MatchesListData>> {
  return apiRequest<ApiResponse<MatchesListData>>(MATCHES_ENDPOINTS.list, {
    method: "GET",
    params: toQueryParams(filters),
  });
}

export async function fetchMatchCenter(
  matchId: string,
  filters: MatchCenterFilters = {},
): Promise<ApiResponse<MatchCenterData>> {
  return apiRequest<ApiResponse<MatchCenterData>>(MATCHES_ENDPOINTS.center(matchId), {
    method: "GET",
    params: toQueryParams(filters),
  });
}
