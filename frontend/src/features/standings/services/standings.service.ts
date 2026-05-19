import { apiRequest, type QueryParams } from "@/shared/services/api-client";
import type { ApiResponse } from "@/shared/types/api-response.types";

import type { StandingsFilters, StandingsTableData } from "@/features/standings/types";

const STANDINGS_ENDPOINT = "/api/v1/standings";
const GROUP_STANDINGS_ENDPOINT = "/api/v1/group-standings";

function toQueryParams(filters: StandingsFilters = {}): QueryParams {
  const queryParams: QueryParams = {};

  for (const [key, value] of Object.entries(filters as Record<string, unknown>)) {
    if (value === null || value === undefined) {
      continue;
    }

    if (typeof value === "string" && value.trim().length === 0) {
      continue;
    }

    queryParams[key] = value as string | number | boolean;
  }

  return queryParams;
}

export async function fetchStandings(
  filters: StandingsFilters = {},
): Promise<ApiResponse<StandingsTableData>> {
  return apiRequest<ApiResponse<StandingsTableData>>(STANDINGS_ENDPOINT, {
    method: "GET",
    params: toQueryParams(filters),
  });
}

export async function fetchGroupStandings(
  filters: StandingsFilters = {},
): Promise<ApiResponse<StandingsTableData>> {
  return apiRequest<ApiResponse<StandingsTableData>>(GROUP_STANDINGS_ENDPOINT, {
    method: "GET",
    params: toQueryParams(filters),
  });
}
