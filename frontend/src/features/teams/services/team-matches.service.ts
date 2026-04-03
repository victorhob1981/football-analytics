import type { ApiResponse } from "@/shared/types/api-response.types";
import { apiRequest, type QueryParams } from "@/shared/services/api-client";

import type { TeamMatchesListData, TeamMatchesListFilters } from "@/features/teams/types";

const TEAM_MATCHES_ENDPOINT = "/api/v1/matches";

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

    const normalizedKey =
      key === "dateRangeStart" ? "dateStart" : key === "dateRangeEnd" ? "dateEnd" : key;
    queryParams[normalizedKey] = value as string | number | boolean;
  }

  return queryParams;
}

export async function fetchTeamMatchesList(
  filters: TeamMatchesListFilters = {},
): Promise<ApiResponse<TeamMatchesListData>> {
  return apiRequest<ApiResponse<TeamMatchesListData>>(TEAM_MATCHES_ENDPOINT, {
    method: "GET",
    params: toQueryParams(filters),
  });
}
