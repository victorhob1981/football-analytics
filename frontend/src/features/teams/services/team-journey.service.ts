import { apiRequest, type QueryParams } from "@/shared/services/api-client";
import type { ApiResponse } from "@/shared/types/api-response.types";

import type { TeamJourneyHistoryData, TeamJourneyHistoryFilters } from "@/features/teams/types";

const TEAM_JOURNEY_HISTORY_ENDPOINT = "/api/v1/team-journey-history";

function toQueryParams(filters: TeamJourneyHistoryFilters): QueryParams {
  const queryParams: QueryParams = {};

  for (const [key, value] of Object.entries(filters)) {
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

export async function fetchTeamJourneyHistory(
  filters: TeamJourneyHistoryFilters,
): Promise<ApiResponse<TeamJourneyHistoryData>> {
  return apiRequest<ApiResponse<TeamJourneyHistoryData>>(TEAM_JOURNEY_HISTORY_ENDPOINT, {
    method: "GET",
    params: toQueryParams(filters),
  });
}
