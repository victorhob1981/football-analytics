import type { ApiResponse } from "@/shared/types/api-response.types";
import { apiRequest, type QueryParams } from "@/shared/services/api-client";

import type {
  CoachProfile,
  CoachProfileFilters,
  CoachesListData,
  CoachesListFilters,
} from "@/features/coaches/types";

export const COACHES_ENDPOINTS = {
  list: "/api/v1/coaches",
  profile: (coachId: string) => `/api/v1/coaches/${coachId}`,
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

export async function fetchCoachesList(
  filters: CoachesListFilters = {},
): Promise<ApiResponse<CoachesListData>> {
  return apiRequest<ApiResponse<CoachesListData>>(COACHES_ENDPOINTS.list, {
    method: "GET",
    params: toQueryParams(filters),
  });
}

export async function fetchCoachProfile(
  coachId: string,
  filters: CoachProfileFilters = {},
): Promise<ApiResponse<CoachProfile>> {
  return apiRequest<ApiResponse<CoachProfile>>(COACHES_ENDPOINTS.profile(coachId), {
    method: "GET",
    params: toQueryParams(filters),
  });
}
