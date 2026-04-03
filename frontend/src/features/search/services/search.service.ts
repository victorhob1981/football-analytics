import { apiRequest, type QueryParams } from "@/shared/services/api-client";
import type { ApiResponse } from "@/shared/types/api-response.types";

import type { GlobalSearchData, GlobalSearchFilters } from "@/features/search/types";

const SEARCH_ENDPOINT = "/api/v1/search";

function toQueryParams(filters: GlobalSearchFilters): QueryParams {
  const queryParams: QueryParams = {
    q: filters.q,
  };

  if (filters.types && filters.types.length > 0) {
    queryParams.types = filters.types;
  }

  if (filters.competitionId) {
    queryParams.competitionId = filters.competitionId;
  }

  if (filters.seasonId) {
    queryParams.seasonId = filters.seasonId;
  }

  if (typeof filters.limitPerType === "number") {
    queryParams.limitPerType = filters.limitPerType;
  }

  return queryParams;
}

export async function fetchGlobalSearch(
  filters: GlobalSearchFilters,
): Promise<ApiResponse<GlobalSearchData>> {
  return apiRequest<ApiResponse<GlobalSearchData>>(SEARCH_ENDPOINT, {
    method: "GET",
    params: toQueryParams(filters),
  });
}
