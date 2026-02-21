import { apiRequest, type QueryParams } from "@/shared/services/api-client";
import type { InsightFilters, InsightQueryInput, InsightResponse } from "@/shared/types/insight.types";

const INSIGHTS_ENDPOINT = "/api/v1/insights";

function appendFiltersToQueryParams(queryParams: QueryParams, filters: InsightFilters | undefined): void {
  if (!filters) {
    return;
  }

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
}

function toInsightsQueryParams(input: InsightQueryInput): QueryParams {
  const queryParams: QueryParams = {
    entityType: input.entityType,
  };

  const normalizedEntityId = input.entityId?.trim();

  if (normalizedEntityId) {
    queryParams.entityId = normalizedEntityId;
  }

  appendFiltersToQueryParams(queryParams, input.filters);

  return queryParams;
}

export async function fetchInsights(input: InsightQueryInput): Promise<InsightResponse> {
  return apiRequest<InsightResponse>(INSIGHTS_ENDPOINT, {
    method: "GET",
    params: toInsightsQueryParams(input),
  });
}

export const INSIGHTS_SERVICE_ENDPOINTS = {
  insights: INSIGHTS_ENDPOINT,
} as const;
