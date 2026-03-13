import type { ApiResponse } from "@/shared/types/api-response.types";
import { apiRequest, type QueryParams } from "@/shared/services/api-client";
import type { H2HFilters, H2HFixture, H2HSummary } from "@/features/head-to-head/types";

export const H2H_ENDPOINTS = {
    summary: "/api/v1/head-to-head/summary",
    fixtures: "/api/v1/head-to-head/fixtures",
} as const;

function toQueryParams<TFilters extends object>(filters: TFilters): QueryParams {
    const params: QueryParams = {};
    for (const [key, value] of Object.entries(filters as Record<string, unknown>)) {
        if (value === null || value === undefined) continue;
        if (typeof value === "string" && value.trim().length === 0) continue;
        const normalizedKey =
            key === "dateRangeStart" ? "dateStart" :
                key === "dateRangeEnd" ? "dateEnd" : key;
        params[normalizedKey] = value as string | number | boolean;
    }
    return params;
}

export async function fetchH2HSummary(filters: H2HFilters): Promise<ApiResponse<H2HSummary>> {
    return apiRequest<ApiResponse<H2HSummary>>(H2H_ENDPOINTS.summary, {
        method: "GET",
        params: toQueryParams(filters),
    });
}

export async function fetchH2HFixtures(
    filters: H2HFilters,
): Promise<ApiResponse<{ items: H2HFixture[] }>> {
    return apiRequest<ApiResponse<{ items: H2HFixture[] }>>(H2H_ENDPOINTS.fixtures, {
        method: "GET",
        params: toQueryParams(filters),
    });
}
