import type { ApiResponse } from "@/shared/types/api-response.types";
import { apiRequest, type QueryParams } from "@/shared/services/api-client";
import type { MarketFilters, SidelinedListData, TransfersListData } from "@/features/market/types";

export const MARKET_ENDPOINTS = {
    transfers: "/api/v1/market/transfers",
    sidelined: "/api/v1/market/sidelined",
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

export async function fetchTransfers(filters: MarketFilters = {}): Promise<ApiResponse<TransfersListData>> {
    return apiRequest<ApiResponse<TransfersListData>>(MARKET_ENDPOINTS.transfers, {
        method: "GET",
        params: toQueryParams(filters),
    });
}

export async function fetchSidelined(filters: MarketFilters = {}): Promise<ApiResponse<SidelinedListData>> {
    return apiRequest<ApiResponse<SidelinedListData>>(MARKET_ENDPOINTS.sidelined, {
        method: "GET",
        params: toQueryParams(filters),
    });
}
