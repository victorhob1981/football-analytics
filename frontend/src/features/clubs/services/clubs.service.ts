import type { ApiResponse } from "@/shared/types/api-response.types";
import { apiRequest, type QueryParams } from "@/shared/services/api-client";
import type { ClubProfile, ClubProfileFilters, ClubsListData, ClubsListFilters } from "@/features/clubs/types";

export const CLUBS_ENDPOINTS = {
    list: "/api/v1/clubs",
    profile: (clubId: string) => `/api/v1/clubs/${clubId}`,
} as const;

function toQueryParams<TFilters extends object>(filters: TFilters): QueryParams {
    const params: QueryParams = {};
    for (const [key, value] of Object.entries(filters as Record<string, unknown>)) {
        if (value === null || value === undefined) continue;
        if (typeof value === "string" && value.trim().length === 0) continue;
        if (key === "venue" && value === "all") continue;
        const normalizedKey =
            key === "dateRangeStart" ? "dateStart" :
                key === "dateRangeEnd" ? "dateEnd" :
                    key === "monthKey" ? "month" : key;
        params[normalizedKey] = value as string | number | boolean;
    }
    return params;
}

export async function fetchClubsList(filters: ClubsListFilters = {}): Promise<ApiResponse<ClubsListData>> {
    return apiRequest<ApiResponse<ClubsListData>>(CLUBS_ENDPOINTS.list, {
        method: "GET",
        params: toQueryParams(filters),
    });
}

export async function fetchClubProfile(
    clubId: string,
    filters: ClubProfileFilters = {},
): Promise<ApiResponse<ClubProfile>> {
    return apiRequest<ApiResponse<ClubProfile>>(CLUBS_ENDPOINTS.profile(clubId), {
        method: "GET",
        params: toQueryParams(filters),
    });
}
