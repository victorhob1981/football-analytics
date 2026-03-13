import type { ApiResponse } from "@/shared/types/api-response.types";
import { apiRequest, type QueryParams } from "@/shared/services/api-client";
import type { AuditCoverageData, AuditFilters, AuditSyncData, MetricCoverageData } from "@/features/audit/types";

export const AUDIT_ENDPOINTS = {
    coverage: "/api/v1/audit/coverage",
    metricCoverage: "/api/v1/audit/metric-coverage",
    syncState: "/api/v1/audit/sync-state",
} as const;

function toQueryParams<TFilters extends object>(filters: TFilters): QueryParams {
    const params: QueryParams = {};
    for (const [key, value] of Object.entries(filters as Record<string, unknown>)) {
        if (value === null || value === undefined) continue;
        if (typeof value === "string" && value.trim().length === 0) continue;
        params[key] = value as string | number | boolean;
    }
    return params;
}

export async function fetchAuditCoverage(filters: AuditFilters = {}): Promise<ApiResponse<AuditCoverageData>> {
    return apiRequest<ApiResponse<AuditCoverageData>>(AUDIT_ENDPOINTS.coverage, {
        method: "GET",
        params: toQueryParams(filters),
    });
}

export async function fetchMetricCoverage(filters: AuditFilters = {}): Promise<ApiResponse<MetricCoverageData>> {
    return apiRequest<ApiResponse<MetricCoverageData>>(AUDIT_ENDPOINTS.metricCoverage, {
        method: "GET",
        params: toQueryParams(filters),
    });
}

export async function fetchSyncState(filters: AuditFilters = {}): Promise<ApiResponse<AuditSyncData>> {
    return apiRequest<ApiResponse<AuditSyncData>>(AUDIT_ENDPOINTS.syncState, {
        method: "GET",
        params: toQueryParams(filters),
    });
}
