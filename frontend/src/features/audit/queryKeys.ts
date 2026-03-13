import { buildQueryKey } from "@/shared/utils/queryKeys";
import type { AuditFilters } from "@/features/audit/types";

const AUDIT_DOMAIN = "audit";

export const auditQueryKeys = {
    all: () => buildQueryKey(AUDIT_DOMAIN, "all"),
    coverage: (filters: AuditFilters) => buildQueryKey(AUDIT_DOMAIN, "coverage", filters),
    metricCoverage: (filters: AuditFilters) =>
        buildQueryKey(AUDIT_DOMAIN, "metric-coverage", filters),
    syncState: (filters: AuditFilters) => buildQueryKey(AUDIT_DOMAIN, "sync-state", filters),
};
