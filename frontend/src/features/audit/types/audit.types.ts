// ── Audit — Cobertura de Dados ────────────────────────────────────────────────

export type CoverageStatus = "ok" | "partial" | "critical" | "missing";

export interface CoverageModule {
    moduleKey: string;
    label: string;
    coveragePct: number;
    status: CoverageStatus;
    fixturesTotal: number;
    fixturesCovered: number;
}

export interface AuditCoverageData {
    modules: CoverageModule[];
}

export interface MetricCoverageRow {
    developerName: string;
    coveragePct: number;
    affectedModules: string[];
    status: "materialized" | "pending" | "missing";
}

export interface MetricCoverageData {
    rows: MetricCoverageRow[];
}

export interface SyncStateRow {
    entity: string;
    lastSync?: string | null;
    status: "ok" | "delayed" | "error";
    volumeCount?: number | null;
}

export interface AuditSyncData {
    rows: SyncStateRow[];
}

// ── Filtros ───────────────────────────────────────────────────────────────────

export interface AuditFilters {
    competitionId?: string | null;
    seasonId?: string | null;
    roundId?: string | null;
}
