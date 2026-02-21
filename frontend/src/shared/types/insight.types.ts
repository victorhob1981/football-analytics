import type { ApiResponse } from "@/shared/types/api-response.types";
import type { VenueFilter } from "@/shared/types/filters.types";

export type InsightSeverity = "info" | "warning" | "critical";

export type InsightEntityType = "player" | "team" | "match" | "competition" | "global";

export interface InsightObject {
  insight_id: string;
  severity: InsightSeverity;
  explanation: string;
  evidences: Record<string, number>;
  reference_period: string;
  data_source: string[];
}

export interface InsightFilters {
  competitionId?: string | null;
  seasonId?: string | null;
  roundId?: string | null;
  venue?: VenueFilter;
  lastN?: number | null;
  dateRangeStart?: string | null;
  dateRangeEnd?: string | null;
}

export interface InsightQueryInput {
  entityType: InsightEntityType;
  entityId?: string | null;
  filters?: InsightFilters;
}

export type InsightResponse = ApiResponse<InsightObject[]>;
