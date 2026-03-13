import type { VenueFilter } from "@/shared/types/filters.types";

// ── Entidades base ────────────────────────────────────────────────────────────

export interface Coach {
    coachId: string;
    coachName: string;
    nationality?: string | null;
    photoUrl?: string | null;
}

export interface CoachListItem extends Coach {
    currentTeamId?: string | null;
    currentTeamName?: string | null;
    matchesManaged?: number | null;
    wins?: number | null;
    draws?: number | null;
    losses?: number | null;
    winRate?: number | null;
    goalsForPerGame?: number | null;
}

export interface CoachProfile {
    coach: Coach;
    performance?: CoachPerformanceSummary;
}

export interface CoachPerformanceSummary {
    matchesManaged?: number | null;
    wins?: number | null;
    draws?: number | null;
    losses?: number | null;
    winRate?: number | null;
    goalsForPerGame?: number | null;
    goalsAgainstPerGame?: number | null;
}

export interface CoachesListData {
    items: CoachListItem[];
}

// ── Filtros ───────────────────────────────────────────────────────────────────

export interface CoachesGlobalFilters {
    competitionId?: string | null;
    seasonId?: string | null;
    lastN?: number | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
}

export interface CoachesListLocalFilters {
    search?: string;
    orderBy?: "winRate" | "goalsForPerGame" | "wins";
    page?: number;
    pageSize?: number;
}

export interface CoachProfileLocalFilters {
    includeHistory?: boolean;
}

export type CoachesListFilters = CoachesGlobalFilters & CoachesListLocalFilters;
export type CoachProfileFilters = CoachesGlobalFilters & CoachProfileLocalFilters;
