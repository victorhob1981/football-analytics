import type { VenueFilter } from "@/shared/types/filters.types";

// ── Entidades base ────────────────────────────────────────────────────────────

export interface Club {
    teamId: string;
    teamName: string;
    shortName?: string | null;
    logoUrl?: string | null;
}

export interface ClubListItem extends Club {
    position?: number | null;
    points?: number | null;
    matchesPlayed?: number | null;
    wins?: number | null;
    draws?: number | null;
    losses?: number | null;
    goalsFor?: number | null;
    goalsAgainst?: number | null;
    goalDifference?: number | null;
    recentForm?: string[] | null; // ex: ["W", "D", "L", "W", "W"]
}

export interface ClubProfile {
    club: Club;
    season?: ClubSeasonSummary;
}

export interface ClubSeasonSummary {
    points?: number | null;
    matchesPlayed?: number | null;
    wins?: number | null;
    draws?: number | null;
    losses?: number | null;
    goalsFor?: number | null;
    goalsAgainst?: number | null;
    goalDifference?: number | null;
    winRate?: number | null;
}

export interface ClubsListData {
    items: ClubListItem[];
}

// ── Filtros ───────────────────────────────────────────────────────────────────

export interface ClubsGlobalFilters {
    competitionId?: string | null;
    seasonId?: string | null;
    roundId?: string | null;
    monthKey?: string | null;
    venue?: VenueFilter;
    lastN?: number | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
}

export interface ClubsListLocalFilters {
    search?: string;
    orderBy?: "points" | "goalsFor" | "goalsAgainst";
    page?: number;
    pageSize?: number;
}

export interface ClubProfileLocalFilters {
    includeRecentMatches?: boolean;
}

export type ClubsListFilters = ClubsGlobalFilters & ClubsListLocalFilters;
export type ClubProfileFilters = ClubsGlobalFilters & ClubProfileLocalFilters;
