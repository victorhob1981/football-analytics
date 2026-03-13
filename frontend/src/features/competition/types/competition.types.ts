// ── Competition ───────────────────────────────────────────────────────────────

export interface Competition {
    competitionId: string;
    competitionName: string;
    country?: string | null;
    logoUrl?: string | null;
}

export interface Season {
    seasonId: string;
    seasonName: string;
    currentRound?: number | null;
    totalRounds?: number | null;
}

export interface Round {
    roundId: string;
    roundNumber: number;
    name?: string | null;
}

export interface StandingsSnapshot {
    teamId: string;
    teamName: string;
    logoUrl?: string | null;
    position: number;
    points: number;
    matchesPlayed: number;
    wins: number;
    draws: number;
    losses: number;
    goalsFor: number;
    goalsAgainst: number;
    goalDifference: number;
    recentForm?: string[] | null;
    zone?: "title" | "championship" | "libertadores" | "sulamericana" | "relegation" | null;
}

export interface StandingsEvolutionPoint {
    teamId: string;
    teamName: string;
    roundNumber: number;
    points: number;
    position: number;
}

export interface FixtureCard {
    fixtureId: string;
    homeTeamId: string;
    homeTeamName: string;
    awayTeamId: string;
    awayTeamName: string;
    homeGoals?: number | null;
    awayGoals?: number | null;
    playedAt?: string | null;
    roundId?: string | null;
    status?: string | null;
}

// ── Filtros ───────────────────────────────────────────────────────────────────

export interface CompetitionFilters {
    competitionId?: string | null;
    seasonId?: string | null;
    roundId?: string | null;
    monthKey?: string | null;
    lastN?: number | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
}
