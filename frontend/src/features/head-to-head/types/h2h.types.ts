// ── Head-to-Head ──────────────────────────────────────────────────────────────

export interface H2HSummary {
    teamAId: string;
    teamAName: string;
    teamBId: string;
    teamBName: string;
    teamAWins: number;
    draws: number;
    teamBWins: number;
    teamAGoals: number;
    teamBGoals: number;
    totalMatches: number;
    dominanceIndex?: number | null;
    dominanceLabel?: "equilibrado" | "vantagem_leve" | "dominancia_forte" | null;
}

export interface H2HFixture {
    fixtureId?: string | null;
    playedAt?: string | null;
    competitionName?: string | null;
    venue?: "home" | "away" | "neutral" | null;
    teamAGoals?: number | null;
    teamBGoals?: number | null;
    result?: "teamA" | "teamB" | "draw" | null;
}

// ── Filtros ───────────────────────────────────────────────────────────────────

export interface H2HFilters {
    teamAId: string;
    teamBId: string;
    competitionId?: string | null;
    seasonId?: string | null;
    lastN?: number | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
}
