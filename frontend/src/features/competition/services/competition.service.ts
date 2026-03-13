import type { ApiResponse } from "@/shared/types/api-response.types";
import { apiRequest, type QueryParams } from "@/shared/services/api-client";
import type { CompetitionFilters, FixtureCard, StandingsEvolutionPoint, StandingsSnapshot } from "@/features/competition/types";

export const COMPETITION_ENDPOINTS = {
    standings: "/api/v1/standings",
    evolution: "/api/v1/standings/evolution",
    fixtures: "/api/v1/matches",
} as const;

type MatchesFixtureItem = {
    matchId: string;
    fixtureId?: string | null;
    homeTeamId?: string | null;
    homeTeamName?: string | null;
    awayTeamId?: string | null;
    awayTeamName?: string | null;
    homeScore?: number | null;
    awayScore?: number | null;
    kickoffAt?: string | null;
    roundId?: string | null;
    status?: string | null;
};

function toQueryParams<TFilters extends object>(filters: TFilters): QueryParams {
    const params: QueryParams = {};
    for (const [key, value] of Object.entries(filters as Record<string, unknown>)) {
        if (value === null || value === undefined) continue;
        if (typeof value === "string" && value.trim().length === 0) continue;
        const normalizedKey =
            key === "dateRangeStart" ? "dateStart" :
                key === "dateRangeEnd" ? "dateEnd" :
                    key === "monthKey" ? "month" : key;
        params[normalizedKey] = value as string | number | boolean;
    }
    return params;
}

function normalizeFixtureStatus(status?: string | null): string | null {
    if (!status) return null;

    const normalized = status.trim().toLowerCase();
    if (normalized.length === 0) return null;

    if (["ft", "aet", "pen", "finished", "fulltime"].some((token) => normalized.includes(token))) {
        return "finished";
    }

    if (["ns", "scheduled", "tbd", "not started"].some((token) => normalized.includes(token))) {
        return "scheduled";
    }

    if (["live", "1h", "2h", "ht", "in play"].some((token) => normalized.includes(token))) {
        return "live";
    }

    if (["pst", "canc", "abandoned", "postponed"].some((token) => normalized.includes(token))) {
        return "cancelled";
    }

    return normalized;
}

function toFixtureCard(item: MatchesFixtureItem): FixtureCard {
    return {
        fixtureId: item.fixtureId ?? item.matchId,
        homeTeamId: item.homeTeamId ?? "",
        homeTeamName: item.homeTeamName ?? "Mandante",
        awayTeamId: item.awayTeamId ?? "",
        awayTeamName: item.awayTeamName ?? "Visitante",
        homeGoals: item.homeScore ?? null,
        awayGoals: item.awayScore ?? null,
        playedAt: item.kickoffAt ?? null,
        roundId: item.roundId ?? null,
        status: normalizeFixtureStatus(item.status),
    };
}

export async function fetchStandings(
    filters: CompetitionFilters,
): Promise<ApiResponse<{ items: StandingsSnapshot[] }>> {
    return apiRequest<ApiResponse<{ items: StandingsSnapshot[] }>>(COMPETITION_ENDPOINTS.standings, {
        method: "GET",
        params: toQueryParams(filters),
    });
}

export async function fetchStandingsEvolution(
    filters: CompetitionFilters,
): Promise<ApiResponse<{ items: StandingsEvolutionPoint[] }>> {
    return apiRequest<ApiResponse<{ items: StandingsEvolutionPoint[] }>>(
        COMPETITION_ENDPOINTS.evolution,
        { method: "GET", params: toQueryParams(filters) },
    );
}

export async function fetchCompetitionFixtures(
    filters: CompetitionFilters,
): Promise<ApiResponse<{ items: FixtureCard[] }>> {
    const response = await apiRequest<ApiResponse<{ items: MatchesFixtureItem[] }>>(COMPETITION_ENDPOINTS.fixtures, {
        method: "GET",
        params: toQueryParams(filters),
    });

    return {
        ...response,
        data: {
            items: (response.data.items ?? []).map((item) => toFixtureCard(item)),
        },
    };
}
