// ── Mercado — Transferências ──────────────────────────────────────────────────

export interface Transfer {
    transferId?: string | null;
    playerId: string;
    playerName: string;
    fromTeamId?: string | null;
    fromTeamName?: string | null;
    toTeamId?: string | null;
    toTeamName?: string | null;
    type: "permanent" | "loan" | "loan_return" | "free" | string;
    date?: string | null;
}

export interface TransfersListData {
    items: Transfer[];
}

// ── Mercado — Disponibilidade ─────────────────────────────────────────────────

export interface SidelinedPlayer {
    playerId: string;
    playerName: string;
    teamId: string;
    teamName: string;
    reason?: string | null;
    startDate?: string | null;
    endDate?: string | null;
}

export interface SidelinedListData {
    items: SidelinedPlayer[];
}

// ── Filtros ───────────────────────────────────────────────────────────────────

export interface MarketFilters {
    competitionId?: string | null;
    seasonId?: string | null;
    teamId?: string | null;
    transferType?: string | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
    page?: number;
    pageSize?: number;
}
