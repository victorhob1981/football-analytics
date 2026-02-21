import type { VenueFilter } from "@/shared/types/filters.types";

export interface Player {
  playerId: string;
  playerName: string;
  teamId?: string | null;
  teamName?: string | null;
  position?: string | null;
  nationality?: string | null;
}

export interface PlayerListItem extends Player {
  matchesPlayed?: number | null;
  minutesPlayed?: number | null;
  goals?: number | null;
  assists?: number | null;
  shotsTotal?: number | null;
  passAccuracyPct?: number | null;
  yellowCards?: number | null;
  redCards?: number | null;
  rating?: number | null;
}

export interface PlayerStatsSummary {
  matchesPlayed?: number | null;
  minutesPlayed?: number | null;
  goals?: number | null;
  assists?: number | null;
  shotsTotal?: number | null;
  shotsOnTarget?: number | null;
  passesCompleted?: number | null;
  passesAttempted?: number | null;
  passAccuracyPct?: number | null;
  yellowCards?: number | null;
  redCards?: number | null;
  rating?: number | null;
}

export interface PlayerMatchStatsPoint {
  fixtureId: string;
  playedAt?: string | null;
  opponentName?: string | null;
  minutesPlayed?: number | null;
  goals?: number | null;
  assists?: number | null;
  rating?: number | null;
}

export interface PlayerProfile {
  player: Player;
  summary: PlayerStatsSummary;
  recentMatches?: PlayerMatchStatsPoint[];
}

export interface PlayersListData {
  items: PlayerListItem[];
}

export interface PlayersGlobalFilters {
  competitionId?: string | null;
  seasonId?: string | null;
  roundId?: string | null;
  venue?: VenueFilter;
  lastN?: number | null;
  dateRangeStart?: string | null;
  dateRangeEnd?: string | null;
}

export interface PlayersListLocalFilters {
  search?: string;
  minMinutes?: number | null;
  teamId?: string | null;
  position?: string | null;
  page?: number;
  pageSize?: number;
}

export interface PlayerProfileLocalFilters {
  includeRecentMatches?: boolean;
}

export type PlayersListFilters = PlayersGlobalFilters & PlayersListLocalFilters;

export type PlayerProfileFilters = PlayersGlobalFilters & PlayerProfileLocalFilters;
