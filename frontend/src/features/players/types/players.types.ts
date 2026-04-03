import type { VenueFilter } from "@/shared/types/filters.types";
import type { CoverageState } from "@/shared/types/coverage.types";

export interface Player {
  playerId: string;
  playerName: string;
  teamId?: string | null;
  teamName?: string | null;
  position?: string | null;
  nationality?: string | null;
  lastMatchAt?: string | null;
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
  matchId?: string | null;
  playedAt?: string | null;
  competitionId?: string | null;
  competitionName?: string | null;
  seasonId?: string | null;
  roundId?: string | null;
  teamId?: string | null;
  teamName?: string | null;
  opponentTeamId?: string | null;
  opponentName?: string | null;
  venue?: "home" | "away" | null;
  goalsFor?: number | null;
  goalsAgainst?: number | null;
  result?: "win" | "draw" | "loss" | null;
  minutesPlayed?: number | null;
  goals?: number | null;
  assists?: number | null;
  shotsTotal?: number | null;
  shotsOnTarget?: number | null;
  passesAttempted?: number | null;
  rating?: number | null;
}

export interface PlayerHistoryEntry {
  competitionId?: string | null;
  competitionKey?: string | null;
  competitionName?: string | null;
  seasonId?: string | null;
  seasonLabel?: string | null;
  teamId?: string | null;
  teamName?: string | null;
  matchesPlayed?: number | null;
  minutesPlayed?: number | null;
  goals?: number | null;
  assists?: number | null;
  rating?: number | null;
  lastMatchAt?: string | null;
}

export interface PlayerStatsTrendPoint {
  periodKey?: string | null;
  label?: string | null;
  matchesPlayed?: number | null;
  minutesPlayed?: number | null;
  goals?: number | null;
  assists?: number | null;
  shotsTotal?: number | null;
  shotsOnTarget?: number | null;
  passesAttempted?: number | null;
  rating?: number | null;
}

export interface PlayerProfileStats {
  minutesPerMatch?: number | null;
  goalsPer90?: number | null;
  assistsPer90?: number | null;
  goalContributionsPer90?: number | null;
  shotsPer90?: number | null;
  shotsOnTargetPer90?: number | null;
  shotsOnTargetPct?: number | null;
  passesAttemptedPer90?: number | null;
  yellowCardsPer90?: number | null;
  redCardsPer90?: number | null;
  trend?: PlayerStatsTrendPoint[];
}

export interface PlayerProfileSectionCoverage {
  overview?: CoverageState;
  history?: CoverageState;
  matches?: CoverageState;
  stats?: CoverageState;
}

export interface PlayerProfile {
  player: Player;
  summary: PlayerStatsSummary;
  recentMatches?: PlayerMatchStatsPoint[];
  history?: PlayerHistoryEntry[];
  stats?: PlayerProfileStats | null;
  sectionCoverage?: PlayerProfileSectionCoverage;
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
  includeHistory?: boolean;
  includeStats?: boolean;
}

export type PlayersListFilters = PlayersGlobalFilters & PlayersListLocalFilters;

export type PlayerProfileFilters = PlayersGlobalFilters & PlayerProfileLocalFilters;
