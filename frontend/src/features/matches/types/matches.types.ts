import type { VenueFilter } from "@/shared/types/filters.types";

export type MatchesListSortBy = "kickoffAt" | "status" | "homeTeamName" | "awayTeamName";
export type MatchesListSortDirection = "asc" | "desc";

export interface MatchListItem {
  matchId: string;
  fixtureId?: string | null;
  competitionId?: string | null;
  competitionName?: string | null;
  seasonId?: string | null;
  roundId?: string | null;
  kickoffAt?: string | null;
  status?: string | null;
  venueName?: string | null;
  homeTeamId?: string | null;
  homeTeamName?: string | null;
  awayTeamId?: string | null;
  awayTeamName?: string | null;
  homeScore?: number | null;
  awayScore?: number | null;
}

export interface MatchTimelineEvent {
  eventId?: string | null;
  minute?: number | null;
  second?: number | null;
  period?: string | null;
  type?: string | null;
  detail?: string | null;
  teamId?: string | null;
  teamName?: string | null;
  playerId?: string | null;
  playerName?: string | null;
}

export interface MatchLineupPlayer {
  playerId?: string | null;
  playerName?: string | null;
  teamId?: string | null;
  teamName?: string | null;
  position?: string | null;
  shirtNumber?: number | null;
  isStarter?: boolean | null;
}

export interface MatchPlayerStat {
  playerId?: string | null;
  playerName?: string | null;
  teamId?: string | null;
  teamName?: string | null;
  minutesPlayed?: number | null;
  goals?: number | null;
  assists?: number | null;
  shotsTotal?: number | null;
  passesCompleted?: number | null;
  rating?: number | null;
}

export interface MatchCenterData {
  match: MatchListItem;
  timeline?: MatchTimelineEvent[];
  lineups?: MatchLineupPlayer[];
  playerStats?: MatchPlayerStat[];
}

export interface MatchesListData {
  items: MatchListItem[];
}

export interface MatchesGlobalFilters {
  competitionId?: string | null;
  seasonId?: string | null;
  roundId?: string | null;
  venue?: VenueFilter;
  lastN?: number | null;
  dateRangeStart?: string | null;
  dateRangeEnd?: string | null;
}

export interface MatchesListLocalFilters {
  search?: string;
  status?: string | null;
  page?: number;
  pageSize?: number;
  sortBy?: MatchesListSortBy;
  sortDirection?: MatchesListSortDirection;
}

export interface MatchCenterLocalFilters {
  includeTimeline?: boolean;
  includeLineups?: boolean;
  includePlayerStats?: boolean;
}

export type MatchesListFilters = MatchesGlobalFilters & MatchesListLocalFilters;
export type MatchCenterFilters = MatchesGlobalFilters & MatchCenterLocalFilters;
