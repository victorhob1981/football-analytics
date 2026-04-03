export type SearchGroupType = "competition" | "team" | "player" | "match";

export interface SearchDefaultContext {
  competitionId: string;
  competitionKey?: string | null;
  competitionName?: string | null;
  seasonId: string;
  seasonLabel?: string | null;
}

export interface CompetitionSearchResult {
  competitionId: string;
  competitionKey: string;
  competitionName: string;
}

export interface TeamSearchResult {
  teamId: string;
  teamName: string;
  defaultContext: SearchDefaultContext;
}

export interface PlayerSearchResult {
  playerId: string;
  playerName: string;
  teamId?: string | null;
  teamName?: string | null;
  position?: string | null;
  defaultContext: SearchDefaultContext;
}

export interface MatchSearchResult {
  matchId: string;
  competitionId?: string | null;
  competitionName?: string | null;
  seasonId?: string | null;
  roundId?: string | null;
  kickoffAt?: string | null;
  status?: string | null;
  homeTeamId?: string | null;
  homeTeamName?: string | null;
  awayTeamId?: string | null;
  awayTeamName?: string | null;
  homeScore?: number | null;
  awayScore?: number | null;
  defaultContext: SearchDefaultContext;
}

export type SearchGroup =
  | {
      type: "competition";
      total: number;
      items: CompetitionSearchResult[];
    }
  | {
      type: "team";
      total: number;
      items: TeamSearchResult[];
    }
  | {
      type: "player";
      total: number;
      items: PlayerSearchResult[];
    }
  | {
      type: "match";
      total: number;
      items: MatchSearchResult[];
    };

export interface GlobalSearchData {
  query: string;
  groups: SearchGroup[];
  totalResults: number;
}

export interface GlobalSearchFilters {
  q: string;
  types?: SearchGroupType[];
  competitionId?: string | null;
  seasonId?: string | null;
  limitPerType?: number;
}
