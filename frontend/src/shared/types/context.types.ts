export interface CompetitionSeasonContext {
  competitionId: string;
  competitionKey: string;
  competitionName: string;
  seasonId: string;
  seasonLabel: string;
}

export interface CompetitionSeasonContextInput {
  competitionId?: string | null;
  competitionKey?: string | null;
  seasonId?: string | null;
  seasonLabel?: string | null;
}

export interface CompetitionSeasonContextFilters {
  competitionId?: string | null;
  seasonId?: string | null;
}

export interface CompetitionSeasonContextsData {
  defaultContext: CompetitionSeasonContext | null;
  availableContexts: CompetitionSeasonContext[];
}
