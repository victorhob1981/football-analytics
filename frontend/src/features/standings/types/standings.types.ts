export interface StandingsCompetitionScope {
  competitionId: string;
  competitionKey?: string | null;
  competitionName?: string | null;
  seasonId: string;
  seasonLabel?: string | null;
  providerSeasonId?: string | null;
}

export interface StandingsStage {
  stageId: string;
  stageName?: string | null;
  stageFormat?: string | null;
  expectedTeams?: number | null;
}

export interface StandingsGroup {
  groupId: string;
  groupName?: string | null;
  groupOrder?: number | null;
  expectedTeams?: number | null;
}

export interface StandingsRound {
  roundId: string;
  providerRoundId?: string | null;
  roundName?: string | null;
  label: string;
  startingAt?: string | null;
  endingAt?: string | null;
  isCurrent: boolean;
}

export interface StandingsTableRow {
  position: number;
  teamId: string;
  teamName?: string | null;
  matchesPlayed: number;
  wins: number;
  draws: number;
  losses: number;
  goalsFor: number;
  goalsAgainst: number;
  goalDiff: number;
  points: number;
}

export interface StandingsTableData {
  competition: StandingsCompetitionScope;
  stage?: StandingsStage | null;
  group?: StandingsGroup | null;
  selectedRound?: StandingsRound | null;
  currentRound?: StandingsRound | null;
  rounds: StandingsRound[];
  rows: StandingsTableRow[];
  updatedAt?: string | null;
}

export interface StandingsFilters {
  competitionId?: string | null;
  competitionKey?: string | null;
  seasonId?: string | null;
  seasonLabel?: string | null;
  roundId?: string | null;
  stageId?: string | null;
  groupId?: string | null;
}
