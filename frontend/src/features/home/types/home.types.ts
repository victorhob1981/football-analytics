import type { VenueFilter } from "@/shared/types/filters.types";

// ─── Overview / KPIs ──────────────────────────────────────────────────────────

export interface LeagueKpiData {
  totalMatches?: number | null;
  totalGoals?: number | null;
  avgGoalsPerMatch?: number | null;
  totalTeams?: number | null;
}

// ─── Top Attacks / Defenses ───────────────────────────────────────────────────

export interface TeamStrengthItem {
  teamId: string;
  teamName: string;
  value: number;
  /** Monthly goals trend for sparkline */
  trend?: Array<{ period: string; value: number }>;
}

export interface TopTeamsData {
  topAttacks: TeamStrengthItem[];
  topDefenses: TeamStrengthItem[];
}

// ─── Standings Evolution ──────────────────────────────────────────────────────

export interface StandingsEvolutionPoint {
  roundLabel: string;
  [teamName: string]: string | number;
}

export interface StandingsEvolutionData {
  series: StandingsEvolutionPoint[];
  teamNames: string[];
}

// ─── Top Players ─────────────────────────────────────────────────────────────

export interface TopPlayerItem {
  playerId: string;
  playerName: string;
  teamName?: string | null;
  goals?: number | null;
  assists?: number | null;
  rating?: number | null;
  minutesPlayed?: number | null;
  variationValue?: number | null;
  variationPct?: number | null;
}

export interface TopPlayersData {
  items: TopPlayerItem[];
}

// ─── Coverage Summary ─────────────────────────────────────────────────────────

export type CoverageLevel = "high" | "partial" | "low";

export interface CoverageModuleItem {
  label: string;
  percentage: number;
  level: CoverageLevel;
}

export interface CoverageSummaryData {
  modules: CoverageModuleItem[];
}

// ─── Filters ─────────────────────────────────────────────────────────────────

export interface HomeGlobalFilters {
  competitionId?: string | null;
  seasonId?: string | null;
  roundId?: string | null;
  venue?: VenueFilter;
  lastN?: number | null;
  dateRangeStart?: string | null;
  dateRangeEnd?: string | null;
}
