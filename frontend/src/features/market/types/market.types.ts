import type { VenueFilter } from "@/shared/types/filters.types";

export type MarketTransfersSortBy = "transferDate" | "playerName";

export type MarketTransfersSortDirection = "asc" | "desc";

export interface MarketGlobalFilters {
  competitionId?: string | null;
  seasonId?: string | null;
  roundId?: string | null;
  stageId?: string | null;
  stageFormat?: string | null;
  venue?: VenueFilter;
  lastN?: number | null;
  dateRangeStart?: string | null;
  dateRangeEnd?: string | null;
}

export interface MarketTransfersLocalFilters {
  search?: string;
  page?: number;
  pageSize?: number;
  sortBy?: MarketTransfersSortBy;
  sortDirection?: MarketTransfersSortDirection;
}

export type MarketTransfersFilters = MarketGlobalFilters & MarketTransfersLocalFilters;

export interface MarketTransferItem {
  transferId: string;
  playerId?: string | null;
  playerName: string;
  fromTeamId?: string | null;
  fromTeamName?: string | null;
  toTeamId?: string | null;
  toTeamName?: string | null;
  transferDate?: string | null;
  completed: boolean;
  careerEnded: boolean;
  typeId?: number | null;
  amount?: string | null;
}

export interface MarketTransfersData {
  items: MarketTransferItem[];
}
