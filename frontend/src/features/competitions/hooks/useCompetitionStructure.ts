import { useMemo } from "react";

import { useQuery } from "@tanstack/react-query";

import { competitionStructureQueryKeys } from "@/features/competitions/queryKeys";
import { fetchCompetitionStructure } from "@/features/competitions/services/competition-hub.service";
import type {
  CompetitionStructureData,
  CompetitionStructureFilters,
} from "@/features/competitions/types/competition-structure.types";
import { ApiClientError } from "@/shared/services/api-client";
import type { CoverageState } from "@/shared/types/coverage.types";
import type { ApiError } from "@/shared/types/error.types";

const UNKNOWN_COVERAGE: CoverageState = { status: "unknown" };
const STRUCTURE_STALE_TIME_MS = 5 * 60 * 1000;
const STRUCTURE_GC_TIME_MS = 20 * 60 * 1000;

type UseCompetitionStructureOptions = {
  enabled?: boolean;
};

function isCompetitionStructureData(value: unknown): value is CompetitionStructureData {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }

  const maybeData = value as Partial<CompetitionStructureData>;
  return Boolean(
    maybeData.competition &&
      typeof maybeData.competition === "object" &&
      Array.isArray(maybeData.stages),
  );
}

function normalizeError(error: unknown): ApiError | null {
  if (!error || typeof error !== "object") {
    return null;
  }

  const maybeError = error as Partial<ApiError>;
  return {
    message: maybeError.message ?? "Falha ao carregar a estrutura da competição.",
    code: maybeError.code,
    status: maybeError.status,
    details: maybeError.details,
  };
}

export function useCompetitionStructure(
  filters: CompetitionStructureFilters,
  options: UseCompetitionStructureOptions = {},
) {
  const query = useQuery({
    queryKey: competitionStructureQueryKeys.structure(filters),
    queryFn: async () => {
      try {
        return await fetchCompetitionStructure(filters);
      } catch (error) {
        if (error instanceof ApiClientError && error.status === 404) {
          return null;
        }

        throw error;
      }
    },
    enabled: (options.enabled ?? true) && Boolean(filters.competitionKey && filters.seasonLabel),
    staleTime: STRUCTURE_STALE_TIME_MS,
    gcTime: STRUCTURE_GC_TIME_MS,
  });

  return useMemo(
    () => ({
      data: isCompetitionStructureData(query.data?.data) ? query.data.data : null,
      isLoading: query.isLoading,
      isError: query.isError,
      error: normalizeError(query.error),
      coverage: query.data?.meta?.coverage ?? UNKNOWN_COVERAGE,
      hasStructure: isCompetitionStructureData(query.data?.data),
    }),
    [query.data, query.error, query.isError, query.isLoading],
  ) as {
    data: CompetitionStructureData | null;
    isLoading: boolean;
    isError: boolean;
    error: ApiError | null;
    coverage: CoverageState;
    hasStructure: boolean;
  };
}
