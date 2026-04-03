"use client";

import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";

import { fetchHomePage } from "@/features/home/services/home.service";
import type { HomePageData } from "@/features/home/types/home.types";

export function useHomePage() {
  return useQueryWithCoverage<HomePageData>({
    queryKey: ["home", "landing"],
    queryFn: () => fetchHomePage(),
    staleTime: 5 * 60 * 1000,
    isDataEmpty: (data) =>
      !data ||
      !Array.isArray((data as Partial<HomePageData>).competitions) ||
      (data as Partial<HomePageData>).competitions?.length === 0,
  });
}
