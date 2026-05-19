import type { ApiResponse } from "@/shared/types/api-response.types";
import { apiRequest } from "@/shared/services/api-client";

import type { HomePageData } from "@/features/home/types/home.types";

export const HOME_ENDPOINT = "/api/v1/home";

export async function fetchHomePage(): Promise<ApiResponse<HomePageData>> {
  return apiRequest<ApiResponse<HomePageData>>(HOME_ENDPOINT, {
    method: "GET",
  });
}
