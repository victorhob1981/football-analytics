import type { CoverageState } from "@/shared/types/coverage.types";
import type { Pagination } from "@/shared/types/pagination.types";

export interface ApiResponseMeta {
  pagination?: Pagination;
  coverage?: CoverageState;
  requestId?: string;
  generatedAt?: string;
}

export interface ApiResponse<T> {
  data: T;
  meta?: ApiResponseMeta;
}
