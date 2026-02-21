export interface ApiError {
  message: string;
  code?: string;
  status?: number;
  details?: unknown;
}

export interface HttpError extends ApiError {
  status: number;
}
