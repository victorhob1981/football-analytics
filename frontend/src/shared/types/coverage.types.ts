export type CoverageStatus = "complete" | "partial" | "empty" | "unknown";

export interface CoverageState {
  status: CoverageStatus;
  percentage?: number;
  label?: string;
}
