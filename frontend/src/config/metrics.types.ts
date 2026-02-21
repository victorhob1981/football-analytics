export type MetricFormat = "number" | "percentage" | "rating" | "seconds" | "minutes";

export type MetricEntity = "player" | "team" | "match" | "coach" | "h2h";

export interface MetricDefinition {
  key: string;
  label: string;
  description: string;
  format: MetricFormat;
  precision?: number;
  unit?: string;
  entity: MetricEntity;
  normalizable: boolean;
  coverageWarning?: string;
  sourceHints?: string[];
}

export type MetricRegistry = Record<string, MetricDefinition>;
