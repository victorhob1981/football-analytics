export type ComparisonEntityType = "player" | "team";

export interface ComparisonState {
  entityType: ComparisonEntityType;
  selectedIds: string[];
  activeMetrics: string[];
}

export interface ComparisonActions {
  add: (id: string) => void;
  remove: (id: string) => void;
  clear: () => void;
  setEntityType: (entityType: ComparisonEntityType) => void;
  setActiveMetrics: (metricKeys: string[]) => void;
}

export type ComparisonStore = ComparisonState & ComparisonActions;
