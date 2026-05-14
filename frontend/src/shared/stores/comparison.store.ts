import { create } from "zustand";

import type { ComparisonEntityType, ComparisonState, ComparisonStore } from "@/shared/types/comparison.types";

export const MAX_COMPARISON_SELECTED_IDS = 2;

const INITIAL_COMPARISON_STATE: ComparisonState = {
  entityType: "player",
  selectedIds: [],
  activeMetrics: [],
};

function normalizeId(value: string): string {
  return value.trim();
}

function normalizeMetrics(metricKeys: string[]): string[] {
  const normalizedMetricKeys = metricKeys.map((metricKey) => metricKey.trim()).filter((metricKey) => metricKey.length > 0);
  return Array.from(new Set(normalizedMetricKeys));
}

function addSelectedId(currentIds: string[], id: string): string[] {
  if (currentIds.includes(id)) {
    return currentIds;
  }

  const nextIds = [...currentIds, id];

  if (nextIds.length <= MAX_COMPARISON_SELECTED_IDS) {
    return nextIds;
  }

  return nextIds.slice(nextIds.length - MAX_COMPARISON_SELECTED_IDS);
}

function applyEntityTypeChange(currentState: ComparisonState, entityType: ComparisonEntityType): ComparisonState {
  if (currentState.entityType === entityType) {
    return currentState;
  }

  return {
    ...currentState,
    entityType,
    selectedIds: [],
    activeMetrics: [],
  };
}

export const useComparisonStore = create<ComparisonStore>((set) => ({
  ...INITIAL_COMPARISON_STATE,
  add: (id) => {
    const normalizedId = normalizeId(id);

    if (normalizedId.length === 0) {
      return;
    }

    set((state) => ({
      ...state,
      selectedIds: addSelectedId(state.selectedIds, normalizedId),
    }));
  },
  remove: (id) => {
    const normalizedId = normalizeId(id);

    if (normalizedId.length === 0) {
      return;
    }

    set((state) => ({
      ...state,
      selectedIds: state.selectedIds.filter((selectedId) => selectedId !== normalizedId),
    }));
  },
  clear: () => {
    set((state) => ({
      ...state,
      selectedIds: [],
    }));
  },
  setEntityType: (entityType) => {
    set((state) => applyEntityTypeChange(state, entityType));
  },
  setActiveMetrics: (metricKeys) => {
    set((state) => ({
      ...state,
      activeMetrics: normalizeMetrics(metricKeys),
    }));
  },
}));

export { INITIAL_COMPARISON_STATE };
