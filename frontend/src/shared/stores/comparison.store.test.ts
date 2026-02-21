import { beforeEach, describe, expect, it } from "vitest";

import {
  INITIAL_COMPARISON_STATE,
  MAX_COMPARISON_SELECTED_IDS,
  useComparisonStore,
} from "@/shared/stores/comparison.store";

describe("comparison.store", () => {
  beforeEach(() => {
    useComparisonStore.setState({ ...INITIAL_COMPARISON_STATE });
  });

  it("mantem no maximo dois ids selecionados", () => {
    useComparisonStore.getState().add("player-1");
    useComparisonStore.getState().add("player-2");
    useComparisonStore.getState().add("player-3");

    const state = useComparisonStore.getState();

    expect(state.selectedIds).toHaveLength(MAX_COMPARISON_SELECTED_IDS);
    expect(state.selectedIds).toEqual(["player-2", "player-3"]);
  });

  it("normaliza id e ignora ids vazios/duplicados", () => {
    useComparisonStore.getState().add("  player-10  ");
    useComparisonStore.getState().add("player-10");
    useComparisonStore.getState().add("   ");

    const state = useComparisonStore.getState();

    expect(state.selectedIds).toEqual(["player-10"]);
  });

  it("remove e clear funcionam conforme contrato", () => {
    useComparisonStore.getState().add("player-1");
    useComparisonStore.getState().add("player-2");
    useComparisonStore.getState().remove("player-1");

    expect(useComparisonStore.getState().selectedIds).toEqual(["player-2"]);

    useComparisonStore.getState().clear();

    expect(useComparisonStore.getState().selectedIds).toEqual([]);
  });

  it("setEntityType limpa selectedIds e activeMetrics quando muda o tipo", () => {
    useComparisonStore.getState().add("player-1");
    useComparisonStore.getState().setActiveMetrics(["goals", "assists"]);

    useComparisonStore.getState().setEntityType("team");

    const state = useComparisonStore.getState();

    expect(state.entityType).toBe("team");
    expect(state.selectedIds).toEqual([]);
    expect(state.activeMetrics).toEqual([]);
  });
});
