import { describe, expect, it } from "vitest";

import { isFilterEnabled, isFilterPartial, resolvePageFilterConfig } from "@/config/page-filters.config";

describe("page-filters.config", () => {
  it("resolve config da home com rodada parcial", () => {
    const config = resolvePageFilterConfig("/");

    expect(config.season).toBe("enabled");
    expect(config.round).toBe("partial");
    expect(config.month).toBe("enabled");
  });

  it("resolve config de market com rodada desabilitada", () => {
    const config = resolvePageFilterConfig("/market");

    expect(config.round).toBe("disabled");
    expect(config.lastN).toBe("partial");
    expect(config.dateRange).toBe("enabled");
  });

  it("resolve config padrao para rota nao mapeada", () => {
    const config = resolvePageFilterConfig("/alguma-rota-inexistente");

    expect(config.season).toBe("enabled");
    expect(config.round).toBe("enabled");
    expect(config.month).toBe("enabled");
    expect(config.lastN).toBe("enabled");
    expect(config.dateRange).toBe("enabled");
    expect(config.venue).toBe("enabled");
  });

  it("helpers de estado funcionam para enabled/partial/disabled", () => {
    expect(isFilterEnabled("enabled")).toBe(true);
    expect(isFilterEnabled("partial")).toBe(true);
    expect(isFilterEnabled("disabled")).toBe(false);
    expect(isFilterPartial("enabled")).toBe(false);
    expect(isFilterPartial("partial")).toBe(true);
  });
});
