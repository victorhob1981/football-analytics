import { expect, test } from "@playwright/test";

import { installApiMocks } from "./fixtures/mock-api";

test.describe("Fluxo critico: rankings estavel com filtros globais", () => {
  test("mantem pagina de ranking estavel ao trocar timeRange", async ({ page }) => {
    await installApiMocks(page);

    await page.goto("/rankings/player-goals");

    await expect(page.getByRole("heading", { name: "Artilharia" })).toBeVisible();

    await page.locator("#global-filter-last-n").fill("5");
    await expect.poll(() => page.url()).toContain("lastN=5");

    await page.locator("#global-filter-date-start").fill("2026-02-01");
    await page.locator("#global-filter-date-end").fill("2026-02-20");

    await expect.poll(() => page.url()).toContain("dateRangeStart=2026-02-01");
    await expect.poll(() => page.url()).toContain("dateRangeEnd=2026-02-20");
    await expect.poll(() => page.url()).not.toContain("lastN=5");

    await expect(page.getByRole("heading", { name: "Artilharia" })).toBeVisible();
    await expect(page.getByText("RankingMetricSelector: placeholder")).toBeVisible();
    await expect(page.getByText("Falha ao carregar ranking.")).toHaveCount(0);
  });
});
