import { expect, test } from "@playwright/test";

import { installApiMocks } from "./fixtures/mock-api";

test.describe("Fluxo critico: filtro global -> players", () => {
  test("atualiza querystring e navega para /players sem erro", async ({ page }) => {
    await installApiMocks(page);

    await page.goto("/");

    await expect(page.getByRole("heading", { name: "Home da Plataforma" })).toBeVisible();

    await page.locator("#global-filter-competition-id").fill("648");
    await page.locator("#global-filter-season-id").fill("2024");

    await expect.poll(() => page.url()).toContain("competitionId=648");
    await expect.poll(() => page.url()).toContain("seasonId=2024");

    await page.getByRole("link", { name: "Players" }).click();

    await expect(page).toHaveURL(/\/players/);
    await expect.poll(() => page.url()).toContain("competitionId=648");
    await expect(page.getByRole("heading", { name: "Jogadores" })).toBeVisible();
    await expect(page.getByRole("link", { name: "Arrascaeta" })).toBeVisible();
    await expect(page.getByText("Falha ao carregar lista de jogadores.")).toHaveCount(0);
  });
});
