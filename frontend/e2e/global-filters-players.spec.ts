import { expect, test } from "@playwright/test";

import { installApiMocks } from "./fixtures/mock-api";

test.describe("Fluxo critico: filtro global -> players", () => {
  test("atualiza querystring e navega para /players sem erro", async ({ page }) => {
    await installApiMocks(page);

    await page.goto("/");

    await expect(page.getByRole("heading", { name: "Entrada principal do produto" })).toBeVisible();
    await expect(page.locator('[aria-label="Filtros do produto"]')).toHaveAttribute(
      "data-url-hydrated",
      "true",
    );

    await page.locator("#global-filter-competition-id").selectOption("8");
    await expect(page.locator("#global-filter-competition-id")).toHaveValue("8");
    await expect.poll(() => page.url()).toContain("competitionId=8");

    await page.locator("#global-filter-season-id").selectOption("2024");
    await expect(page.locator("#global-filter-season-id")).toHaveValue("2024");

    await expect.poll(() => page.url()).toContain("seasonId=2024");

    await page
      .getByLabel("Navegação principal")
      .getByRole("link", { exact: true, name: "Jogadores" })
      .click();

    await expect(page).toHaveURL(/\/players/);
    await expect.poll(() => page.url()).toContain("competitionId=8");
    await expect(page.getByRole("heading", { exact: true, name: "Jogadores" })).toBeVisible();
    await expect(page.getByRole("link", { exact: true, name: "Arrascaeta" })).toBeVisible();
    await expect(page.getByText("Falha ao carregar a lista")).toHaveCount(0);
  });
});
