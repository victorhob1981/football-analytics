import fs from "node:fs";
import path from "node:path";

import { expect, test } from "@playwright/test";

import { installApiMocks } from "./fixtures/mock-api";

const OUTPUT_DIR = path.resolve(__dirname, "..", "test-results", "players-list-visual-audit");

function ensureOutputDir() {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

test.describe("Auditoria visual da lista de jogadores", () => {
  test("gera screenshot da lista migrada", async ({ page }) => {
    ensureOutputDir();
    await installApiMocks(page);
    await page.setViewportSize({ width: 1600, height: 1800 });

    await page.goto("/players?competitionId=8&seasonId=2024&lastN=5");
    await expect(page.getByRole("heading", { exact: true, name: "Jogadores" })).toBeVisible();
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(800);

    await page
      .locator("main")
      .first()
      .screenshot({
        animations: "disabled",
        path: path.join(OUTPUT_DIR, "players-list-implementation.png"),
      });
  });
});
