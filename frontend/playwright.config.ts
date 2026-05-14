import { defineConfig, devices } from "@playwright/test";

const PORT = 3100;
const BASE_URL = `http://127.0.0.1:${PORT}`;

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: true,
  timeout: 30_000,
  expect: {
    timeout: 8_000,
  },
  use: {
    baseURL: BASE_URL,
    trace: "on-first-retry",
    headless: true,
  },
  webServer: {
    command: "pnpm exec next dev -p 3100",
    url: BASE_URL,
    reuseExistingServer: true,
    timeout: 120_000,
    env: {
      NEXT_PUBLIC_BFF_BASE_URL: BASE_URL,
      NEXT_PUBLIC_APP_ENV: "test",
    },
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
      },
    },
  ],
});
