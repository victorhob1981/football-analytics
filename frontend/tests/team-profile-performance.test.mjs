import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import path from "node:path";

const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const teamProfilePath = path.join(
  projectRoot,
  "src",
  "features",
  "teams",
  "components",
  "TeamProfileContent.tsx",
);
const teamProfileSource = readFileSync(teamProfilePath, "utf8");
const playerProfilePath = path.join(
  projectRoot,
  "src",
  "app",
  "(platform)",
  "players",
  "[playerId]",
  "PlayerProfileContent.tsx",
);
const playerProfileSource = readFileSync(playerProfilePath, "utf8");
const rankingsPagePath = path.join(projectRoot, "src", "app", "(platform)", "rankings", "page.tsx");
const rankingsPageSource = readFileSync(rankingsPagePath, "utf8");

test("team profile defers optional squad and stats payloads", () => {
  assert.match(teamProfileSource, /activeTab = resolveTeamProfileTab/);
  assert.match(teamProfileSource, /includeSquad: activeTab === "squad"/);
  assert.match(teamProfileSource, /includeStats: activeTab === "stats"/);
});

test("player profile defers history, matches and stats by active tab", () => {
  assert.match(playerProfileSource, /includeHistory: activeTab === "history"/);
  assert.match(playerProfileSource, /includeRecentMatches: activeTab === "overview" \|\| activeTab === "matches"/);
  assert.match(playerProfileSource, /includeStats: activeTab === "stats"/);
});

test("rankings hub does not force an uncached server render", () => {
  assert.doesNotMatch(rankingsPageSource, /export const dynamic = "force-dynamic"/);
  assert.doesNotMatch(rankingsPageSource, /export const revalidate = 0/);
  assert.doesNotMatch(rankingsPageSource, /searchParams/);
  assert.match(rankingsPageSource, /<RankingCatalogLink/);
});
