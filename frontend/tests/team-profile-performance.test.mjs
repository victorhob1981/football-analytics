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

test("team profile defers optional squad and stats payloads", () => {
  assert.match(teamProfileSource, /includeSquad: false/);
  assert.match(teamProfileSource, /includeStats: false/);
  assert.match(teamProfileSource, /const detailsQuery = useTeamProfile/);
  assert.match(teamProfileSource, /includeSquad: activeTab === "squad"/);
  assert.match(teamProfileSource, /includeStats: activeTab === "stats"/);
});
