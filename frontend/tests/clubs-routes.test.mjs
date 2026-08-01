import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const read = (path) => readFileSync(path, "utf8");

const clubsRoute = read("src/app/(platform)/clubs/page.tsx");
const teamsRoute = read("src/app/(platform)/teams/page.tsx");
const teamsContent = read("src/features/teams/components/TeamsPageContent.tsx");

test("clubs renders the existing catalog filtered to clubs", () => {
  assert.match(clubsRoute, /<TeamsPageContent entityType="club" \/>/);
  assert.doesNotMatch(clubsRoute, /redirect|permanentRedirect/);
});

test("teams permanently redirects to clubs preserving search params", () => {
  assert.match(teamsRoute, /permanentRedirect\(/);
  assert.match(teamsRoute, /`\/clubs\$\{buildPassthroughSearchParamsQueryString\(resolvedSearchParams\)\}`/);
});

test("TeamsPageContent forwards entityType to the catalog hook", () => {
  assert.match(teamsContent, /entityType\?: TeamType \| null/);
  assert.match(teamsContent, /entityType,\n\s+sortBy/);
});
