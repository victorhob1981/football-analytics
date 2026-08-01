import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const read = (path) => readFileSync(path, "utf8");

const clubsRoute = read("src/app/(platform)/clubs/page.tsx");
const teamsRoute = read("src/app/(platform)/teams/page.tsx");
const teamsContent = read("src/features/teams/components/TeamsPageContent.tsx");
const clubProfileRoute = read("src/app/(platform)/clubs/[clubId]/page.tsx");
const teamProfileRoute = read("src/app/(platform)/teams/[teamId]/page.tsx");
const teamResolver = read("src/app/(platform)/teams/[teamId]/TeamRouteResolver.tsx");

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

test("team profile aliases guard identity before rendering or redirecting", () => {
  assert.match(clubProfileRoute, /surface="clubs"/);
  assert.match(teamProfileRoute, /surface="teams"/);
  assert.match(teamResolver, /useTeamContexts/);
  assert.match(teamResolver, /useTeamProfile/);
  assert.match(teamResolver, /teamType === "club"/);
  assert.match(teamResolver, /teamType === "national_team"/);
  assert.match(teamResolver, /fifa_world_cup_mens/);
  assert.match(teamResolver, /buildWorldCupTeamPath/);
  assert.match(
    teamResolver,
    /isClubSurface \? "Perfil de clube indisponível" : "Equipe indisponível"/,
  );
  assert.match(teamResolver, /isClubSurface \? "\/clubs" : "\/competitions"/);
  assert.match(teamResolver, /isClubSurface \? "Voltar para clubes" : "Abrir competições"/);
  assert.match(teamResolver, /identidade de equipe confirmada/);
});
