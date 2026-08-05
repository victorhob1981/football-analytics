import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const read = (path) => readFileSync(path, "utf8").replaceAll("\r\n", "\n");

const clubsRoute = read("src/app/(platform)/clubs/page.tsx");
const teamsRoute = read("src/app/(platform)/teams/page.tsx");
const teamsContent = read("src/features/teams/components/TeamsPageContent.tsx");
const clubProfileRoute = read("src/app/(platform)/clubs/[clubId]/page.tsx");
const teamProfileRoute = read("src/app/(platform)/teams/[teamId]/page.tsx");
const teamResolver = read("src/app/(platform)/teams/[teamId]/TeamRouteResolver.tsx");
const contextRouting = read("src/shared/utils/context-routing.ts");
const contextualTeamsRoute = read(
  "src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/teams/[teamId]/page.tsx",
);
const contextualClubsRoute = read(
  "src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/clubs/[clubId]/page.tsx",
);
const contextualResolver = read("src/features/teams/components/ContextualTeamRouteResolver.tsx");
const aggregateProfile = read("src/features/teams/components/TeamAggregateProfileContent.tsx");
const contextualProfile = read("src/features/teams/components/TeamProfileContent.tsx");
const shellState = read("src/shared/components/navigation/usePlatformShellState.ts");
const platformShell = read("src/app/(platform)/PlatformShell.tsx");
const profileMedia = read("src/shared/components/profile/ProfileMedia.tsx");
const searchTypes = read("src/features/search/types/search.types.ts");
const searchOverlay = read("src/features/search/components/GlobalSearchOverlay.tsx");
const searchHook = read("src/features/search/hooks/useGlobalSearch.ts");
const searchService = read("src/features/search/services/search.service.ts");
const teamTypes = read("src/features/teams/types/teams.types.ts");
const teamsApi = read("../api/src/routers/teams.py");
const playersPage = read("src/app/(platform)/players/page.tsx");
const playerProfile = read("src/app/(platform)/players/[playerId]/PlayerProfileContent.tsx");

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
  assert.match(teamsContent, /const profileHref = buildProfileHref\(team\.teamId\)/);
  assert.doesNotMatch(teamsContent, /const profileHref = resolvedContext/);
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

test("club builders and contextual profile routes keep entity guards", () => {
  assert.match(contextRouting, /export function buildClubsPath/);
  assert.match(contextRouting, /export function buildClubResolverPath/);
  assert.match(contextRouting, /export function buildCanonicalClubPath/);
  assert.match(contextualClubsRoute, /surface="clubs"/);
  assert.match(contextualClubsRoute, /ContextualTeamRouteResolver/);
  assert.match(contextualTeamsRoute, /surface="teams"/);
  assert.match(contextualTeamsRoute, /ContextualTeamRouteResolver/);
  assert.match(contextualResolver, /useTeamProfile/);
  assert.match(contextualResolver, /buildCanonicalClubPath/);
  assert.match(contextualResolver, /teamType === "club"/);
  assert.match(contextualResolver, /teamType === "national_team"/);
  assert.match(contextualResolver, /fifa_world_cup_mens/);
  assert.match(contextualResolver, /Equipe indisponível/);
});

test("validated club consumers use club routes and labels", () => {
  assert.match(teamsContent, /isClubCatalog/);
  assert.match(teamsContent, /buildCanonicalClubPath/);
  assert.match(teamsContent, /buildClubResolverPath/);
  assert.match(teamsContent, /entityLabel = isClubCatalog \? "Clubes"/);
  assert.match(aggregateProfile, /buildClubsPath/);
  assert.match(aggregateProfile, /buildClubResolverPath/);
  assert.match(aggregateProfile, /buildCanonicalClubPath/);
  assert.match(aggregateProfile, /Ver clubes no recorte/);
  assert.match(contextualProfile, /buildClubsPath/);
  assert.match(contextualProfile, /buildClubResolverPath/);
  assert.match(contextualProfile, /buildCanonicalClubPath/);
  assert.match(contextualProfile, /Perfil de clube/);
  assert.match(shellState, /buildClubsPath/);
  assert.doesNotMatch(shellState, /buildTeamsPath/);
  assert.match(shellState, /surfaceLabel = "Clubes"/);
  assert.match(shellState, /surfaceLabel = "Perfil de clube"/);
  assert.match(shellState, /surfaceLabel = "Perfil de equipe"/);
  assert.match(shellState, /pathname\.startsWith\("\/clubs\/"\)/);
  assert.match(shellState, /pathname\.startsWith\("\/teams\/"\)/);
  assert.doesNotMatch(shellState, /isLegacyClubResolverPath\(pathname\) \|\| isShortTeamResolverPath\(pathname\)/);
  assert.match(profileMedia, /return `\/clubs\/\$\{encodePathSegment\(normalizedAssetId\)\}`/);
});

test("typed team search uses entity routes and labels", () => {
  assert.match(searchTypes, /teamType: TeamType/);
  assert.match(searchOverlay, /buildClubResolverPath/);
  assert.match(searchOverlay, /result\.teamType === "club"/);
  assert.match(searchOverlay, /buildTeamResolverPath/);
  assert.match(searchOverlay, /buildWorldCupTeamPath/);
  assert.match(searchOverlay, /teamType === "national_team"/);
  assert.match(searchOverlay, /\$\{typeLabel\} • \$\{contextLine\}/);
  assert.match(searchOverlay, /"Clube"/);
  assert.match(searchOverlay, /"Seleção"/);
  assert.match(searchOverlay, /"Equipe"/);
});

test("public shell keeps the mobile drawer closed and names the club surface canonically", () => {
  assert.match(platformShell, /buildClubsPath/);
  assert.doesNotMatch(platformShell, /buildTeamsPath/);
  assert.match(platformShell, /label: "Clubes"/);
  assert.match(platformShell, /isSidebarOpen \? "flex translate-x-0" : "hidden -translate-x-full"/);
  assert.match(platformShell, /lg:flex lg:translate-x-0/);
  assert.doesNotMatch(platformShell, /lg:overflow-visible/);
  assert.match(platformShell, /platform-mobile-bottom-nav[^\n]+grid-cols-6/);
  assert.match(platformShell, /const searchParamsKey = searchParams\.toString\(\)/);
  assert.match(platformShell, /\[pathname, searchParamsKey\]/);
  assert.match(platformShell, /buscar competições, partidas, clubes ou jogadores/);
  assert.doesNotMatch(platformShell, /\bTimes\b|\btimes\b/);
  assert.match(searchOverlay, /team: "Clubes e seleções"/);
  assert.match(searchOverlay, /"Clubes"/);
  assert.doesNotMatch(searchOverlay, /\bTimes\b|\btimes\b/);
});

test("global search waits for a stable query and cancels stale requests", () => {
  assert.match(searchOverlay, /useDebouncedValue/);
  assert.match(searchOverlay, /const debouncedQuery = useDebouncedValue\(query\.trim\(\), 250\)/);
  assert.match(searchOverlay, /useGlobalSearch\(debouncedQuery/);
  assert.match(searchOverlay, /const hasQuery = debouncedQuery\.length >= 2/);
  assert.match(searchHook, /queryFn: \(\{ signal \}\) => fetchGlobalSearch\(filters, signal\)/);
  assert.match(searchService, /filters: GlobalSearchFilters,\n\s+signal\?: AbortSignal/);
  assert.match(searchService, /signal,\n\s+\}\);/);
});

test("canonical club responses expose the legacy visual asset and the profile renders a visual archive", () => {
  assert.match(teamTypes, /visualAssetId\?: number \| string \| null/);
  assert.match(teamsApi, /raw\.provider_entity_map/);
  assert.match(teamsApi, /as visual_asset_id/);
  assert.match(teamsApi, /"visualAssetId": row\.get\("visual_asset_id"\)/);
  assert.match(aggregateProfile, /data-testid="club-identity-hero"/);
  assert.match(aggregateProfile, /data-testid="club-archive-timeline"/);
  assert.match(aggregateProfile, /assetId=\{team\.visualAssetId \?\? team\.teamId\}/);
  assert.match(contextualProfile, /assetId=\{team\.visualAssetId \?\? team\.teamId\}/);
  assert.match(teamsContent, /assetId=\{team\.visualAssetId \?\? team\.teamId\}/);
});

test("desktop sidebar fits the viewport without a persistent scrollbar", () => {
  assert.match(platformShell, /lg:h-dvh lg:overflow-hidden/);
  assert.match(platformShell, /lg:flex lg:h-full lg:flex-col lg:justify-between/);
  assert.match(platformShell, /lg:py-2/);
  assert.match(platformShell, /border-t border-white\/10 p-3 lg:hidden/);
});

test("player surfaces preserve unknown team resolution and known club semantics", () => {
  assert.match(playersPage, /buildTeamResolverPath\(teamId, sharedFilters\)/);
  assert.match(playersPage, /cb: "Zagueiro"/);
  assert.match(playersPage, /gk: "Goleiro"/);
  assert.match(playersPage, /st: "Centroavante"/);
  assert.match(playersPage, /identidades de jogadores no acervo bruto publicado/);
  assert.match(playersPage, /carreiras documentadas disponíveis/);
  assert.match(playerProfile, /item\.teamType === "club"/);
  assert.match(playerProfile, /attacker: "Atacante"/);
  assert.match(playerProfile, /category="clubs"/);
  assert.match(playerProfile, /aria-label={`Abrir \$\{item\.teamName\}`}/);
});
