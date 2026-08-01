import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import test from "node:test";

const read = (path) => readFileSync(path, "utf8");

const rankingsHub = read("src/app/(platform)/rankings/page.tsx");
const rankingDetail = read("src/app/(platform)/rankings/[rankingType]/page.tsx");
const worldCupRoute = read("src/app/(platform)/copa-do-mundo/rankings/page.tsx");
const routing = read("src/shared/utils/context-routing.ts");
const seasonPage = read("src/app/(platform)/competitions/[competitionKey]/seasons/[seasonLabel]/page.tsx");
const legacyPowerBiRoute = read("src/app/(platform)/analises/page.tsx");
const legacyAnalyticsRoute = read("src/app/(platform)/analytics/page.tsx");

test("ranking routes remain direct pages with their historical surfaces", () => {
  assert.match(rankingsHub, /listRankingsByEntity/);
  assert.doesNotMatch(rankingsHub, /fetchRanking/);
  assert.doesNotMatch(rankingsHub, /Times|jogadores e times/);
  assert.doesNotMatch(rankingsHub, /redirect/);
  assert.match(rankingDetail, /getRankingDefinition/);
  assert.match(rankingDetail, /RankingTable/);
  assert.equal(existsSync("src/app/(platform)/rankings/page.module.css"), true);
});

test("World Cup rankings keep their route and historical component", () => {
  assert.match(worldCupRoute, /WorldCupRankingsContent/);
  assert.equal(existsSync("src/features/world-cup/components/WorldCupRankingsContent.tsx"), true);
});

test("context navigation points to rankings and keeps season ranking tabs local", () => {
  assert.equal(routing.includes("return `/rankings${buildFilterQueryString"), true);
  assert.equal(routing.includes("return `/rankings/${encodePathSegment(rankingType)"), true);

  const seasonTabRouting = routing.slice(
    routing.indexOf("export function buildSeasonHubTabPath"),
    routing.indexOf("export function buildCanonicalPlayerPath"),
  );
  assert.doesNotMatch(seasonTabRouting, /buildAnalysesPath/);
  assert.doesNotMatch(seasonPage, /buildAnalysesPath|redirect/);
});

test("Power BI has an independent route while old links remain aliases", () => {
  const powerBiPage = "src/app/(platform)/power-bi/page.tsx";

  assert.equal(existsSync(powerBiPage), true);
  assert.match(read(powerBiPage), /<iframe/);
  assert.equal(legacyPowerBiRoute.includes("/power-bi"), true);
  assert.equal(legacyAnalyticsRoute.includes("/power-bi"), true);
});
