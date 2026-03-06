export type PageFilterState = "enabled" | "partial" | "disabled";

export interface PageFilterConfig {
  season: PageFilterState;
  round: PageFilterState;
  month: PageFilterState;
  lastN: PageFilterState;
  dateRange: PageFilterState;
  venue: PageFilterState;
}

const ALL_ENABLED_FILTERS: Readonly<PageFilterConfig> = Object.freeze({
  season: "enabled",
  round: "enabled",
  month: "enabled",
  lastN: "enabled",
  dateRange: "enabled",
  venue: "enabled",
});

const HOME_FILTERS: Readonly<PageFilterConfig> = Object.freeze({
  season: "enabled",
  round: "partial",
  month: "enabled",
  lastN: "enabled",
  dateRange: "enabled",
  venue: "partial",
});

const COMPETITION_FILTERS: Readonly<PageFilterConfig> = Object.freeze({
  season: "enabled",
  round: "enabled",
  month: "partial",
  lastN: "enabled",
  dateRange: "enabled",
  venue: "enabled",
});

const PLAYERS_FILTERS: Readonly<PageFilterConfig> = Object.freeze({
  season: "enabled",
  round: "partial",
  month: "partial",
  lastN: "enabled",
  dateRange: "enabled",
  venue: "enabled",
});

const MARKET_FILTERS: Readonly<PageFilterConfig> = Object.freeze({
  season: "enabled",
  round: "disabled",
  month: "partial",
  lastN: "partial",
  dateRange: "enabled",
  venue: "disabled",
});

const HEAD_TO_HEAD_FILTERS: Readonly<PageFilterConfig> = Object.freeze({
  season: "enabled",
  round: "disabled",
  month: "partial",
  lastN: "enabled",
  dateRange: "enabled",
  venue: "disabled",
});

const COACHES_FILTERS: Readonly<PageFilterConfig> = Object.freeze({
  season: "enabled",
  round: "disabled",
  month: "partial",
  lastN: "enabled",
  dateRange: "enabled",
  venue: "disabled",
});

const AUDIT_FILTERS: Readonly<PageFilterConfig> = Object.freeze({
  season: "enabled",
  round: "disabled",
  month: "partial",
  lastN: "partial",
  dateRange: "enabled",
  venue: "disabled",
});

type PageFilterRule = {
  prefix: string;
  config: Readonly<PageFilterConfig>;
};

const PAGE_FILTER_RULES: readonly PageFilterRule[] = [
  { prefix: "/competition", config: COMPETITION_FILTERS },
  { prefix: "/matches", config: ALL_ENABLED_FILTERS },
  { prefix: "/clubs", config: ALL_ENABLED_FILTERS },
  { prefix: "/players", config: PLAYERS_FILTERS },
  { prefix: "/rankings", config: ALL_ENABLED_FILTERS },
  { prefix: "/market", config: MARKET_FILTERS },
  { prefix: "/head-to-head", config: HEAD_TO_HEAD_FILTERS },
  { prefix: "/coaches", config: COACHES_FILTERS },
  { prefix: "/audit", config: AUDIT_FILTERS },
];

function matchesPrefix(pathname: string, prefix: string): boolean {
  if (prefix === "/") {
    return pathname === "/";
  }

  return pathname === prefix || pathname.startsWith(`${prefix}/`);
}

export function resolvePageFilterConfig(pathname: string | null | undefined): PageFilterConfig {
  const normalizedPathname = pathname && pathname.trim().length > 0 ? pathname.trim() : "/";

  if (matchesPrefix(normalizedPathname, "/")) {
    return { ...HOME_FILTERS };
  }

  const matchedRule = PAGE_FILTER_RULES.find((rule) => matchesPrefix(normalizedPathname, rule.prefix));
  return matchedRule ? { ...matchedRule.config } : { ...ALL_ENABLED_FILTERS };
}

export function isFilterEnabled(filterState: PageFilterState): boolean {
  return filterState !== "disabled";
}

export function isFilterPartial(filterState: PageFilterState): boolean {
  return filterState === "partial";
}
