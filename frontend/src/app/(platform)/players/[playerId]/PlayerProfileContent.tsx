"use client";

import type { ReactNode } from "react";

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";

import { usePlayerProfile } from "@/features/players/hooks";
import { PlayerHistorySection } from "@/features/players/components/PlayerHistorySection";
import { PlayerMatchesSection } from "@/features/players/components/PlayerMatchesSection";
import { PlayerOverviewSection } from "@/features/players/components/PlayerOverviewSection";
import { PlayerStatsSection } from "@/features/players/components/PlayerStatsSection";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { useInsights } from "@/shared/hooks/useInsights";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import {
  ProfileAlert,
  ProfileCoveragePill,
  ProfileKpi,
  ProfilePanel,
  ProfileShell,
  ProfileTag,
  ProfileTabs,
} from "@/shared/components/profile/ProfilePrimitives";
import { ProfileMedia } from "@/shared/components/profile/ProfileMedia";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";
import type { CoverageState } from "@/shared/types/coverage.types";
import {
  appendFilterQueryString,
  buildCanonicalTeamPath,
  buildMatchesPath,
  buildPlayersPath,
  buildSeasonHubTabPath,
  buildTeamResolverPath,
} from "@/shared/utils/context-routing";

type PlayerProfileContentProps = {
  playerId: string;
  contextOverride?: CompetitionSeasonContext | null;
  notice?: ReactNode;
};

const PLAYER_PROFILE_TABS = ["overview", "history", "matches", "stats"] as const;
type PlayerProfileTab = (typeof PLAYER_PROFILE_TABS)[number];

function isPlayerProfileTab(value: string | null | undefined): value is PlayerProfileTab {
  return typeof value === "string" && PLAYER_PROFILE_TABS.includes(value as PlayerProfileTab);
}

function resolvePlayerProfileTab(value: string | null | undefined): PlayerProfileTab {
  return isPlayerProfileTab(value) ? value : "overview";
}

function buildPlayerProfileTabHref(
  pathname: string,
  searchParams: URLSearchParams | Readonly<Pick<URLSearchParams, "toString">>,
  tab: PlayerProfileTab,
): string {
  const nextSearchParams = new URLSearchParams(searchParams.toString());

  if (tab === "overview") {
    nextSearchParams.delete("tab");
  } else {
    nextSearchParams.set("tab", tab);
  }

  const serialized = nextSearchParams.toString();
  return serialized.length > 0 ? `${pathname}?${serialized}` : pathname;
}

function resolveSectionCoverage(
  coverage: CoverageState | undefined,
  fallback: CoverageState,
): CoverageState {
  if (coverage) {
    return coverage;
  }

  return fallback;
}

function getPlayerMonogram(playerName: string): string {
  const initials = playerName
    .split(/\s+/)
    .map((token) => token.trim())
    .filter(Boolean)
    .map((token) => token[0]?.toUpperCase() ?? "")
    .join("")
    .slice(0, 3);

  return initials.length > 0 ? initials : "PLY";
}

export function PlayerProfileContent({
  playerId,
  contextOverride = null,
  notice = null,
}: PlayerProfileContentProps) {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { competitionId, seasonId, roundId, venue, lastN, dateRangeStart, dateRangeEnd } =
    useGlobalFiltersState();
  const activeTab = resolvePlayerProfileTab(searchParams.get("tab"));
  const profileQuery = usePlayerProfile(
    playerId,
    {
      includeRecentMatches: true,
      includeHistory: true,
      includeStats: true,
    },
    contextOverride,
  );
  const insightsQuery = useInsights({
    entityType: "player",
    entityId: playerId,
    filters: contextOverride
      ? {
          competitionId: contextOverride.competitionId,
          seasonId: contextOverride.seasonId,
        }
      : undefined,
  });
  const sharedFilters = {
    competitionId: contextOverride?.competitionId ?? competitionId,
    seasonId: contextOverride?.seasonId ?? seasonId,
    roundId,
    venue,
    lastN,
    dateRangeStart,
    dateRangeEnd,
  };
  const seasonHubHref = contextOverride
    ? buildSeasonHubTabPath(contextOverride, "calendar", sharedFilters)
    : null;
  const rankingsHref = contextOverride
    ? buildSeasonHubTabPath(contextOverride, "rankings", sharedFilters)
    : null;
  const playersHref = buildPlayersPath(sharedFilters);
  const matchesHref = buildMatchesPath(sharedFilters);

  if (profileQuery.isLoading) {
    return (
      <ProfileShell className="space-y-6">
        {notice}
        <header className="space-y-3">
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.22em] text-[#57657a]">
            Jogador
          </p>
          <h1 className="font-[family:var(--font-profile-headline)] text-4xl font-extrabold tracking-tight text-[#111c2d]">
            Carregando perfil do jogador
          </h1>
        </header>
        <LoadingSkeleton height={140} />
        <LoadingSkeleton height={110} />
        <LoadingSkeleton height={280} />
      </ProfileShell>
    );
  }

  if (profileQuery.isError && !profileQuery.data) {
    return (
      <ProfileShell className="space-y-6">
        {notice}
        <header className="space-y-3">
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.22em] text-[#57657a]">
            Jogador
          </p>
          <h1 className="font-[family:var(--font-profile-headline)] text-4xl font-extrabold tracking-tight text-[#111c2d]">
            Falha ao carregar perfil do jogador
          </h1>
        </header>
        <ProfileAlert title="Erro no carregamento" tone="critical">
          <p>{profileQuery.error?.message}</p>
        </ProfileAlert>
      </ProfileShell>
    );
  }

  if (profileQuery.isEmpty || !profileQuery.data) {
    return (
      <ProfileShell className="space-y-6">
        {notice}
        <header className="space-y-3">
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.22em] text-[#57657a]">
            Jogador
          </p>
          <h1 className="font-[family:var(--font-profile-headline)] text-4xl font-extrabold tracking-tight text-[#111c2d]">
            Perfil de jogador indisponível
          </h1>
        </header>
        <EmptyState
          title="Perfil indisponível"
          description="Nao ha dados suficientes para montar este perfil agora."
        />
      </ProfileShell>
    );
  }

  const { history, player, recentMatches, sectionCoverage, stats, summary } = profileQuery.data;
  const teamHref = player.teamId
    ? contextOverride
      ? appendFilterQueryString(
          buildCanonicalTeamPath(contextOverride, player.teamId),
          sharedFilters,
          ["competitionId", "seasonId"],
        )
      : buildTeamResolverPath(player.teamId, sharedFilters)
    : null;
  const overviewCoverage = resolveSectionCoverage(sectionCoverage?.overview, {
    ...profileQuery.coverage,
    label: sectionCoverage?.overview?.label ?? "Player overview coverage",
  });
  const historyCoverage = resolveSectionCoverage(sectionCoverage?.history, {
    status: history && history.length > 0 ? "complete" : "unknown",
    label: "Player history coverage",
  });
  const matchesCoverage = resolveSectionCoverage(sectionCoverage?.matches, {
    ...profileQuery.coverage,
    label: sectionCoverage?.matches?.label ?? "Player matches coverage",
  });
  const statsCoverage = resolveSectionCoverage(sectionCoverage?.stats, {
    status: stats ? "complete" : "unknown",
    label: "Player stats coverage",
  });
  const tabLinks = [
    { key: "overview" as const, label: "Overview", coverage: overviewCoverage, badge: "Resumo" },
    {
      key: "history" as const,
      label: "History",
      coverage: historyCoverage,
      badge: `${history?.length ?? 0} contextos`,
    },
    {
      key: "matches" as const,
      label: "Matches",
      coverage: matchesCoverage,
      badge: `${recentMatches?.length ?? 0} jogos`,
    },
    {
      key: "stats" as const,
      label: "Stats",
      coverage: statsCoverage,
      badge: `${stats?.trend?.length ?? 0} períodos`,
    },
  ];

  return (
    <ProfileShell className="space-y-6">
      {notice}

      <div className="flex flex-wrap items-center gap-2 text-[0.7rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
        <Link className="transition-colors hover:text-[#00513b]" href="/competitions">
          Competições
        </Link>
        {contextOverride ? (
          <>
            <span className="text-[#8fa097]">/</span>
            <Link className="transition-colors hover:text-[#00513b]" href={seasonHubHref ?? "/competitions"}>
              {contextOverride.competitionName}
            </Link>
            <span className="text-[#8fa097]">/</span>
          </>
        ) : (
          <>
            <span className="text-[#8fa097]">/</span>
          </>
        )}
        <Link className="transition-colors hover:text-[#00513b]" href={playersHref}>
          Jogadores
        </Link>
        <span className="text-[#8fa097]">/</span>
        <span>{player.playerName}</span>
      </div>

      <ProfilePanel className="space-y-6" tone="accent">
        <div className="flex flex-col gap-5 xl:flex-row xl:items-start xl:justify-between">
          <div className="flex items-start gap-5">
            <ProfileMedia
              alt={player.playerName}
              assetId={player.playerId}
              category="players"
              className="h-20 w-20 border-white/10 bg-white/12"
              fallback={getPlayerMonogram(player.playerName)}
              fallbackClassName="text-xl tracking-[0.08em] text-white"
              imageClassName="p-2.5"
              shape="circle"
              tone="contrast"
            />
            <div className="space-y-2">
              <p className="text-[0.72rem] font-semibold uppercase tracking-[0.22em] text-white/65">
                Jogador
              </p>
              <div className="flex flex-wrap items-center gap-2">
                <ProfileCoveragePill coverage={profileQuery.coverage} className="bg-white/16 text-white" />
                {contextOverride ? (
                  <ProfileTag className="bg-white/12 text-white/82">
                    {contextOverride.competitionName}
                  </ProfileTag>
                ) : null}
                {contextOverride ? (
                  <ProfileTag className="bg-white/12 text-white/82">
                    {contextOverride.seasonLabel}
                  </ProfileTag>
                ) : null}
                {player.teamName ? (
                  <ProfileTag className="bg-white/12 text-white/82">{player.teamName}</ProfileTag>
                ) : null}
                {player.nationality ? (
                  <ProfileTag className="bg-white/12 text-white/82">{player.nationality}</ProfileTag>
                ) : null}
              </div>
              <h1 className="font-[family:var(--font-profile-headline)] text-4xl font-extrabold tracking-tight text-white md:text-5xl">
                {player.playerName}
              </h1>
              <p className="max-w-3xl text-sm leading-6 text-white/74">
                Resumo, historico, partidas e tendencia do atleta em uma leitura unica dentro da
                temporada selecionada.
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            {teamHref ? (
              <Link
                className="inline-flex items-center rounded-full bg-white/10 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-white"
                href={teamHref}
              >
                Time
              </Link>
            ) : null}
            {rankingsHref ? (
              <Link
                className="inline-flex items-center rounded-full bg-white/10 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-white"
                href={rankingsHref}
              >
                Rankings
              </Link>
            ) : null}
            <Link
              className="inline-flex items-center rounded-full bg-white px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#003526]"
              href={matchesHref}
            >
              Abrir partidas
            </Link>
          </div>
        </div>

        <div className="grid gap-3 md:grid-cols-4">
          <ProfileKpi hint="Na temporada selecionada" invert label="Jogos" value={summary.matchesPlayed ?? "-"} />
          <ProfileKpi
            hint={`${summary.goals ?? 0} gols · ${summary.assists ?? 0} assist.`}
            invert
            label="G+A"
            value={(summary.goals ?? 0) + (summary.assists ?? 0)}
          />
          <ProfileKpi hint="Rating consolidado" invert label="Nota" value={summary.rating?.toFixed(2) ?? "-"} />
          <ProfileKpi hint="Contextos disponiveis" invert label="Historico" value={history?.length ?? 0} />
        </div>
      </ProfilePanel>

      {profileQuery.isError ? (
        <ProfileAlert title="Perfil carregado com alerta" tone="warning">
          <p>{profileQuery.error?.message}</p>
        </ProfileAlert>
      ) : null}

      <ProfileTabs
        ariaLabel="Player profile tabs"
        aside={<ProfileTag>{activeTab}</ProfileTag>}
        items={tabLinks.map((tabLink) => ({
          key: tabLink.key,
          label: tabLink.label,
          href: buildPlayerProfileTabHref(pathname, searchParams, tabLink.key),
          isActive: activeTab === tabLink.key,
          badge: tabLink.badge,
        }))}
      />

      {activeTab === "overview" ? (
        <PlayerOverviewSection
          coverage={overviewCoverage}
          insights={{
            coverage: insightsQuery.coverage,
            errorMessage: insightsQuery.error?.message,
            isError: insightsQuery.isError,
            isLoading: insightsQuery.isLoading,
            isPartial: insightsQuery.isPartial,
            items: insightsQuery.data ?? [],
          }}
          matchesHref={matchesHref}
          profile={profileQuery.data}
          rankingsHref={rankingsHref}
          seasonHubHref={seasonHubHref}
          teamHref={teamHref}
        />
      ) : null}

      {activeTab === "history" ? (
        <PlayerHistorySection coverage={historyCoverage} filters={sharedFilters} history={history} />
      ) : null}

      {activeTab === "matches" ? (
        <PlayerMatchesSection
          competitionContext={contextOverride}
          coverage={matchesCoverage}
          filters={sharedFilters}
          matches={recentMatches}
        />
      ) : null}

      {activeTab === "stats" ? (
        <PlayerStatsSection coverage={statsCoverage} stats={stats} summary={summary} />
      ) : null}
    </ProfileShell>
  );
}
