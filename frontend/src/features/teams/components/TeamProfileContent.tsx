"use client";

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";

import { TeamHonorsSection } from "@/features/teams/components/TeamHonorsSection";
import { TeamJourneySection } from "@/features/teams/components/TeamJourneySection";
import { TeamMatchesSection } from "@/features/teams/components/TeamMatchesSection";
import { TeamSquadSection } from "@/features/teams/components/TeamSquadSection";
import { TeamStatsSection } from "@/features/teams/components/TeamStatsSection";
import { useTeamMatches } from "@/features/teams/hooks/useTeamMatches";
import { useTeamProfile } from "@/features/teams/hooks/useTeamProfile";
import type { TeamHonorsPreview } from "@/features/teams/types";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { ProfileMedia } from "@/shared/components/profile/ProfileMedia";
import {
  ProfileAlert,
  ProfileCoveragePill,
  ProfilePanel,
  ProfileShell,
  ProfileTag,
  ProfileTabs,
} from "@/shared/components/profile/ProfilePrimitives";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";
import {
  buildCanonicalClubPath,
  buildClubResolverPath,
  buildClubsPath,
  buildHeadToHeadPath,
  buildSeasonHubTabPath,
} from "@/shared/utils/context-routing";
import { formatDate } from "@/shared/utils/formatters";

type TeamProfileContentProps = {
  teamId: string;
  contextOverride: CompetitionSeasonContext;
  honorsPreview?: TeamHonorsPreview | null;
};

const TEAM_PROFILE_TABS = ["overview", "journey", "squad", "matches", "stats"] as const;
type TeamProfileTab = (typeof TEAM_PROFILE_TABS)[number];

const INTEGER_FORMATTER = new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 });

function isTeamProfileTab(value: string | null): value is TeamProfileTab {
  return typeof value === "string" && TEAM_PROFILE_TABS.includes(value as TeamProfileTab);
}

function resolveTeamProfileTab(value: string | null): TeamProfileTab {
  return isTeamProfileTab(value) ? value : "overview";
}

function buildTabHref(
  pathname: string,
  searchParams: Readonly<Pick<URLSearchParams, "toString">>,
  tab: TeamProfileTab,
): string {
  const next = new URLSearchParams(searchParams.toString());
  if (tab === "overview") {
    next.delete("tab");
  } else {
    next.set("tab", tab);
  }

  const query = next.toString();
  return query ? `${pathname}?${query}` : pathname;
}

function formatInteger(value: number | null | undefined): string {
  return typeof value === "number" && !Number.isNaN(value) ? INTEGER_FORMATTER.format(value) : "—";
}

function getMonogram(name: string): string {
  const initials = name
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() ?? "")
    .join("")
    .slice(0, 3);
  return initials || "CLB";
}

function getArchiveSpan(firstMatchAt?: string | null, lastMatchAt?: string | null): string {
  if (firstMatchAt && lastMatchAt) {
    return `${formatDate(firstMatchAt)} — ${formatDate(lastMatchAt)}`;
  }
  return firstMatchAt
    ? `Desde ${formatDate(firstMatchAt)}`
    : lastMatchAt
      ? `Até ${formatDate(lastMatchAt)}`
      : "Período ainda não informado";
}

function ScopeMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="border-l border-white/18 pl-4 first:border-l-0 first:pl-0">
      <p className="text-[0.64rem] font-bold uppercase tracking-[0.18em] text-white/54">{label}</p>
      <p className="mt-1 font-[family:var(--font-profile-headline)] text-2xl font-extrabold tracking-[-0.04em] text-white sm:text-3xl">{value}</p>
    </div>
  );
}

export function TeamProfileContent({ teamId, contextOverride }: TeamProfileContentProps) {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { roundId, venue, lastN, dateRangeStart, dateRangeEnd } = useGlobalFiltersState();
  const activeTab = resolveTeamProfileTab(searchParams.get("tab"));
  const profileQuery = useTeamProfile(
    teamId,
    {
      includeRecentMatches: false,
      includeSquad: activeTab === "squad",
      includeStats: activeTab === "stats",
    },
    contextOverride,
  );
  const matchesQuery = useTeamMatches(teamId, contextOverride, {
    pageSize: 10,
    sortBy: "kickoffAt",
    sortDirection: "desc",
  });
  const sharedFilters = {
    competitionId: contextOverride.competitionId,
    seasonId: contextOverride.seasonId,
    roundId,
    venue,
    lastN,
    dateRangeStart,
    dateRangeEnd,
  };
  const clubsHref = buildClubsPath(sharedFilters);
  const canonicalClubHref = buildCanonicalClubPath(contextOverride, teamId);
  const clubResolverHref = buildClubResolverPath(teamId, sharedFilters);
  const seasonHubHref = buildSeasonHubTabPath(contextOverride, "calendar", sharedFilters);
  const headToHeadHref = buildHeadToHeadPath({ ...sharedFilters, teamA: teamId });

  if (profileQuery.isLoading) {
    return (
      <ProfileShell className="space-y-6" aria-busy="true">
        <span className="sr-only" role="status">Carregando perfil do clube</span>
        <LoadingSkeleton height={240} />
        <LoadingSkeleton height={300} />
        <LoadingSkeleton height={90} />
      </ProfileShell>
    );
  }

  if (profileQuery.isError && !profileQuery.data) {
    const isNotFound = profileQuery.error?.status === 404;
    return (
      <ProfileShell className="space-y-6">
        <ProfileAlert title={isNotFound ? "Clube não encontrado" : "Não foi possível carregar o perfil"} tone="critical">
          <p>{isNotFound ? "Este clube não está disponível neste recorte publicado." : profileQuery.error?.message}</p>
          {!isNotFound ? (
            <button className="button-pill button-pill-secondary mt-3" onClick={() => void profileQuery.refetch()} type="button">Tentar novamente</button>
          ) : null}
        </ProfileAlert>
        <Link className="button-pill button-pill-primary w-fit" href={clubsHref}>Voltar para clubes</Link>
      </ProfileShell>
    );
  }

  if (profileQuery.isEmpty || !profileQuery.data) {
    return (
      <ProfileShell className="space-y-6">
        <EmptyState
          title="Perfil de clube indisponível neste recorte"
          description="O clube existe no acervo, mas não há dados suficientes para esta competição e temporada."
        />
        <Link className="button-pill button-pill-primary w-fit" href={clubsHref}>Voltar para clubes</Link>
      </ProfileShell>
    );
  }

  const { archive, honors, identity, sectionCoverage, squad, stats, summary, team } = profileQuery.data;
  const identityFacts = [
    identity.city && identity.countryOrTerritory
      ? `${identity.city}, ${identity.countryOrTerritory}`
      : identity.city ?? identity.countryOrTerritory,
    identity.foundedYear ? `Fundado em ${identity.foundedYear}` : null,
    identity.stadiumName ? `Estádio: ${identity.stadiumName}` : null,
  ].filter((value): value is string => Boolean(value));
  const tabs = [
    { key: "overview" as const, label: "Visão geral", badge: "Recorte" },
    { key: "journey" as const, label: "Jornada", badge: "História" },
    { key: "squad" as const, label: "Elenco", badge: squad ? `${squad.length}` : "Abrir" },
    { key: "matches" as const, label: "Partidas", badge: matchesQuery.data ? `${matchesQuery.data.items.length}` : "Abrir" },
    { key: "stats" as const, label: "Estatísticas", badge: stats ? "Detalhes" : "Abrir" },
  ];
  const isPartial =
    profileQuery.isPartial ||
    sectionCoverage?.identity?.status === "partial" ||
    sectionCoverage?.archive?.status === "partial";

  return (
    <ProfileShell className="space-y-6">
      <nav aria-label="Navegação estrutural" className="flex flex-wrap items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#57657a]">
        <Link className="hover:text-[#00513b]" href={clubsHref}>Clubes</Link>
        <span aria-hidden="true" className="text-[#9aa6a0]">/</span>
        <Link className="hover:text-[#00513b]" href={seasonHubHref}>{contextOverride.competitionName}</Link>
        <span aria-hidden="true" className="text-[#9aa6a0]">/</span>
        <span aria-current="page">{identity.officialName || team.teamName}</span>
      </nav>

      <ProfilePanel className="overflow-hidden bg-[#06271d] p-0" tone="accent">
        <div className="grid gap-8 p-5 sm:p-7 lg:grid-cols-[minmax(0,1fr)_minmax(19rem,0.42fr)] lg:p-9">
          <div className="flex min-w-0 flex-col justify-between gap-8">
            <div className="flex min-w-0 items-start gap-4 sm:items-center sm:gap-6">
              <ProfileMedia
                alt={`Escudo de ${team.teamName}`}
                assetId={team.visualAssetId ?? team.teamId}
                assetUrl={team.visualAssetUrl ?? identity.assetUrl}
                category="clubs"
                className="h-20 w-20 shrink-0 border border-white/18 bg-white/10 sm:h-28 sm:w-28"
                fallback={getMonogram(team.teamName)}
                href={canonicalClubHref}
                imageClassName="p-3"
                tone="contrast"
              />
              <div className="min-w-0">
                <p className="text-[0.68rem] font-bold uppercase tracking-[0.22em] text-white/56">Perfil de clube · identidade</p>
                <h1 className="mt-2 break-words font-[family:var(--font-profile-headline)] text-3xl font-extrabold leading-[0.94] tracking-[-0.055em] text-white sm:text-6xl">
                  {identity.officialName || team.teamName}
                </h1>
                <p className="mt-4 max-w-3xl text-sm leading-6 text-white/68">
                  {identityFacts.length > 0 ? identityFacts.join(" · ") : "Dados de origem ainda não documentados no acervo."}
                </p>
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <Link className="button-pill button-pill-on-dark" href={seasonHubHref}>Ver temporada</Link>
              <Link className="button-pill button-pill-on-dark" href={headToHeadHref}>Comparar clube</Link>
              <Link className="button-pill button-pill-on-dark" href={clubResolverHref}>Abrir arquivo do clube</Link>
            </div>
          </div>

          <aside className="border-t border-white/14 pt-6 lg:border-l lg:border-t-0 lg:pl-8 lg:pt-0">
            <p className="text-[0.66rem] font-bold uppercase tracking-[0.2em] text-white/55">
              {contextOverride.competitionName} · {contextOverride.seasonLabel}
            </p>
            <div className="mt-5 grid grid-cols-3 gap-3">
              <ScopeMetric label="Jogos" value={formatInteger(summary.matchesPlayed)} />
              <ScopeMetric label="Gols pró" value={formatInteger(summary.goalsFor)} />
              <ScopeMetric label="Gols contra" value={formatInteger(summary.goalsAgainst)} />
            </div>
            <p className="mt-6 text-sm leading-6 text-white/62">Campanha no recorte: {formatInteger(summary.wins)} vitórias, {formatInteger(summary.draws)} empates e {formatInteger(summary.losses)} derrotas.</p>
          </aside>
        </div>
      </ProfilePanel>

      {profileQuery.isFetching ? (
        <p aria-live="polite" className="text-xs font-semibold text-[#57657a]">Atualizando dados do clube…</p>
      ) : null}

      {profileQuery.isError ? (
        <ProfileAlert title="Perfil carregado com ressalvas" tone="warning">
          <p>{profileQuery.error?.message}</p>
          <button className="button-pill button-pill-secondary mt-3" onClick={() => void profileQuery.refetch()} type="button">Tentar novamente</button>
        </ProfileAlert>
      ) : null}

      {isPartial ? (
        <ProfileAlert title="Perfil parcial" tone="warning">
          <p>Algumas informações de identidade, arquivo ou desempenho ainda não estão cobertas por esta publicação.</p>
        </ProfileAlert>
      ) : null}

      {honors ? (
        <TeamHonorsSection honors={honors} />
      ) : (
        <ProfilePanel className="space-y-3">
          <p className="text-[0.68rem] font-bold uppercase tracking-[0.2em] text-[#57657a]">Conquistas documentadas</p>
          <h2 className="font-[family:var(--font-profile-headline)] text-3xl font-extrabold tracking-[-0.045em] text-[#111c2d]">Ainda sem registros publicados</h2>
          <p className="max-w-3xl text-sm leading-6 text-[#57657a]">Isso indica ausência de documentação nesta API, não ausência de conquistas na história do clube.</p>
        </ProfilePanel>
      )}

      <ProfilePanel className="grid gap-5 md:grid-cols-[minmax(0,1fr)_auto] md:items-end" tone="soft">
        <div>
          <p className="text-[0.68rem] font-bold uppercase tracking-[0.2em] text-[#57657a]">Arquivo publicado do clube</p>
          <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold tracking-[-0.04em] text-[#111c2d] md:text-3xl">
            {formatInteger(archive.matchesPlayed)} partidas em {formatInteger(archive.seasonCount)} temporadas
          </h2>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-[#57657a]">{formatInteger(archive.competitionCount)} competições · {getArchiveSpan(archive.firstMatchAt, archive.lastMatchAt)}.</p>
          <p className="mt-2 max-w-3xl text-xs leading-5 text-[#69778d]">O arquivo reúne somente o material publicado pela plataforma e não representa toda a história existente do clube.</p>
        </div>
        <ProfileCoveragePill
          coverage={{
            ...(sectionCoverage?.archive ?? profileQuery.coverage),
            label: "Cobertura do arquivo",
          }}
        />
      </ProfilePanel>

      <ProfileTabs
        ariaLabel="Detalhes do perfil do clube"
        aside={<ProfileTag>{contextOverride.seasonLabel}</ProfileTag>}
        items={tabs.map((tab) => ({
          key: tab.key,
          label: tab.label,
          badge: tab.badge,
          href: buildTabHref(pathname, searchParams, tab.key),
          isActive: activeTab === tab.key,
        }))}
      />

      {activeTab === "overview" ? (
        <ProfilePanel className="space-y-5" tone="soft">
          <header>
            <p className="text-[0.68rem] font-bold uppercase tracking-[0.2em] text-[#57657a]">Recorte ativo</p>
            <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold tracking-[-0.04em] text-[#111c2d] md:text-3xl">Partidas recentes disponíveis</h2>
          </header>

          {matchesQuery.isLoading ? (
            <div className="space-y-2"><LoadingSkeleton height={64} /><LoadingSkeleton height={64} /></div>
          ) : matchesQuery.data?.items.length ? (
            <ol className="divide-y divide-[rgba(191,201,195,0.42)] border-y border-[rgba(191,201,195,0.42)]">
              {matchesQuery.data.items.slice(0, 3).map((match) => (
                <li className="grid gap-2 py-3 text-sm sm:grid-cols-[7rem_minmax(0,1fr)_auto] sm:items-center" key={match.matchId}>
                  <time className="text-[#69778d]">{formatDate(match.kickoffAt)}</time>
                  <span className="font-semibold text-[#111c2d]">{match.homeTeamName ?? "Mandante"} × {match.awayTeamName ?? "Visitante"}</span>
                  <span className="font-bold text-[#00513b]">{formatInteger(match.homeScore)}–{formatInteger(match.awayScore)}</span>
                </li>
              ))}
            </ol>
          ) : matchesQuery.isError ? (
            <ProfileAlert title="Partidas indisponíveis" tone="warning"><p>{matchesQuery.error?.message}</p></ProfileAlert>
          ) : (
            <p className="text-sm leading-6 text-[#57657a]">Ainda não há partidas recentes publicadas para este recorte.</p>
          )}

          <div className="flex flex-wrap gap-2">
            <Link className="button-pill button-pill-primary" href={buildTabHref(pathname, searchParams, "matches")}>Ver partidas</Link>
            <Link className="button-pill button-pill-secondary" href={buildTabHref(pathname, searchParams, "journey")}>Abrir jornada</Link>
          </div>
        </ProfilePanel>
      ) : null}

      {activeTab === "journey" ? <TeamJourneySection competitionContext={contextOverride} teamId={teamId} /> : null}

      {activeTab === "squad" ? (
        <TeamSquadSection competitionContext={contextOverride} filters={sharedFilters} squad={squad} />
      ) : null}

      {activeTab === "matches" ? (
        <TeamMatchesSection
          competitionContext={contextOverride}
          errorMessage={matchesQuery.error?.message}
          filters={sharedFilters}
          isError={matchesQuery.isError}
          isLoading={matchesQuery.isLoading}
          matches={matchesQuery.data?.items ?? []}
          teamId={teamId}
        />
      ) : null}

      {activeTab === "stats" ? <TeamStatsSection stats={stats} /> : null}
    </ProfileShell>
  );
}
