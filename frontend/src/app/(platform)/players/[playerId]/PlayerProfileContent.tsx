"use client";

import type { ReactNode } from "react";

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";

import { PlayerHistorySection } from "@/features/players/components/PlayerHistorySection";
import { PlayerMatchesSection } from "@/features/players/components/PlayerMatchesSection";
import { PlayerStatsSection } from "@/features/players/components/PlayerStatsSection";
import { usePlayerProfile } from "@/features/players/hooks";
import type { CareerTeamType, PlayerCareerTeam } from "@/features/players/types";
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
import { useComparisonStore } from "@/shared/stores/comparison.store";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";
import type { CoverageState } from "@/shared/types/coverage.types";
import {
  buildClubResolverPath,
  buildPlayersPath,
  buildSeasonHubTabPath,
  buildTeamResolverPath,
} from "@/shared/utils/context-routing";
import { formatDate } from "@/shared/utils/formatters";

type PlayerProfileContentProps = {
  playerId: string;
  contextOverride?: CompetitionSeasonContext | null;
  notice?: ReactNode;
};

const PLAYER_PROFILE_TABS = ["overview", "history", "matches", "stats"] as const;
type PlayerProfileTab = (typeof PLAYER_PROFILE_TABS)[number];

const INTEGER_FORMATTER = new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 });

const TEAM_TYPE_LABELS: Record<CareerTeamType, string> = {
  club: "Clube",
  national_team: "Seleção",
  representative: "Equipe representativa",
  other: "Outra equipe",
  unknown: "Equipe",
};

const POSITION_LABELS: Record<string, string> = {
  goalkeeper: "Goleiro",
  defender: "Defensor",
  midfielder: "Meio-campista",
  forward: "Atacante",
  "center forward": "Centroavante",
  "right winger": "Ponta direita",
  "left winger": "Ponta esquerda",
  "attacking midfielder": "Meia ofensivo",
  "defensive midfielder": "Volante",
  "left back": "Lateral esquerdo",
  "right back": "Lateral direito",
};

function isPlayerProfileTab(value: string | null): value is PlayerProfileTab {
  return typeof value === "string" && PLAYER_PROFILE_TABS.includes(value as PlayerProfileTab);
}

function resolvePlayerProfileTab(value: string | null): PlayerProfileTab {
  return isPlayerProfileTab(value) ? value : "overview";
}

function buildTabHref(
  pathname: string,
  searchParams: Readonly<Pick<URLSearchParams, "toString">>,
  tab: PlayerProfileTab,
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

function formatPosition(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? POSITION_LABELS[normalized.toLowerCase()] ?? normalized : null;
}

function getMonogram(name: string): string {
  const initials = name
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() ?? "")
    .join("")
    .slice(0, 3);
  return initials || "JOG";
}

function getCareerSpan(firstMatchAt?: string | null, lastMatchAt?: string | null): string {
  const first = firstMatchAt ? new Date(firstMatchAt).getUTCFullYear() : null;
  const last = lastMatchAt ? new Date(lastMatchAt).getUTCFullYear() : null;

  if (first && last) {
    return first === last ? String(first) : `${first}–${last}`;
  }

  return first ? `Desde ${first}` : last ? `Até ${last}` : "Período não informado";
}

function resolveCoverage(coverage: CoverageState | undefined, fallback: CoverageState): CoverageState {
  return coverage ?? fallback;
}

function PlayerSummaryMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="border-l border-white/18 pl-4 first:border-l-0 first:pl-0">
      <p className="text-[0.64rem] font-bold uppercase tracking-[0.18em] text-white/55">{label}</p>
      <p className="mt-1 font-[family:var(--font-profile-headline)] text-2xl font-extrabold tracking-[-0.04em] text-white sm:text-3xl">
        {value}
      </p>
    </div>
  );
}

function CareerTeamCard({
  filters,
  item,
}: {
  filters: Parameters<typeof buildClubResolverPath>[1];
  item: PlayerCareerTeam;
}) {
  const href =
    item.teamType === "club"
      ? buildClubResolverPath(item.teamId, filters)
      : buildTeamResolverPath(item.teamId, filters);

  return (
    <article className="flex min-h-full flex-col rounded-[1.35rem] border border-[rgba(191,201,195,0.48)] bg-white/74 p-4 shadow-[0_18px_42px_-38px_rgba(17,28,45,0.36)]">
      <div className="flex items-start gap-3">
        <ProfileMedia
          alt={item.teamName}
          assetId={item.teamId}
          category="clubs"
          className="h-12 w-12 shrink-0 border border-[rgba(191,201,195,0.42)] bg-[#f4f7f2]"
          fallback={getMonogram(item.teamName)}
          href={href}
          imageClassName="p-2"
        />
        <div className="min-w-0 flex-1">
          <p className="text-[0.64rem] font-bold uppercase tracking-[0.17em] text-[#69778d]">
            {TEAM_TYPE_LABELS[item.teamType]}
          </p>
          <h3 className="mt-1 break-words text-base font-bold text-[#111c2d]">
            <Link className="hover:text-[#00513b]" href={href}>
              {item.teamName}
            </Link>
          </h3>
          <p className="mt-1 text-sm text-[#57657a]">{getCareerSpan(item.firstMatchAt, item.lastMatchAt)}</p>
        </div>
      </div>

      <dl className="mt-5 grid grid-cols-2 gap-x-4 gap-y-3 border-t border-[rgba(191,201,195,0.42)] pt-4 text-sm sm:grid-cols-4">
        <div>
          <dt className="text-xs text-[#69778d]">Jogos</dt>
          <dd className="mt-1 font-bold text-[#111c2d]">{formatInteger(item.matchesPlayed)}</dd>
        </div>
        <div>
          <dt className="text-xs text-[#69778d]">Gols</dt>
          <dd className="mt-1 font-bold text-[#111c2d]">{formatInteger(item.goals)}</dd>
        </div>
        <div>
          <dt className="text-xs text-[#69778d]">Assist.</dt>
          <dd className="mt-1 font-bold text-[#111c2d]">{formatInteger(item.assists)}</dd>
        </div>
        <div>
          <dt className="text-xs text-[#69778d]">Temporadas</dt>
          <dd className="mt-1 font-bold text-[#111c2d]">{formatInteger(item.seasonCount)}</dd>
        </div>
      </dl>
    </article>
  );
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
      includeHistory: true,
      includeRecentMatches: true,
      includeStats: true,
      stageId: searchParams.get("stageId"),
      stageFormat: searchParams.get("stageFormat"),
    },
    contextOverride,
  );
  const comparisonEntityType = useComparisonStore((state) => state.entityType);
  const selectedIds = useComparisonStore((state) => state.selectedIds);
  const addComparison = useComparisonStore((state) => state.add);
  const removeComparison = useComparisonStore((state) => state.remove);
  const setComparisonEntityType = useComparisonStore((state) => state.setEntityType);
  const isSelectedForComparison =
    comparisonEntityType === "player" && selectedIds.includes(playerId);

  const sharedFilters = {
    competitionId: contextOverride?.competitionId ?? competitionId,
    seasonId: contextOverride?.seasonId ?? seasonId,
    roundId,
    venue,
    lastN,
    dateRangeStart,
    dateRangeEnd,
  };
  const playersHref = buildPlayersPath(sharedFilters);
  const seasonHubHref = contextOverride
    ? buildSeasonHubTabPath(contextOverride, "calendar", sharedFilters)
    : null;

  if (profileQuery.isLoading) {
    return (
      <ProfileShell className="space-y-6" aria-busy="true">
        {notice}
        <span className="sr-only" role="status">Carregando perfil do jogador</span>
        <LoadingSkeleton height={220} />
        <LoadingSkeleton height={300} />
        <LoadingSkeleton height={90} />
      </ProfileShell>
    );
  }

  if (profileQuery.isError && !profileQuery.data) {
    const isNotFound = profileQuery.error?.status === 404;
    return (
      <ProfileShell className="space-y-6">
        {notice}
        <ProfileAlert title={isNotFound ? "Jogador não encontrado" : "Não foi possível carregar o perfil"} tone="critical">
          <p>{isNotFound ? "Este jogador não está disponível no acervo publicado." : profileQuery.error?.message}</p>
          {!isNotFound ? (
            <button className="button-pill button-pill-secondary mt-3" onClick={() => void profileQuery.refetch()} type="button">
              Tentar novamente
            </button>
          ) : null}
        </ProfileAlert>
        <Link className="button-pill button-pill-primary w-fit" href={playersHref}>Voltar para jogadores</Link>
      </ProfileShell>
    );
  }

  if (profileQuery.isEmpty || !profileQuery.data) {
    return (
      <ProfileShell className="space-y-6">
        {notice}
        <EmptyState
          title="Perfil indisponível"
          description="Há uma referência para este jogador, mas ainda não há conteúdo suficiente para publicar o perfil."
        />
        <Link className="button-pill button-pill-primary w-fit" href={playersHref}>Voltar para jogadores</Link>
      </ProfileShell>
    );
  }

  const { career, history, player, profileMeta, recentMatches, sectionCoverage, stats, summary } =
    profileQuery.data;
  const displayPosition = formatPosition(profileMeta.worldCup?.primaryPosition ?? player.position);
  const careerTeams = career.teams ?? [];
  const clubs = careerTeams.filter((team) => team.teamType === "club");
  const nationalTeams = careerTeams.filter((team) => team.teamType === "national_team");
  const otherTeams = careerTeams.filter(
    (team) => team.teamType !== "club" && team.teamType !== "national_team",
  );
  const careerHasDetails = careerTeams.length > 0;
  const scopeLabel = contextOverride
    ? `${contextOverride.competitionName} · ${contextOverride.seasonLabel}`
    : "Recorte ativo";
  const overviewCoverage = resolveCoverage(sectionCoverage?.overview, profileQuery.coverage);
  const historyCoverage = resolveCoverage(sectionCoverage?.history, {
    status: history?.length ? "complete" : "unknown",
    label: "Cobertura do histórico",
  });
  const matchesCoverage = resolveCoverage(sectionCoverage?.matches, profileQuery.coverage);
  const statsCoverage = resolveCoverage(sectionCoverage?.stats, {
    status: stats ? "complete" : "unknown",
    label: "Cobertura estatística",
  });
  const tabs = [
    { key: "overview" as const, label: "Visão geral", badge: "Recorte" },
    { key: "history" as const, label: "Histórico", badge: `${history?.length ?? 0}` },
    { key: "matches" as const, label: "Partidas", badge: `${recentMatches?.length ?? 0}` },
    { key: "stats" as const, label: "Estatísticas", badge: stats ? "Detalhes" : "—" },
  ];

  function toggleComparison() {
    if (isSelectedForComparison) {
      removeComparison(playerId);
      return;
    }

    if (comparisonEntityType !== "player") {
      setComparisonEntityType("player");
    }
    addComparison(playerId);
  }

  return (
    <ProfileShell className="space-y-6">
      {notice}

      <nav aria-label="Navegação estrutural" className="flex flex-wrap items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#57657a]">
        <Link className="hover:text-[#00513b]" href={playersHref}>Jogadores</Link>
        <span aria-hidden="true" className="text-[#9aa6a0]">/</span>
        <span aria-current="page">{player.playerName}</span>
      </nav>

      <ProfilePanel className="overflow-hidden bg-[#06271d] p-0" tone="accent">
        <div className="grid gap-8 p-5 sm:p-7 lg:grid-cols-[minmax(0,1fr)_minmax(19rem,0.44fr)] lg:p-9">
          <div className="flex min-w-0 flex-col justify-between gap-8">
            <div className="flex min-w-0 items-start gap-4 sm:items-center sm:gap-6">
              <ProfileMedia
                alt={player.playerName}
                assetId={profileMeta.worldCup?.imageAssetId ?? player.playerId}
                category="players"
                className="h-20 w-20 shrink-0 border border-white/18 bg-white/10 sm:h-28 sm:w-28"
                fallback={getMonogram(player.playerName)}
                fallbackClassName="text-xl tracking-[0.08em] text-white"
                href={pathname}
                imageClassName="p-2"
                shape="circle"
                tone="contrast"
              />
              <div className="min-w-0">
                <p className="text-[0.68rem] font-bold uppercase tracking-[0.22em] text-white/56">Carreira documentada</p>
                <h1 className="mt-2 break-words font-[family:var(--font-profile-headline)] text-4xl font-extrabold leading-[0.94] tracking-[-0.055em] text-white sm:text-6xl">
                  {player.playerName}
                </h1>
                <p className="mt-4 text-sm leading-6 text-white/68">
                  {[displayPosition, player.nationality].filter(Boolean).join(" · ") || "Identidade esportiva disponível no acervo"}
                </p>
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <Link className="button-pill button-pill-on-dark" href={playersHref}>Explorar jogadores</Link>
              {seasonHubHref ? <Link className="button-pill button-pill-on-dark" href={seasonHubHref}>Ver contexto</Link> : null}
              <button
                aria-pressed={isSelectedForComparison}
                className="button-pill button-pill-on-dark"
                onClick={toggleComparison}
                type="button"
              >
                {isSelectedForComparison ? "Remover da comparação" : "Adicionar à comparação"}
              </button>
            </div>
          </div>

          <aside className="border-t border-white/14 pt-6 lg:border-l lg:border-t-0 lg:pl-8 lg:pt-0">
            <p className="text-[0.66rem] font-bold uppercase tracking-[0.2em] text-white/55">{scopeLabel}</p>
            <div className="mt-5 grid grid-cols-3 gap-3">
              <PlayerSummaryMetric label="Jogos" value={formatInteger(summary.matchesPlayed)} />
              <PlayerSummaryMetric label="Gols" value={formatInteger(summary.goals)} />
              <PlayerSummaryMetric label="Assist." value={formatInteger(summary.assists)} />
            </div>
            <p className="mt-6 text-sm leading-6 text-white/62">
              Estes números pertencem ao recorte ativo. A trajetória abaixo reúne o que está documentado no acervo publicado.
            </p>
          </aside>
        </div>
      </ProfilePanel>

      {profileQuery.isFetching ? (
        <p aria-live="polite" className="text-xs font-semibold text-[#57657a]">Atualizando dados do perfil…</p>
      ) : null}

      {profileQuery.isError ? (
        <ProfileAlert title="Perfil carregado com ressalvas" tone="warning">
          <p>{profileQuery.error?.message}</p>
          <button className="button-pill button-pill-secondary mt-3" onClick={() => void profileQuery.refetch()} type="button">Tentar novamente</button>
        </ProfileAlert>
      ) : null}

      {profileQuery.isPartial || overviewCoverage.status === "partial" ? (
        <ProfileAlert title="Perfil parcial" tone="warning">
          <p>Alguns períodos ou estatísticas ainda não estão cobertos por esta publicação.</p>
        </ProfileAlert>
      ) : null}

      <ProfilePanel className="space-y-6">
        <header className="grid gap-4 md:grid-cols-[minmax(0,1fr)_auto] md:items-end">
          <div>
            <p className="text-[0.68rem] font-bold uppercase tracking-[0.2em] text-[#57657a]">Trajetória</p>
            <h2 className="mt-2 max-w-3xl font-[family:var(--font-profile-headline)] text-3xl font-extrabold tracking-[-0.045em] text-[#111c2d] md:text-4xl">
              Clubes e seleções documentados
            </h2>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-[#57657a]">
              {careerHasDetails
                ? `${getCareerSpan(career.firstMatchAt, career.lastMatchAt)} · ${formatInteger(career.competitionCount)} competições · ${formatInteger(career.seasonCount)} temporadas.`
                : "A identidade do jogador está publicada, mas a sequência detalhada de equipes ainda não está disponível."}
            </p>
          </div>
          <ProfileCoveragePill
            coverage={{ ...profileQuery.coverage, label: "Cobertura do perfil" }}
          />
        </header>

        {careerHasDetails ? (
          <div className="space-y-7">
            {clubs.length > 0 ? (
              <section aria-labelledby="player-career-clubs">
                <h3 className="text-sm font-bold uppercase tracking-[0.17em] text-[#57657a]" id="player-career-clubs">Clubes</h3>
                <div className="mt-3 grid gap-3 lg:grid-cols-2">
                  {clubs.map((item) => <CareerTeamCard filters={sharedFilters} item={item} key={`club-${item.teamId}`} />)}
                </div>
              </section>
            ) : null}

            {nationalTeams.length > 0 ? (
              <section aria-labelledby="player-career-national-teams">
                <h3 className="text-sm font-bold uppercase tracking-[0.17em] text-[#57657a]" id="player-career-national-teams">Seleções</h3>
                <div className="mt-3 grid gap-3 lg:grid-cols-2">
                  {nationalTeams.map((item) => <CareerTeamCard filters={sharedFilters} item={item} key={`selection-${item.teamId}`} />)}
                </div>
              </section>
            ) : profileMeta.worldCup?.teamNames.length ? (
              <section aria-labelledby="player-world-cup-teams">
                <h3 className="text-sm font-bold uppercase tracking-[0.17em] text-[#57657a]" id="player-world-cup-teams">Seleções em Copas</h3>
                <p className="mt-3 text-sm leading-6 text-[#111c2d]">
                  {profileMeta.worldCup.teamNames.join(" · ")} · {formatInteger(profileMeta.worldCup.editionCount)} edições · {formatInteger(profileMeta.worldCup.goalCount)} gols documentados.
                </p>
              </section>
            ) : null}

            {otherTeams.length > 0 ? (
              <section aria-labelledby="player-career-other-teams">
                <h3 className="text-sm font-bold uppercase tracking-[0.17em] text-[#57657a]" id="player-career-other-teams">Outras equipes</h3>
                <div className="mt-3 grid gap-3 lg:grid-cols-2">
                  {otherTeams.map((item) => <CareerTeamCard filters={sharedFilters} item={item} key={`other-${item.teamId}`} />)}
                </div>
              </section>
            ) : null}
          </div>
        ) : (
          <EmptyState
            title="Carreira detalhada ainda não publicada"
            description="Este perfil permanece acessível e será enriquecido conforme novas passagens forem documentadas."
          />
        )}

        <p className="border-t border-[rgba(191,201,195,0.42)] pt-4 text-xs leading-5 text-[#69778d]">
          O acervo publicado não pretende representar toda a carreira existente fora da plataforma.
        </p>
      </ProfilePanel>

      <ProfileTabs
        ariaLabel="Detalhes do perfil do jogador"
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
          <header className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <p className="text-[0.68rem] font-bold uppercase tracking-[0.2em] text-[#57657a]">Recorte ativo</p>
              <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold tracking-[-0.04em] text-[#111c2d] md:text-3xl">Partidas recentes disponíveis</h2>
            </div>
            <ProfileCoveragePill coverage={matchesCoverage} />
          </header>

          {recentMatches?.length ? (
            <ol className="divide-y divide-[rgba(191,201,195,0.42)] border-y border-[rgba(191,201,195,0.42)]">
              {recentMatches.slice(0, 3).map((match) => (
                <li className="grid gap-2 py-3 text-sm sm:grid-cols-[7rem_minmax(0,1fr)_auto] sm:items-center" key={match.fixtureId}>
                  <time className="text-[#69778d]">{formatDate(match.playedAt)}</time>
                  <span className="font-semibold text-[#111c2d]">{match.teamName ?? player.teamName ?? "Equipe"} × {match.opponentName ?? "Adversário"}</span>
                  <span className="font-bold text-[#00513b]">{formatInteger(match.goalsFor)}–{formatInteger(match.goalsAgainst)}</span>
                </li>
              ))}
            </ol>
          ) : (
            <p className="text-sm leading-6 text-[#57657a]">Ainda não há partidas recentes publicadas para este recorte.</p>
          )}

          <div className="flex flex-wrap gap-2">
            <Link className="button-pill button-pill-primary" href={buildTabHref(pathname, searchParams, "matches")}>Ver partidas</Link>
            <Link className="button-pill button-pill-secondary" href={buildTabHref(pathname, searchParams, "stats")}>Abrir estatísticas</Link>
          </div>
        </ProfilePanel>
      ) : null}

      {activeTab === "history" ? (
        <PlayerHistorySection coverage={historyCoverage} filters={sharedFilters} history={history} profileMeta={profileMeta} />
      ) : null}

      {activeTab === "matches" ? (
        <PlayerMatchesSection
          competitionContext={contextOverride}
          coverage={matchesCoverage}
          filters={sharedFilters}
          matches={recentMatches}
          profileMeta={profileMeta}
        />
      ) : null}

      {activeTab === "stats" ? (
        <PlayerStatsSection coverage={statsCoverage} profileMeta={profileMeta} stats={stats} summary={summary} />
      ) : null}
    </ProfileShell>
  );
}
