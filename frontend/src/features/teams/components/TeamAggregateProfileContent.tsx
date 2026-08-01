"use client";

import Link from "next/link";

import { TeamHonorsSection } from "@/features/teams/components/TeamHonorsSection";
import { useTeamContexts } from "@/features/teams/hooks/useTeamContexts";
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
} from "@/shared/components/profile/ProfilePrimitives";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";
import {
  buildCanonicalClubPath,
  buildClubResolverPath,
  buildClubsPath,
  buildHeadToHeadPath,
} from "@/shared/utils/context-routing";
import { formatDate } from "@/shared/utils/formatters";

type TeamAggregateProfileContentProps = {
  teamId: string;
  honorsPreview?: TeamHonorsPreview | null;
};

const INTEGER_FORMATTER = new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 });

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

function groupContexts(contexts: CompetitionSeasonContext[]) {
  return Array.from(
    contexts.reduce((groups, context) => {
      const key = `${context.competitionId}-${context.competitionKey}`;
      const current = groups.get(key) ?? {
        competitionId: context.competitionId,
        competitionKey: context.competitionKey,
        competitionName: context.competitionName,
        contexts: [] as CompetitionSeasonContext[],
      };
      current.contexts.push(context);
      groups.set(key, current);
      return groups;
    }, new Map<string, { competitionId: string; competitionKey: string; competitionName: string; contexts: CompetitionSeasonContext[] }>()),
  ).map(([, group]) => group);
}

function ArchiveMetric({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline justify-between gap-4 py-3 first:pt-0 last:pb-0">
      <p className="text-[0.64rem] font-bold uppercase tracking-[0.16em] text-white/54">{label}</p>
      <p className="mt-1 font-[family:var(--font-profile-headline)] text-2xl font-extrabold tracking-[-0.04em] text-white sm:text-3xl">{value}</p>
    </div>
  );
}

export function TeamAggregateProfileContent({ teamId }: TeamAggregateProfileContentProps) {
  const { competitionId, seasonId, roundId, venue, lastN, dateRangeStart, dateRangeEnd } =
    useGlobalFiltersState();
  const contextsQuery = useTeamContexts(teamId);
  const defaultContext = contextsQuery.data?.defaultContext ?? null;
  const profileQuery = useTeamProfile(
    teamId,
    { includeRecentMatches: false, includeSquad: false, includeStats: false },
    defaultContext,
  );
  const sharedFilters = {
    competitionId,
    seasonId,
    roundId,
    venue,
    lastN,
    dateRangeStart,
    dateRangeEnd,
  };
  const clubsHref = buildClubsPath(sharedFilters);
  const headToHeadHref = buildHeadToHeadPath({ ...sharedFilters, teamA: teamId });

  if (contextsQuery.isLoading || (defaultContext && profileQuery.isLoading)) {
    return (
      <ProfileShell className="space-y-6" aria-busy="true">
        <span className="sr-only" role="status">Carregando perfil do clube</span>
        <LoadingSkeleton height={240} />
        <LoadingSkeleton height={300} />
        <LoadingSkeleton height={220} />
      </ProfileShell>
    );
  }

  if ((contextsQuery.isError && !contextsQuery.data) || (profileQuery.isError && !profileQuery.data)) {
    const error = profileQuery.error ?? contextsQuery.error;
    const isNotFound = error?.status === 404;
    return (
      <ProfileShell className="space-y-6">
        <ProfileAlert title={isNotFound ? "Clube não encontrado" : "Não foi possível carregar o clube"} tone="critical">
          <p>{isNotFound ? "Este clube não está disponível no acervo publicado." : error?.message}</p>
          {!isNotFound ? (
            <button
              className="button-pill button-pill-secondary mt-3"
              onClick={() => {
                void contextsQuery.refetch();
                void profileQuery.refetch();
              }}
              type="button"
            >
              Tentar novamente
            </button>
          ) : null}
        </ProfileAlert>
        <Link className="button-pill button-pill-primary w-fit" href={clubsHref}>Voltar para clubes</Link>
      </ProfileShell>
    );
  }

  if (!defaultContext || contextsQuery.isEmpty || !profileQuery.data) {
    return (
      <ProfileShell className="space-y-6">
        <EmptyState
          title="Perfil de clube indisponível"
          description="O clube foi identificado, mas ainda não possui uma participação publicada que permita montar o perfil."
        />
        <Link className="button-pill button-pill-primary w-fit" href={clubsHref}>Voltar para clubes</Link>
      </ProfileShell>
    );
  }

  const { archive, honors, identity, team } = profileQuery.data;
  const availableContexts = contextsQuery.data?.availableContexts ?? [];
  const contextGroups = groupContexts(availableContexts);
  const identityFacts = [
    identity.city && identity.countryOrTerritory
      ? `${identity.city}, ${identity.countryOrTerritory}`
      : identity.city ?? identity.countryOrTerritory,
    identity.foundedYear ? `Fundado em ${identity.foundedYear}` : null,
    identity.stadiumName ? `Estádio: ${identity.stadiumName}` : null,
  ].filter((value): value is string => Boolean(value));
  const profileIsPartial =
    profileQuery.isPartial ||
    contextsQuery.isPartial ||
    profileQuery.data.sectionCoverage?.archive?.status === "partial" ||
    profileQuery.data.sectionCoverage?.identity?.status === "partial";

  return (
    <ProfileShell className="space-y-6">
      <nav aria-label="Navegação estrutural" className="flex flex-wrap items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#57657a]">
        <Link className="hover:text-[#00513b]" href={clubsHref}>Clubes</Link>
        <span aria-hidden="true" className="text-[#9aa6a0]">/</span>
        <span aria-current="page">{identity.officialName || team.teamName}</span>
      </nav>

      <ProfilePanel className="overflow-hidden bg-[#06271d] p-0" tone="accent">
        <div className="grid gap-8 p-5 sm:p-7 lg:grid-cols-[minmax(0,1fr)_minmax(19rem,0.42fr)] lg:p-9">
          <div className="flex min-w-0 flex-col justify-between gap-8">
            <div className="flex min-w-0 items-start gap-4 sm:items-center sm:gap-6">
              <ProfileMedia
                alt={`Escudo de ${team.teamName}`}
                assetId={team.teamId}
                category="clubs"
                className="h-20 w-20 shrink-0 border border-white/18 bg-white/10 sm:h-28 sm:w-28"
                fallback={getMonogram(team.teamName)}
                href={buildClubResolverPath(team.teamId, sharedFilters)}
                imageClassName="p-3"
                tone="contrast"
              />
              <div className="min-w-0">
                <p className="text-[0.68rem] font-bold uppercase tracking-[0.22em] text-white/56">Identidade do clube</p>
                <h1 className="mt-2 break-words font-[family:var(--font-profile-headline)] text-3xl font-extrabold leading-[0.94] tracking-[-0.055em] text-white sm:text-6xl">
                  {identity.officialName || team.teamName}
                </h1>
                <p className="mt-4 max-w-3xl text-sm leading-6 text-white/68">
                  {identityFacts.length > 0 ? identityFacts.join(" · ") : "Dados de origem ainda não documentados no acervo."}
                </p>
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              {defaultContext ? (
                <Link className="button-pill button-pill-on-dark" href={buildCanonicalClubPath(defaultContext, teamId)}>
                  Abrir temporada mais recente
                </Link>
              ) : null}
              <Link className="button-pill button-pill-on-dark" href={headToHeadHref}>Comparar clube</Link>
              <Link className="button-pill button-pill-on-dark" href={clubsHref}>Ver clubes no recorte</Link>
            </div>
          </div>

          <aside className="border-t border-white/14 pt-6 lg:border-l lg:border-t-0 lg:pl-8 lg:pt-0">
            <p className="text-[0.66rem] font-bold uppercase tracking-[0.2em] text-white/55">Arquivo publicado</p>
            <div className="mt-5 divide-y divide-white/14">
              <ArchiveMetric label="Partidas" value={formatInteger(archive.matchesPlayed)} />
              <ArchiveMetric label="Temporadas" value={formatInteger(archive.seasonCount)} />
              <ArchiveMetric label="Competições" value={formatInteger(archive.competitionCount)} />
            </div>
            <p className="mt-6 text-sm leading-6 text-white/62">{getArchiveSpan(archive.firstMatchAt, archive.lastMatchAt)}</p>
            <p className="mt-2 text-xs leading-5 text-white/52">O arquivo descreve apenas o material publicado pela plataforma; não é um histórico totalizante do clube.</p>
          </aside>
        </div>
      </ProfilePanel>

      {contextsQuery.isFetching || profileQuery.isFetching ? (
        <p aria-live="polite" className="text-xs font-semibold text-[#57657a]">Atualizando arquivo do clube…</p>
      ) : null}

      {contextsQuery.isError || profileQuery.isError ? (
        <ProfileAlert title="Perfil carregado com ressalvas" tone="warning">
          <p>{profileQuery.error?.message ?? contextsQuery.error?.message}</p>
          <button
            className="button-pill button-pill-secondary mt-3"
            onClick={() => {
              void contextsQuery.refetch();
              void profileQuery.refetch();
            }}
            type="button"
          >
            Tentar novamente
          </button>
        </ProfileAlert>
      ) : null}

      {profileIsPartial ? (
        <ProfileAlert title="Arquivo parcial" tone="warning">
          <p>Algumas temporadas, dados de identidade ou seções do clube ainda não estão cobertos pela publicação atual.</p>
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

      <ProfilePanel className="space-y-6">
        <header className="grid gap-4 md:grid-cols-[minmax(0,1fr)_auto] md:items-end">
          <div>
            <p className="text-[0.68rem] font-bold uppercase tracking-[0.2em] text-[#57657a]">Participações no acervo</p>
            <h2 className="mt-2 max-w-3xl font-[family:var(--font-profile-headline)] text-3xl font-extrabold tracking-[-0.045em] text-[#111c2d] md:text-4xl">Competições e temporadas disponíveis</h2>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-[#57657a]">Escolha um recorte para abrir jornada, elenco, partidas e estatísticas com contexto explícito.</p>
          </div>
          <ProfileCoveragePill
            coverage={{ ...contextsQuery.coverage, label: "Cobertura dos recortes" }}
          />
        </header>

        {contextGroups.length > 0 ? (
          <div className="divide-y divide-[rgba(191,201,195,0.42)] border-y border-[rgba(191,201,195,0.42)]">
            {contextGroups.map((group, index) => (
              <details className="group py-4" key={`${group.competitionId}-${group.competitionKey}`} open={index === 0}>
                <summary className="flex min-h-11 cursor-pointer list-none items-center justify-between gap-4 rounded-lg px-1 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[#00513b] [&::-webkit-details-marker]:hidden">
                  <span>
                    <span className="block font-bold text-[#111c2d]">{group.competitionName}</span>
                    <span className="mt-1 block text-sm text-[#69778d]">{formatInteger(group.contexts.length)} temporadas publicadas</span>
                  </span>
                  <svg aria-hidden="true" className="h-5 w-5 shrink-0 text-[#57657a] transition-transform duration-200 group-open:rotate-180 motion-reduce:transition-none" fill="none" viewBox="0 0 20 20">
                    <path d="m6 8 4 4 4-4" stroke="currentColor" strokeLinecap="round" strokeWidth="1.7" />
                  </svg>
                </summary>
                <div className="grid gap-2 pb-1 pt-3 sm:grid-cols-2 lg:grid-cols-3">
                  {group.contexts.map((context) => (
                    <Link
                      className="flex min-h-12 items-center justify-between gap-3 rounded-xl border border-[rgba(191,201,195,0.48)] bg-white/74 px-4 py-3 text-sm font-semibold text-[#111c2d] hover:border-[#8bd6b6] hover:text-[#00513b]"
                      href={buildCanonicalClubPath(context, teamId)}
                      key={`${context.competitionId}-${context.seasonId}`}
                    >
                      {context.seasonLabel}
                      <span aria-hidden="true">→</span>
                    </Link>
                  ))}
                </div>
              </details>
            ))}
          </div>
        ) : (
          <EmptyState title="Sem temporadas detalhadas" description="O clube está identificado, mas os recortes de competição ainda não foram publicados." />
        )}
      </ProfilePanel>
    </ProfileShell>
  );
}
